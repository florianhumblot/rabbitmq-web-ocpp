// Package registry is the charger registry shared by every instance of every
// service, kept in Valkey. Workers record what chargers send; the command API
// and the dashboard read it. The update logic lives in valkey/registry.lua,
// shared with the Java and Rust implementations.
package registry

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/ocpp"
)

// Versions and profiles reported by the dashboard.
var (
	versions = []string{"ocpp16", "ocpp201", "ocpp21"}
	profiles = []string{"1", "2", "3"}
)

// ChargerView is the JSON shape of a charger in the API.
type ChargerView struct {
	ID              string            `json:"id"`
	OcppVersion     string            `json:"ocppVersion"`
	SecurityProfile *string           `json:"securityProfile"`
	Online          bool              `json:"online"`
	LastSeen        int64             `json:"lastSeen"`
	Connectors      map[string]string `json:"connectors"`
}

type Summary struct {
	Known             int64            `json:"known"`
	Online            int64            `json:"online"`
	ByVersion         map[string]int64 `json:"byVersion"`
	BySecurityProfile map[string]int64 `json:"bySecurityProfile"`
}

// PresenceFunc asks the broker whether a charger is connected right now.
type PresenceFunc func(ctx context.Context, chargerID string) (bool, error)

type Registry struct {
	rdb    *redis.Client
	script *redis.Script
	prefix string
}

// New connects to Valkey and loads the shared script from REGISTRY_SCRIPT.
func New(vhost string) (*Registry, error) {
	src, err := os.ReadFile(env.String("REGISTRY_SCRIPT", "../valkey/registry.lua"))
	if err != nil {
		return nil, fmt.Errorf("registry script: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         env.String("VALKEY_HOST", "localhost") + ":" + env.String("VALKEY_PORT", "6379"),
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	// One hash tag per vhost: everything of an implementation on one shard.
	return &Registry{rdb: rdb, script: redis.NewScript(string(src)), prefix: "csms:{" + vhost + "}:"}, nil
}

func (r *Registry) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

func (r *Registry) Close() error { return r.rdb.Close() }

func (r *Registry) chargerKey(id string) string { return r.prefix + "c:" + id }

func (r *Registry) run(ctx context.Context, id string, args ...any) (string, error) {
	all := append([]any{r.prefix, args[0], id, time.Now().UnixMilli()}, args[1:]...)
	return r.script.Run(ctx, r.rdb, []string{r.chargerKey(id)}, all...).Text()
}

// Observe records a CALL or SEND a worker processed. When the script reports
// that an online/offline transition is due, the broker is asked whether the
// charger is really connected, as frames may be processed out of order.
func (r *Registry) Observe(ctx context.Context, chargerID, version, action string, payload map[string]any,
	presence PresenceFunc) error {
	var result string
	var err error
	if ocpp.IsSyntheticOffline(action, payload) {
		result, err = r.run(ctx, chargerID, "offline")
	} else {
		connector, status, ts := statusOf(version, action, payload)
		result, err = r.run(ctx, chargerID, "event", version, ocpp.SecurityProfile(chargerID), connector, status, ts)
	}
	if err != nil || result != "check" {
		return err
	}
	connected, err := presence(ctx, chargerID)
	if err != nil {
		return err
	}
	_, err = r.run(ctx, chargerID, "presence", map[bool]string{true: "1", false: "0"}[connected])
	return err
}

// statusOf extracts the connector status of a StatusNotification.
// 1.6: connectorId + status. 2.x: evseId + connectorStatus (one connector per EVSE).
func statusOf(version, action string, payload map[string]any) (connector, status, ts string) {
	if action != "StatusNotification" {
		return "", "", ""
	}
	connectorKey, statusKey := "connectorId", "status"
	if ocpp.IsV2(version) {
		connectorKey, statusKey = "evseId", "connectorStatus"
	}
	n, _ := payload[connectorKey].(float64)
	status, _ = payload[statusKey].(string)
	ts, _ = payload["timestamp"].(string)
	if n <= 0 || status == "" {
		return "", "", ""
	}
	return strconv.Itoa(int(n)), status, ts
}

// Lookup returns the protocol of a charger and whether it is connected.
func (r *Registry) Lookup(ctx context.Context, id string) (version string, online, known bool, err error) {
	v, err := r.rdb.HMGet(ctx, r.chargerKey(id), "v", "on").Result()
	if err != nil || v[0] == nil {
		return "", false, false, err
	}
	return v[0].(string), v[1] == "1", true, nil
}

func (r *Registry) view(id string, h map[string]string) ChargerView {
	view := ChargerView{ID: id, OcppVersion: h["v"], Online: h["on"] == "1", Connectors: map[string]string{}}
	view.LastSeen, _ = strconv.ParseInt(h["seen"], 10, 64)
	if sp := h["sp"]; sp != "" {
		view.SecurityProfile = &sp
	}
	for k, v := range h {
		if c, ok := strings.CutPrefix(k, "c:"); ok {
			view.Connectors[c] = v
		}
	}
	return view
}

func (r *Registry) Get(ctx context.Context, id string) (ChargerView, bool, error) {
	h, err := r.rdb.HGetAll(ctx, r.chargerKey(id)).Result()
	if err != nil || len(h) == 0 {
		return ChargerView{}, false, err
	}
	return r.view(id, h), true, nil
}

// List returns up to limit connected chargers.
func (r *Registry) List(ctx context.Context, limit int) ([]ChargerView, error) {
	ids, err := r.rdb.SRandMemberN(ctx, r.prefix+"online", int64(limit)).Result()
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, r.chargerKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && len(ids) > 0 {
		return nil, err
	}
	out := make([]ChargerView, 0, len(ids))
	for i, id := range ids {
		out = append(out, r.view(id, cmds[i].Val()))
	}
	return out, nil
}

// Summary counts connected chargers by protocol and security profile, and
// their connectors by status, in one round trip.
func (r *Registry) Summary(ctx context.Context) (Summary, map[string]int64, error) {
	pipe := r.rdb.Pipeline()
	known := pipe.SCard(ctx, r.prefix+"known")
	online := pipe.SCard(ctx, r.prefix+"online")
	byVersion := make(map[string]*redis.IntCmd, len(versions))
	for _, v := range versions {
		byVersion[v] = pipe.SCard(ctx, r.prefix+"online:v:"+v)
	}
	byProfile := make(map[string]*redis.IntCmd, len(profiles))
	for _, sp := range profiles {
		byProfile[sp] = pipe.SCard(ctx, r.prefix+"online:sp:"+sp)
	}
	statuses := pipe.HGetAll(ctx, r.prefix+"status")
	if _, err := pipe.Exec(ctx); err != nil {
		return Summary{}, nil, err
	}
	s := Summary{Known: known.Val(), Online: online.Val(), ByVersion: map[string]int64{}, BySecurityProfile: map[string]int64{}}
	for v, c := range byVersion {
		if c.Val() > 0 {
			s.ByVersion[v] = c.Val()
		}
	}
	for sp, c := range byProfile {
		if c.Val() > 0 {
			s.BySecurityProfile[sp] = c.Val()
		}
	}
	connectors := map[string]int64{}
	for status, n := range statuses.Val() {
		if count, _ := strconv.ParseInt(n, 10, 64); count > 0 {
			connectors[status] = count
		}
	}
	return s, connectors, nil
}
