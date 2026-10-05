// Package monitor turns tapped OCPP traffic into a charger registry and
// traffic statistics. It is shared by the command-api (registry only) and the
// dashboard (both) services.
package monitor

import (
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/ocpp"
)

// Event is one frame seen on amq.topic. Two kinds of frames cross it:
//   - charger → CSMS: routing key ocpp16.Heartbeat.req, reply_to = charge
//     point id, correlation_id = OCPP message id
//   - CSMS → charger: routing key = charge point id
type Event struct {
	FromCharger bool
	ChargerID   string
	Key         ocpp.ChargerKey // only when FromCharger
	Type        int
	MessageID   string
	Frame       ocpp.Frame
}

// Classify decodes a tapped delivery.
func Classify(d amqp.Delivery) (Event, bool) {
	frame, messageType, messageID, ok := ocpp.Decode(d.Body)
	if !ok {
		return Event{}, false
	}
	ev := Event{Type: messageType, MessageID: messageID, Frame: frame}
	if key, ok := ocpp.ParseRoutingKey(d.RoutingKey); ok {
		if d.ReplyTo == "" {
			return Event{}, false
		}
		ev.FromCharger, ev.Key, ev.ChargerID = true, key, d.ReplyTo
	} else {
		ev.ChargerID = d.RoutingKey
	}
	return ev, true
}

// Payload of a CALL/SEND (element 3).
func (e Event) Payload() map[string]any { return e.Frame.Object(3) }

func nowMillis() int64 { return time.Now().UnixMilli() }

// ---- Registry ---------------------------------------------------------------

type charger struct {
	version         string
	online          bool
	lastSeen        int64
	securityProfile string
	connectors      map[string]string
}

// ChargerView is the JSON shape of a charger in the API.
type ChargerView struct {
	ID              string            `json:"id"`
	OcppVersion     string            `json:"ocppVersion"`
	SecurityProfile *string           `json:"securityProfile"`
	Online          bool              `json:"online"`
	LastSeen        int64             `json:"lastSeen"`
	Connectors      map[string]string `json:"connectors"`
}

type ChargerSummary struct {
	Known             int            `json:"known"`
	Online            int            `json:"online"`
	ByVersion         map[string]int `json:"byVersion"`
	BySecurityProfile map[string]int `json:"bySecurityProfile"`
}

// Registry is what the CSMS knows about every charge point, learned purely
// from the OCPP traffic seen on the broker.
type Registry struct {
	mu       sync.RWMutex
	chargers map[string]*charger
}

func NewRegistry() *Registry { return &Registry{chargers: map[string]*charger{}} }

// Apply updates the registry with a frame sent by a charger.
func (r *Registry) Apply(ev Event) {
	if !ev.FromCharger {
		return
	}
	var payload map[string]any
	if ev.Key.Direction == "req" {
		payload = ev.Payload()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.chargers[ev.ChargerID]
	if c == nil {
		c = &charger{securityProfile: ocpp.SecurityProfile(ev.ChargerID), connectors: map[string]string{}}
		r.chargers[ev.ChargerID] = c
	}
	c.version, c.lastSeen = ev.Key.Version, nowMillis()
	if ev.Key.Direction != "req" {
		// A CALLRESULT/CALLERROR from the charger proves it is connected.
		c.online = true
		return
	}
	if ocpp.IsSyntheticOffline(ev.Key.Action, payload) {
		c.online = false
		return
	}
	c.online = true
	if ev.Key.Action == "StatusNotification" {
		// 1.6: connectorId + status. 2.x: evseId + connectorStatus (one connector per EVSE).
		connectorKey, statusKey := "connectorId", "status"
		if ocpp.IsV2(ev.Key.Version) {
			connectorKey, statusKey = "evseId", "connectorStatus"
		}
		connector, _ := payload[connectorKey].(float64)
		status, _ := payload[statusKey].(string)
		if connector > 0 && status != "" {
			c.connectors[strconv.Itoa(int(connector))] = status
		}
	}
}

// Version returns the charger's protocol and whether it is online.
func (r *Registry) Version(id string) (version string, online, known bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := r.chargers[id]
	if c == nil {
		return "", false, false
	}
	return c.version, c.online, true
}

func view(id string, c *charger) ChargerView {
	v := ChargerView{ID: id, OcppVersion: c.version, Online: c.online, LastSeen: c.lastSeen,
		Connectors: make(map[string]string, len(c.connectors))}
	if c.securityProfile != "" {
		sp := c.securityProfile
		v.SecurityProfile = &sp
	}
	for k, s := range c.connectors {
		v.Connectors[k] = s
	}
	return v
}

func (r *Registry) Get(id string) (ChargerView, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := r.chargers[id]
	if c == nil {
		return ChargerView{}, false
	}
	return view(id, c), true
}

// List returns online chargers first, then by id.
func (r *Registry) List(limit int) []ChargerView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.chargers))
	for id := range r.chargers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := r.chargers[ids[i]], r.chargers[ids[j]]
		if a.online != b.online {
			return a.online
		}
		return ids[i] < ids[j]
	})
	out := make([]ChargerView, 0, min(limit, len(ids)))
	for _, id := range ids[:min(limit, len(ids))] {
		out = append(out, view(id, r.chargers[id]))
	}
	return out
}

// Summary counts online chargers by version and security profile, and their
// connectors by status.
func (r *Registry) Summary() (ChargerSummary, map[string]int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := ChargerSummary{Known: len(r.chargers), ByVersion: map[string]int{}, BySecurityProfile: map[string]int{}}
	connectors := map[string]int{}
	for _, c := range r.chargers {
		if !c.online {
			continue
		}
		s.Online++
		s.ByVersion[c.version]++
		if c.securityProfile != "" {
			s.BySecurityProfile[c.securityProfile]++
		}
		for _, status := range c.connectors {
			connectors[status]++
		}
	}
	return s, connectors
}

// ---- Stats --------------------------------------------------------------------

type RecentFrame struct {
	TS        int64   `json:"ts"`
	Direction string  `json:"direction"`
	ChargerID string  `json:"chargerId"`
	Kind      string  `json:"kind"`
	Action    *string `json:"action"`
	MessageID string  `json:"messageId"`
}

type ActionRate struct {
	Action    string  `json:"action"`
	Direction string  `json:"direction"`
	PerSec    float64 `json:"perSec"`
}

type Traffic struct {
	InPerSec  float64      `json:"inPerSec"`
	OutPerSec float64      `json:"outPerSec"`
	ByAction  []ActionRate `json:"byAction"`
}

type Latency struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
	Max     float64 `json:"max"`
}

type Commands struct {
	Sent     int64 `json:"sent"`
	Accepted int64 `json:"accepted"`
	Rejected int64 `json:"rejected"`
	Failed   int64 `json:"failed"`
	Pending  int   `json:"pending"`
}

type Window struct {
	Traffic  Traffic
	Latency  Latency
	Commands Commands
	Recent   []RecentFrame
}

const (
	recentSize      = 30
	pendingExpiryMs = 60_000
)

type flightKey struct{ chargerID, messageID string }

type inFlight struct {
	ts     int64
	action string
}

// Stats keeps traffic counters: message rates, CSMS reply latency (charger
// request seen on the broker until the CSMS answer is seen on the broker)
// and command outcomes.
type Stats struct {
	mu                       sync.Mutex
	inbound, outbound        int
	actions                  map[[2]string]int
	awaitingReply            map[flightKey]int64
	commands                 map[flightKey]inFlight
	sent, accepted, rejected int64
	failed                   int64
	latencies                []float64
	recent                   []RecentFrame
	windowStart              time.Time
}

func NewStats() *Stats {
	return &Stats{actions: map[[2]string]int{}, awaitingReply: map[flightKey]int64{},
		commands: map[flightKey]inFlight{}, windowStart: time.Now()}
}

func (s *Stats) remember(f RecentFrame) {
	s.recent = append([]RecentFrame{f}, s.recent[:min(len(s.recent), recentSize-1)]...)
}

func ptr(s string) *string { return &s }

// Apply accounts one tapped frame.
func (s *Stats) Apply(ev Event) {
	now := nowMillis()
	kind := ocpp.KindName(ev.Type)
	key := flightKey{ev.ChargerID, ev.MessageID}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case ev.FromCharger && ev.Key.Direction == "req":
		// CALL or SEND from a charger. CALLs are timed until the CSMS answers.
		s.inbound++
		s.actions[[2]string{"in", ev.Key.Action}]++
		if ev.Type == ocpp.Call && !ocpp.IsSyntheticOffline(ev.Key.Action, ev.Payload()) {
			s.awaitingReply[key] = now
		}
		s.remember(RecentFrame{now, "in", ev.ChargerID, kind, ptr(ev.Key.Action), ev.MessageID})
	case ev.FromCharger:
		// CALLRESULT/CALLERROR from a charger, answering a CSMS command.
		s.inbound++
		var action *string
		if cmd, ok := s.commands[key]; ok {
			delete(s.commands, key)
			action = ptr(cmd.action)
			status, _ := ev.Frame.Object(2)["status"].(string)
			switch {
			case ev.Type != ocpp.CallResult:
				s.failed++
			case status == "" || status == "Accepted" || status == "Scheduled":
				s.accepted++
			default:
				s.rejected++
			}
		}
		s.remember(RecentFrame{now, "in", ev.ChargerID, kind, action, ev.MessageID})
	case ev.Type == ocpp.Call:
		// CALL from the CSMS to a charger (a command).
		action := ev.Frame.String(2)
		s.outbound++
		s.sent++
		s.actions[[2]string{"out", action}]++
		s.commands[key] = inFlight{now, action}
		s.remember(RecentFrame{now, "out", ev.ChargerID, "CALL", ptr(action), ev.MessageID})
	default:
		// CALLRESULT/CALLERROR from the CSMS, answering a charger request.
		s.outbound++
		if requested, ok := s.awaitingReply[key]; ok {
			delete(s.awaitingReply, key)
			s.latencies = append(s.latencies, float64(now-requested))
		}
		s.remember(RecentFrame{now, "out", ev.ChargerID, kind, nil, ev.MessageID})
	}
}

// Window closes the current window: rates since the previous call and its
// latency distribution.
func (s *Stats) Window() Window {
	s.mu.Lock()
	defer s.mu.Unlock()
	seconds := max(time.Since(s.windowStart).Seconds(), 0.001)
	s.windowStart = time.Now()

	w := Window{Traffic: Traffic{
		InPerSec:  float64(s.inbound) / seconds,
		OutPerSec: float64(s.outbound) / seconds,
		ByAction:  make([]ActionRate, 0, len(s.actions)),
	}}
	for k, n := range s.actions {
		w.Traffic.ByAction = append(w.Traffic.ByAction, ActionRate{k[1], k[0], float64(n) / seconds})
	}
	s.inbound, s.outbound = 0, 0
	clear(s.actions)

	samples := s.latencies
	s.latencies = nil
	slices.Sort(samples)
	pct := func(p float64) float64 {
		if len(samples) == 0 {
			return 0
		}
		i := int(p*float64(len(samples))+0.999999) - 1
		return samples[max(0, min(i, len(samples)-1))]
	}
	w.Latency = Latency{Samples: len(samples), P50: pct(0.50), P95: pct(0.95), P99: pct(0.99)}
	if len(samples) > 0 {
		w.Latency.Max = samples[len(samples)-1]
	}

	cutoff := nowMillis() - pendingExpiryMs
	for k, ts := range s.awaitingReply {
		if ts < cutoff {
			delete(s.awaitingReply, k)
		}
	}
	for k, c := range s.commands {
		if c.ts < cutoff {
			delete(s.commands, k)
		}
	}
	w.Commands = Commands{s.sent, s.accepted, s.rejected, s.failed, len(s.commands)}
	w.Recent = slices.Clone(s.recent)
	if w.Recent == nil {
		w.Recent = []RecentFrame{}
	}
	return w
}
