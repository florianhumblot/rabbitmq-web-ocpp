// Command command-api is the version-neutral CSMS command API.
//
// A request is translated to the OCPP 1.6 or 2.x payload based on the
// protocol recorded in the shared registry, or on an explicit ocppVersion
// field in the body. The CALL is published to amq.topic with the charge point
// id as routing key; the plugin queues it in the charger's own queue.
//
// Any number of replicas can run behind a load balancer. The charger's answer
// (<protocol>.response.conf|error) lands in the shared csms.responses queue
// and may be consumed by any replica. The OCPP message id starts with the id
// of the instance that sent the command, so a replica that receives somebody
// else's answer forwards it to that instance's private queue: at most one
// extra hop, and no instance sees all answers.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/health"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/lifecycle"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/ocpp"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/registry"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/rmq"
)

var (
	versions    = []string{"ocpp16", "ocpp201", "ocpp21"}
	triggerable = []string{"BootNotification", "Heartbeat", "MeterValues", "StatusNotification"}
)

const replyQueuePrefix = "csms.replies."

type key struct{ chargerID, messageID string }

type api struct {
	instance  string
	registry  *registry.Registry
	publisher *amqp.Channel
	timeout   time.Duration

	mu      sync.Mutex
	pending map[key]chan ocpp.Frame
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

// newMessageID returns "<instance>-<random>", 36 characters: the maximum
// length of an OCPP message id.
func (a *api) newMessageID() string { return a.instance + "-" + randomHex(27) }

// ownerOf extracts the instance id from a message id made by newMessageID.
func ownerOf(messageID string) string {
	if len(messageID) != 36 || messageID[8] != '-' {
		return ""
	}
	return messageID[:8]
}

func main() {
	cfg := env.LoadRabbitMQ()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg, err := registry.New(cfg.Vhost)
	if err != nil {
		lifecycle.Fatal("registry", err)
	}
	a := &api{
		instance: randomHex(8),
		registry: reg,
		timeout:  env.Millis("COMMAND_TIMEOUT_MS", 15000),
		pending:  map[key]chan ocpp.Frame{},
	}
	conn := rmq.Dial(cfg.URI(), "csms-go-command-api-"+a.instance)
	if a.publisher, err = conn.Channel(); err != nil {
		lifecycle.Fatal("channel", err)
	}

	// The shared queue of answers, consumed by all replicas...
	sharedCh, err := conn.Channel()
	if err != nil {
		lifecycle.Fatal("channel", err)
	}
	if err := rmq.DeclareResponsesQueue(sharedCh); err != nil {
		lifecycle.Fatal("declare topology", err)
	}
	_ = sharedCh.Qos(200, 0, false)
	shared, err := sharedCh.Consume(rmq.ResponsesQueue, "csms-go-api-"+a.instance, false, false, false, false, nil)
	if err != nil {
		lifecycle.Fatal("consume", err)
	}
	// ...and this instance's private queue, for answers forwarded by others.
	privateCh, err := conn.Channel()
	if err != nil {
		lifecycle.Fatal("channel", err)
	}
	if _, err := privateCh.QueueDeclare(replyQueuePrefix+a.instance, false, true, true, false, nil); err != nil {
		lifecycle.Fatal("declare reply queue", err)
	}
	private, err := privateCh.Consume(replyQueuePrefix+a.instance, "", true, true, false, false, nil)
	if err != nil {
		lifecycle.Fatal("consume", err)
	}
	var consumers sync.WaitGroup
	consumers.Add(2)
	go func() { defer consumers.Done(); a.consumeShared(ctx, shared) }()
	go func() { defer consumers.Done(); a.consumePrivate(ctx, private) }()

	h := health.New(map[string]health.Check{"rabbitmq": lifecycle.AMQPConnected(conn), "valkey": reg.Ping})
	mux := http.NewServeMux()
	h.Register(mux)
	mux.HandleFunc("GET /api/chargers", a.list)
	mux.HandleFunc("GET /api/chargers/{id}", a.get)
	mux.HandleFunc("POST /api/chargers/{id}/reset", a.reset)
	mux.HandleFunc("POST /api/chargers/{id}/change-availability", a.changeAvailability)
	mux.HandleFunc("POST /api/chargers/{id}/trigger-message", a.triggerMessage)
	server := &http.Server{Addr: ":" + env.String("HTTP_PORT", "8080"), Handler: mux}
	lifecycle.Serve(server)
	h.SetReady(true)
	slog.Info("command-api listening", "addr", server.Addr, "vhost", cfg.Vhost, "instance", a.instance)

	<-ctx.Done()
	// Drain: leave the shared queue to the other replicas (they forward our
	// answers to our private queue), finish the requests in flight, then go.
	h.SetReady(false)
	slog.Info("draining")
	_ = sharedCh.Cancel("csms-go-api-"+a.instance, false)
	lifecycle.Shutdown(server, env.ShutdownTimeout())
	_ = conn.Close()
	lifecycle.WaitTimeout(&consumers, 2*time.Second)
	_ = reg.Close()
	slog.Info("stopped")
}

func (a *api) complete(chargerID, messageID string, body []byte) bool {
	a.mu.Lock()
	waiter := a.pending[key{chargerID, messageID}]
	delete(a.pending, key{chargerID, messageID})
	a.mu.Unlock()
	if waiter == nil {
		return false
	}
	if frame, _, _, ok := ocpp.Decode(body); ok {
		waiter <- frame
	}
	return true
}

func (a *api) consumeShared(ctx context.Context, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		owner := ownerOf(d.CorrelationId)
		switch {
		case owner == a.instance:
			a.complete(d.ReplyTo, d.CorrelationId, d.Body)
		case owner != "":
			// Somebody else's answer: forward through the default exchange. If
			// that instance is gone, the message is unroutable and dropped,
			// like its HTTP request.
			fwdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := a.publisher.PublishWithContext(fwdCtx, "", replyQueuePrefix+owner, false, false, amqp.Publishing{
				ContentType: d.ContentType, CorrelationId: d.CorrelationId, ReplyTo: d.ReplyTo, Body: d.Body,
			})
			cancel()
			if err != nil {
				slog.Error("forward failed", "owner", owner, "err", err)
				_ = d.Nack(false, true)
				continue
			}
		}
		_ = d.Ack(false)
	}
	if ctx.Err() == nil {
		lifecycle.Fatal("responses consumer stopped", errors.New("delivery channel closed"))
	}
}

func (a *api) consumePrivate(ctx context.Context, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		a.complete(d.ReplyTo, d.CorrelationId, d.Body)
	}
	if ctx.Err() == nil {
		lifecycle.Fatal("reply consumer stopped", errors.New("delivery channel closed"))
	}
}

// ---- HTTP handlers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 100
	}
	views, err := a.registry.List(r.Context(), max(0, limit))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, views)
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v, ok, err := a.registry.Get(r.Context(), id)
	switch {
	case err != nil:
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case !ok:
		writeError(w, http.StatusNotFound, "unknown charge point "+id)
	default:
		writeJSON(w, http.StatusOK, v)
	}
}

// commandBody is the union of all command request fields.
type commandBody struct {
	Type             *string `json:"type"`
	ConnectorID      *int    `json:"connectorId"`
	RequestedMessage *string `json:"requestedMessage"`
	OcppVersion      *string `json:"ocppVersion"`
}

func or[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

// parseBody accepts an empty body as "all defaults".
func parseBody(w http.ResponseWriter, r *http.Request) (commandBody, bool) {
	var b commandBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "body must be a JSON object")
		return b, false
	}
	return b, true
}

func (a *api) reset(w http.ResponseWriter, r *http.Request) {
	b, ok := parseBody(w, r)
	if !ok {
		return
	}
	kind := or(b.Type, "Soft")
	if kind != "Soft" && kind != "Hard" {
		writeError(w, http.StatusBadRequest, "type must be Soft or Hard")
		return
	}
	a.send(w, r.Context(), r.PathValue("id"), b, "Reset", func(v2 bool) map[string]any {
		if !v2 {
			return map[string]any{"type": kind}
		}
		if kind == "Hard" {
			return map[string]any{"type": "Immediate"}
		}
		return map[string]any{"type": "OnIdle"}
	})
}

func (a *api) changeAvailability(w http.ResponseWriter, r *http.Request) {
	b, ok := parseBody(w, r)
	if !ok {
		return
	}
	kind, connector := or(b.Type, "Inoperative"), or(b.ConnectorID, 0)
	if (kind != "Operative" && kind != "Inoperative") || connector < 0 {
		writeError(w, http.StatusBadRequest, "type must be Operative or Inoperative, connectorId >= 0")
		return
	}
	a.send(w, r.Context(), r.PathValue("id"), b, "ChangeAvailability", func(v2 bool) map[string]any {
		if !v2 {
			return map[string]any{"connectorId": connector, "type": kind}
		}
		p := map[string]any{"operationalStatus": kind}
		if connector > 0 {
			p["evse"] = map[string]any{"id": connector}
		}
		return p
	})
}

func (a *api) triggerMessage(w http.ResponseWriter, r *http.Request) {
	b, ok := parseBody(w, r)
	if !ok {
		return
	}
	requested, connector := or(b.RequestedMessage, "StatusNotification"), or(b.ConnectorID, 0)
	if !slices.Contains(triggerable, requested) || connector < 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("requestedMessage must be one of %v", triggerable))
		return
	}
	a.send(w, r.Context(), r.PathValue("id"), b, "TriggerMessage", func(v2 bool) map[string]any {
		p := map[string]any{"requestedMessage": requested}
		if connector > 0 {
			if v2 {
				p["evse"] = map[string]any{"id": connector}
			} else {
				p["connectorId"] = connector
			}
		}
		return p
	})
}

func (a *api) send(w http.ResponseWriter, ctx context.Context, id string, b commandBody, action string,
	build func(v2 bool) map[string]any) {
	version := or(b.OcppVersion, "")
	switch {
	case version != "" && !slices.Contains(versions, version):
		writeError(w, http.StatusBadRequest, fmt.Sprintf("ocppVersion must be one of %v", versions))
		return
	case version == "":
		v, online, known, err := a.registry.Lookup(ctx, id)
		switch {
		case err != nil:
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		case !known:
			writeError(w, http.StatusNotFound, "unknown charge point "+id+" (pass ocppVersion to force)")
			return
		case !online:
			writeError(w, http.StatusConflict, "charge point "+id+" is offline")
			return
		}
		version = v
	}
	payload := build(ocpp.IsV2(version))
	messageID := a.newMessageID()
	result := map[string]any{"chargerId": id, "action": action, "ocppVersion": version,
		"request": payload, "messageId": messageID}

	k := key{id, messageID}
	answer := make(chan ocpp.Frame, 1)
	a.mu.Lock()
	a.pending[k] = answer
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.pending, k)
		a.mu.Unlock()
	}()

	body, _ := json.Marshal([]any{ocpp.Call, messageID, action, payload})
	start := time.Now()
	// A command nobody picked up in time must not reach the charger later.
	if err := rmq.Publish(ctx, a.publisher, id, messageID, a.timeout, body); err != nil {
		writeError(w, http.StatusServiceUnavailable, "publish failed: "+err.Error())
		return
	}

	timer := time.NewTimer(a.timeout)
	defer timer.Stop()
	select {
	case frame := <-answer:
		result["latencyMs"] = time.Since(start).Milliseconds()
		var messageType int
		_ = json.Unmarshal(frame[0], &messageType)
		if messageType == ocpp.CallResult {
			response := frame.Object(2)
			result["status"] = response["status"]
			result["response"] = response
			writeJSON(w, http.StatusOK, result)
			return
		}
		result["error"] = map[string]any{"code": frame.String(2), "description": frame.String(3)}
		writeJSON(w, http.StatusBadGateway, result)
	case <-timer.C:
		result["latencyMs"] = time.Since(start).Milliseconds()
		result["error"] = "no answer from the charge point in time"
		writeJSON(w, http.StatusGatewayTimeout, result)
	case <-ctx.Done():
	}
}
