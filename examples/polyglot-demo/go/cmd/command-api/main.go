// Command command-api is the version-neutral CSMS command API.
//
// A request is translated to the OCPP 1.6 or 2.x payload based on the
// protocol the charger connected with, or on an explicit ocppVersion field in
// the body. The CALL is published to amq.topic with the charge point id as
// routing key; the plugin queues it in the charger's own queue. The answer
// comes back as <protocol>.response.conf|error.
//
// The service learns about chargers from its own tap on all charger-sent
// frames (*.*.req and *.response.*), which also delivers command answers.
// Every replica taps all answers, so only the one holding the pending call
// completes it.
package main

import (
	"context"
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

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/monitor"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/ocpp"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/rmq"
)

var (
	versions    = []string{"ocpp16", "ocpp201", "ocpp21"}
	triggerable = []string{"BootNotification", "Heartbeat", "MeterValues", "StatusNotification"}
)

type key struct{ chargerID, messageID string }

type api struct {
	registry  *monitor.Registry
	publisher *amqp.Channel
	timeout   time.Duration

	mu      sync.Mutex
	pending map[key]chan ocpp.Frame
}

func main() {
	cfg := env.LoadRabbitMQ()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn := rmq.Dial(cfg.URI(), "csms-go-command-api")
	defer conn.Close()
	a := &api{
		registry: monitor.NewRegistry(),
		timeout:  env.Millis("COMMAND_TIMEOUT_MS", 15000),
		pending:  map[key]chan ocpp.Frame{},
	}
	var err error
	if a.publisher, err = conn.Channel(); err != nil {
		fatal("channel", err)
	}
	tapCh, err := conn.Channel()
	if err != nil {
		fatal("channel", err)
	}
	tap, err := rmq.DeclareTap(tapCh, "*.*.req", "*.response.conf", "*.response.error")
	if err != nil {
		fatal("declare tap", err)
	}
	_ = tapCh.Qos(1000, 0, false)
	deliveries, err := tapCh.Consume(tap, "csms-go-command-api", true, true, false, false, nil)
	if err != nil {
		fatal("consume", err)
	}
	go a.consumeTap(deliveries)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/chargers", a.list)
	mux.HandleFunc("GET /api/chargers/{id}", a.get)
	mux.HandleFunc("POST /api/chargers/{id}/reset", a.reset)
	mux.HandleFunc("POST /api/chargers/{id}/change-availability", a.changeAvailability)
	mux.HandleFunc("POST /api/chargers/{id}/trigger-message", a.triggerMessage)
	server := &http.Server{Addr: ":" + env.String("HTTP_PORT", "8080"), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	slog.Info("command-api listening", "addr", server.Addr, "vhost", cfg.Vhost)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		fatal("http server", err)
	}
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

func (a *api) consumeTap(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		ev, ok := monitor.Classify(d)
		if !ok || !ev.FromCharger {
			continue
		}
		a.registry.Apply(ev)
		if ev.Key.Direction != "req" {
			a.mu.Lock()
			waiter := a.pending[key{ev.ChargerID, ev.MessageID}]
			delete(a.pending, key{ev.ChargerID, ev.MessageID})
			a.mu.Unlock()
			if waiter != nil {
				waiter <- ev.Frame
			}
		}
	}
	slog.Error("tap consumer stopped")
	os.Exit(1)
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
	writeJSON(w, http.StatusOK, a.registry.List(max(0, limit)))
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if v, ok := a.registry.Get(id); ok {
		writeJSON(w, http.StatusOK, v)
		return
	}
	writeError(w, http.StatusNotFound, "unknown charge point "+id)
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
		v, online, known := a.registry.Version(id)
		if !known {
			writeError(w, http.StatusNotFound, "unknown charge point "+id+" (pass ocppVersion to force)")
			return
		}
		if !online {
			writeError(w, http.StatusConflict, "charge point "+id+" is offline")
			return
		}
		version = v
	}
	payload := build(ocpp.IsV2(version))
	messageID := uuid.NewString()
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
