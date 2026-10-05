// Command dashboard serves the shared dashboard page and pushes one JSON
// snapshot per second on /ws. It is the single entry point of the Go CSMS:
// /api/ is reverse-proxied to the command-api service.
//
// Everything shown comes from RabbitMQ: a tap on all OCPP traffic of the
// vhost (both directions) feeds the registry and the statistics, and the
// management API provides the broker-wide figures.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/broker"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/monitor"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/rmq"
)

type snapshot struct {
	Implementation string                 `json:"implementation"`
	Instance       string                 `json:"instance"`
	UptimeSeconds  int64                  `json:"uptimeSeconds"`
	Timestamp      int64                  `json:"timestamp"`
	Chargers       monitor.ChargerSummary `json:"chargers"`
	Connectors     map[string]int         `json:"connectors"`
	Traffic        monitor.Traffic        `json:"traffic"`
	Latency        monitor.Latency        `json:"latency"`
	Commands       monitor.Commands       `json:"commands"`
	Broker         broker.View            `json:"broker"`
	Recent         []monitor.RecentFrame  `json:"recent"`
}

// hub fans snapshots out to the connected browsers.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, 4)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *hub) broadcast(msg []byte) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- msg:
		default: // a slow viewer just skips snapshots
		}
	}
	return len(h.subs)
}

func main() {
	cfg := env.LoadRabbitMQ()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry, stats := monitor.NewRegistry(), monitor.NewStats()
	poller := broker.NewPoller(cfg)
	go poller.Run(ctx)

	conn := rmq.Dial(cfg.URI(), "csms-go-dashboard")
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		fatal("channel", err)
	}
	tap, err := rmq.DeclareTap(ch, "#")
	if err != nil {
		fatal("declare tap", err)
	}
	_ = ch.Qos(1000, 0, false)
	deliveries, err := ch.Consume(tap, "csms-go-dashboard", true, true, false, false, nil)
	if err != nil {
		fatal("consume", err)
	}
	go func() {
		for d := range deliveries {
			if ev, ok := monitor.Classify(d); ok {
				registry.Apply(ev)
				stats.Apply(ev)
			}
		}
		slog.Error("tap consumer stopped")
		os.Exit(1)
	}()

	h := &hub{subs: map[chan []byte]struct{}{}}
	go publishSnapshots(ctx, h, registry, stats, poller)

	commandAPI, err := url.Parse(env.String("COMMAND_API_URL", "http://localhost:8081"))
	if err != nil {
		fatal("COMMAND_API_URL", err)
	}
	static := env.String("STATIC_DIR", "../dashboard")
	mux := http.NewServeMux()
	mux.Handle("/api/", httputil.NewSingleHostReverseProxy(commandAPI))
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) { serveWS(w, r, h) })
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(static, "index.html"))
	})
	server := &http.Server{Addr: ":" + env.String("HTTP_PORT", "8080"), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	slog.Info("dashboard listening", "addr", server.Addr, "vhost", cfg.Vhost, "commandApi", commandAPI.String())
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		fatal("http server", err)
	}
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

func publishSnapshots(ctx context.Context, h *hub, registry *monitor.Registry, stats *monitor.Stats, poller *broker.Poller) {
	started := time.Now()
	instance, _ := os.Hostname()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// Close the statistics window even without viewers, so rates stay per second.
		w := stats.Window()
		chargers, connectors := registry.Summary()
		msg, err := json.Marshal(snapshot{
			Implementation: "go",
			Instance:       instance,
			UptimeSeconds:  int64(time.Since(started).Seconds()),
			Timestamp:      time.Now().UnixMilli(),
			Chargers:       chargers,
			Connectors:     connectors,
			Traffic:        w.Traffic,
			Latency:        w.Latency,
			Commands:       w.Commands,
			Broker:         poller.Current(),
			Recent:         w.Recent,
		})
		if err == nil {
			h.broadcast(msg)
		}
	}
}

func serveWS(w http.ResponseWriter, r *http.Request, h *hub) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context()) // we never expect messages from the browser
	sub := h.subscribe()
	defer h.unsubscribe(sub)
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sub:
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(writeCtx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
