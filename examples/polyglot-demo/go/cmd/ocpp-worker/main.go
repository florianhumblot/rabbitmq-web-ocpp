// Command ocpp-worker answers charger-initiated OCPP CALLs.
//
// It is stateless: it consumes the shared csms.requests queue, publishes each
// answer to amq.topic with the charge point id as routing key (the plugin
// delivers it down the charger's WebSocket) and records the charger's state
// in the shared registry. Run as many replicas as the load needs; on SIGTERM
// it stops consuming, finishes the messages it holds and exits.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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

// Replies to a charger that went away are useless after a while.
const replyTTL = 60 * time.Second

type worker struct {
	heartbeat int
	registry  *registry.Registry
	presence  *rmq.Presence
}

type consumer struct {
	ch  *amqp.Channel
	tag string
}

func main() {
	cfg := env.LoadRabbitMQ()
	prefetch := env.Int("PREFETCH", 200)
	concurrency := env.Int("WORKER_CONCURRENCY", 8)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg, err := registry.New(cfg.Vhost)
	if err != nil {
		lifecycle.Fatal("registry", err)
	}
	conn := rmq.Dial(cfg.URI(), "csms-go-ocpp-worker")
	w := &worker{heartbeat: env.Int("HEARTBEAT_INTERVAL", 60), registry: reg, presence: rmq.NewPresence(conn)}

	setup, err := conn.Channel()
	if err != nil {
		lifecycle.Fatal("channel", err)
	}
	if err := rmq.DeclareRequestsQueue(setup); err != nil {
		lifecycle.Fatal("declare topology", err)
	}

	h := health.New(map[string]health.Check{"rabbitmq": lifecycle.AMQPConnected(conn), "valkey": reg.Ping})
	mux := http.NewServeMux()
	h.Register(mux)
	server := &http.Server{Addr: ":" + env.String("HTTP_PORT", "8080"), Handler: mux}
	lifecycle.Serve(server)

	// One channel and consumer per goroutine, to use all cores.
	var wg sync.WaitGroup
	consumers := make([]consumer, 0, concurrency)
	for i := range concurrency {
		ch, err := conn.Channel()
		if err != nil {
			lifecycle.Fatal("channel", err)
		}
		if err := ch.Qos(prefetch, 0, false); err != nil {
			lifecycle.Fatal("qos", err)
		}
		tag := "csms-go-worker-" + strconv.Itoa(i)
		deliveries, err := ch.Consume(rmq.RequestsQueue, tag, false, false, false, false, nil)
		if err != nil {
			lifecycle.Fatal("consume", err)
		}
		consumers = append(consumers, consumer{ch, tag})
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.work(ch, deliveries)
			if ctx.Err() == nil {
				lifecycle.Fatal("consumer stopped", errors.New("delivery channel closed"))
			}
		}()
	}
	h.SetReady(true)
	slog.Info("ocpp-worker running", "vhost", cfg.Vhost, "workers", concurrency, "prefetch", prefetch)

	<-ctx.Done()
	// Drain: the broker stops delivering, we finish and acknowledge what we
	// hold, then leave. Nothing is lost: unacknowledged messages would be
	// redelivered to the other replicas anyway.
	h.SetReady(false)
	slog.Info("draining")
	for _, c := range consumers {
		_ = c.ch.Cancel(c.tag, false)
	}
	if !lifecycle.WaitTimeout(&wg, env.ShutdownTimeout()) {
		slog.Warn("drain timed out")
	}
	lifecycle.Shutdown(server, 5*time.Second)
	_ = conn.Close()
	_ = reg.Close()
	slog.Info("stopped")
}

func (w *worker) work(ch *amqp.Channel, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		// Not the signal context: in-flight messages complete during the drain.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.handle(ctx, ch, d); err != nil {
			slog.Error("publish failed", "err", err)
			_ = d.Nack(false, true)
		} else {
			_ = d.Ack(false)
		}
		cancel()
	}
}

// handle answers a charger CALL and records it. SEND frames, malformed frames
// and the plugin's synthetic offline notification get no answer.
func (w *worker) handle(ctx context.Context, ch *amqp.Channel, d amqp.Delivery) error {
	key, ok := ocpp.ParseRoutingKey(d.RoutingKey)
	if !ok || d.ReplyTo == "" {
		return nil
	}
	frame, messageType, messageID, ok := ocpp.Decode(d.Body)
	if !ok || len(frame) < 4 || (messageType != ocpp.Call && messageType != ocpp.Send) {
		return nil
	}
	action, payload := frame.String(2), frame.Object(3)
	if messageType == ocpp.Call && !ocpp.IsSyntheticOffline(action, payload) {
		body, _ := json.Marshal(ocpp.Reply(key.Version, messageID, action, payload, w.heartbeat))
		if err := rmq.Publish(ctx, ch, d.ReplyTo, messageID, replyTTL, body); err != nil {
			return err
		}
	}
	// The registry is a projection: if Valkey is unavailable the charger still
	// gets its answer, and the state converges with its next frames.
	if err := w.registry.Observe(ctx, d.ReplyTo, key.Version, action, payload, w.presence.Connected); err != nil {
		slog.Warn("registry update failed", "charger", d.ReplyTo, "err", err)
	}
	return nil
}
