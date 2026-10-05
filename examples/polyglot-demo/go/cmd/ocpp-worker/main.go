// Command ocpp-worker answers charger-initiated OCPP CALLs.
//
// It is stateless: it consumes the shared csms.requests queue and publishes
// each answer to amq.topic with the charge point id as routing key, which the
// plugin delivers down the charger's WebSocket. Scale it with replicas.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/ocpp"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/rmq"
)

// Replies to a charger that went away are useless after a while.
const replyTTL = 60 * time.Second

func main() {
	cfg := env.LoadRabbitMQ()
	heartbeat := env.Int("HEARTBEAT_INTERVAL", 60)
	prefetch := env.Int("PREFETCH", 200)
	concurrency := env.Int("WORKER_CONCURRENCY", 8)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn := rmq.Dial(cfg.URI(), "csms-go-ocpp-worker")
	defer conn.Close()
	setup, err := conn.Channel()
	if err != nil {
		slog.Error("channel", "err", err)
		os.Exit(1)
	}
	if err := rmq.DeclareRequestsQueue(setup); err != nil {
		slog.Error("declare topology", "err", err)
		os.Exit(1)
	}

	// One channel and consumer per goroutine, to use all cores.
	for i := range concurrency {
		ch, err := conn.Channel()
		if err != nil {
			slog.Error("channel", "err", err)
			os.Exit(1)
		}
		if err := ch.Qos(prefetch, 0, false); err != nil {
			slog.Error("qos", "err", err)
			os.Exit(1)
		}
		deliveries, err := ch.Consume(rmq.RequestsQueue, "csms-go-worker-"+strconv.Itoa(i), false, false, false, false, nil)
		if err != nil {
			slog.Error("consume", "err", err)
			os.Exit(1)
		}
		go work(ctx, ch, deliveries, heartbeat)
	}
	slog.Info("ocpp-worker running", "vhost", cfg.Vhost, "workers", concurrency, "prefetch", prefetch)
	<-ctx.Done()
}

func work(ctx context.Context, ch *amqp.Channel, deliveries <-chan amqp.Delivery, heartbeat int) {
	for d := range deliveries {
		if chargerID, reply, ok := answer(d, heartbeat); ok {
			body, _ := json.Marshal(reply)
			if err := rmq.Publish(ctx, ch, chargerID, d.CorrelationId, replyTTL, body); err != nil {
				slog.Error("publish failed", "err", err)
				_ = d.Nack(false, true)
				continue
			}
		}
		// Acknowledge only after the answer was handed to the broker.
		_ = d.Ack(false)
	}
	slog.Error("consumer stopped")
	os.Exit(1)
}

// answer builds the reply to a charger CALL, or reports false when nothing
// must be sent back (SEND frames, malformed frames, the synthetic offline
// notification).
func answer(d amqp.Delivery, heartbeat int) (string, []any, bool) {
	key, ok := ocpp.ParseRoutingKey(d.RoutingKey)
	if !ok || d.ReplyTo == "" {
		return "", nil, false
	}
	frame, messageType, messageID, ok := ocpp.Decode(d.Body)
	if !ok || messageType != ocpp.Call || len(frame) < 4 {
		return "", nil, false
	}
	action, payload := frame.String(2), frame.Object(3)
	if ocpp.IsSyntheticOffline(action, payload) {
		return "", nil, false
	}
	return d.ReplyTo, ocpp.Reply(key.Version, messageID, action, payload, heartbeat), true
}
