// Package rmq wraps the AMQP 0-9-1 client with the demo topology.
//
// The plugin publishes every charger frame to amq.topic with the routing key
// <protocol>.<Action>.<req|conf|error> and consumes, per charger, a queue
// bound with the charge point id as routing key.
package rmq

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange = "amq.topic"
	// RequestsQueue is the shared work queue: every charger-initiated CALL,
	// consumed by competing workers.
	RequestsQueue = "csms.requests"
)

// Dial connects, retrying until the broker is up. Any later connection loss
// terminates the process: the orchestrator restarts the service with a clean
// connection, which is the simplest correct recovery strategy.
func Dial(uri, name string) *amqp.Connection {
	for {
		conn, err := amqp.DialConfig(uri, amqp.Config{Properties: amqp.Table{"connection_name": name}})
		if err == nil {
			go func() {
				if err := <-conn.NotifyClose(make(chan *amqp.Error, 1)); err != nil {
					slog.Error("RabbitMQ connection lost", "err", err)
					os.Exit(1)
				}
			}()
			return conn
		}
		slog.Warn("RabbitMQ not reachable yet", "err", err)
		time.Sleep(2 * time.Second)
	}
}

// DeclareRequestsQueue declares the shared work queue and its binding.
func DeclareRequestsQueue(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(RequestsQueue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(RequestsQueue, "*.*.req", Exchange, false, nil)
}

// DeclareTap declares an exclusive, auto-deleted, server-named queue bound to
// amq.topic with the given keys. It is bounded so that a slow consumer can
// never hurt the broker.
func DeclareTap(ch *amqp.Channel, bindings ...string) (string, error) {
	q, err := ch.QueueDeclare("", false, true, true, false, amqp.Table{
		"x-max-length": int32(50_000),
		"x-overflow":   "drop-head",
	})
	if err != nil {
		return "", err
	}
	for _, key := range bindings {
		if err := ch.QueueBind(q.Name, key, Exchange, false, nil); err != nil {
			return "", err
		}
	}
	return q.Name, nil
}

// Publish sends an OCPP frame to a charger: amq.topic, routing key = charge
// point id. No publisher confirms, same semantics as the other implementations.
func Publish(ctx context.Context, ch *amqp.Channel, chargerID, correlationID string, ttl time.Duration, body []byte) error {
	return ch.PublishWithContext(ctx, Exchange, chargerID, false, false, amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Transient,
		CorrelationId: correlationID,
		Expiration:    formatMillis(ttl),
		Body:          body,
	})
}

func formatMillis(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return strconv.FormatInt(d.Milliseconds(), 10)
}
