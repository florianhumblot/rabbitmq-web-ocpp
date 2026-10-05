// Package rmq wraps the AMQP 0-9-1 client with the demo topology.
//
// The plugin publishes every charger frame to amq.topic with the routing key
// <protocol>.<Action>.<req|conf|error> and consumes, per charger, a queue
// bound with the charge point id as routing key.
package rmq

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange = "amq.topic"
	// RequestsQueue is the shared work queue: every charger-initiated CALL,
	// consumed by competing workers.
	RequestsQueue = "csms.requests"
	// ResponsesQueue receives the chargers' answers to CSMS commands,
	// consumed by competing command API instances.
	ResponsesQueue = "csms.responses"
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

func declareShared(ch *amqp.Channel, queue string, keys ...string) error {
	// Durable classic queues: the plugin publishes into them directly, and
	// does not yet keep the client state quorum queues need.
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	for _, key := range keys {
		if err := ch.QueueBind(queue, key, Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// DeclareRequestsQueue declares the shared work queue and its binding.
func DeclareRequestsQueue(ch *amqp.Channel) error {
	return declareShared(ch, RequestsQueue, "*.*.req")
}

// DeclareResponsesQueue declares the shared queue of command answers.
func DeclareResponsesQueue(ch *amqp.Channel) error {
	return declareShared(ch, ResponsesQueue, "*.response.conf", "*.response.error")
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

// Presence tells whether a charger is connected right now: the plugin
// consumes the charger's queue (ocpp.<id>) for as long as its WebSocket is
// open, on whichever broker node it is connected to. A passive declare
// returns the consumer count without changing anything.
type Presence struct {
	conn *amqp.Connection
	mu   sync.Mutex
	ch   *amqp.Channel
}

func NewPresence(conn *amqp.Connection) *Presence { return &Presence{conn: conn} }

func (p *Presence) Connected(_ context.Context, chargerID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil || p.ch.IsClosed() {
		ch, err := p.conn.Channel()
		if err != nil {
			return false, err
		}
		p.ch = ch
	}
	q, err := p.ch.QueueDeclarePassive("ocpp."+chargerID, true, false, false, false, nil)
	if err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
			return false, nil // never connected; the broker closed the channel
		}
		return false, err
	}
	return q.Consumers > 0, nil
}
