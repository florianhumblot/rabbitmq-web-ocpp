// Package monitor turns the dashboard's tap on all OCPP traffic into traffic
// statistics: message rates, CSMS reply latency and command outcomes. The
// charger registry is shared state in Valkey, see package registry.
package monitor

import (
	"slices"
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
