// Package broker polls the RabbitMQ management API for broker-wide figures.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/env"
	"github.com/vampirebyte/rabbitmq-web-ocpp/examples/polyglot-demo/go/internal/rmq"
)

type RequestQueue struct {
	Messages  int64   `json:"messages"`
	Consumers int64   `json:"consumers"`
	AckRate   float64 `json:"ackRate"`
}

type View struct {
	Available        bool         `json:"available"`
	Connections      int64        `json:"connections"`
	Queues           int64        `json:"queues"`
	PublishRate      float64      `json:"publishRate"`
	DeliverRate      float64      `json:"deliverRate"`
	RequestQueue     RequestQueue `json:"requestQueue"`
	MemoryBytes      int64        `json:"memoryBytes"`
	MemoryLimitBytes int64        `json:"memoryLimitBytes"`
	FdUsed           int64        `json:"fdUsed"`
	FdTotal          int64        `json:"fdTotal"`
	ErlangProcesses  int64        `json:"erlangProcesses"`
}

type rate struct {
	Rate float64 `json:"rate"`
}

type Poller struct {
	cfg     env.RabbitMQ
	client  *http.Client
	current atomic.Pointer[View]
}

func NewPoller(cfg env.RabbitMQ) *Poller {
	p := &Poller{cfg: cfg, client: &http.Client{Timeout: 5 * time.Second}}
	p.current.Store(&View{})
	return p
}

func (p *Poller) Current() View { return *p.current.Load() }

// Run polls every two seconds until the context ends.
func (p *Poller) Run(ctx context.Context) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		view, err := p.fetch(ctx)
		if err != nil {
			slog.Debug("management API poll failed", "err", err)
			view = View{}
		}
		p.current.Store(&view)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *Poller) get(ctx context.Context, path string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.ManagementURL+path, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(p.cfg.User, p.cfg.Password)
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", path, res.Status)
	}
	return json.NewDecoder(res.Body).Decode(into)
}

func (p *Poller) fetch(ctx context.Context) (View, error) {
	var overview struct {
		ObjectTotals struct {
			Connections int64 `json:"connections"`
			Queues      int64 `json:"queues"`
		} `json:"object_totals"`
		MessageStats struct {
			Publish    rate `json:"publish_details"`
			DeliverGet rate `json:"deliver_get_details"`
		} `json:"message_stats"`
	}
	var queue struct {
		Messages     int64 `json:"messages"`
		Consumers    int64 `json:"consumers"`
		MessageStats struct {
			Ack rate `json:"ack_details"`
		} `json:"message_stats"`
	}
	var nodes []struct {
		MemUsed  int64 `json:"mem_used"`
		MemLimit int64 `json:"mem_limit"`
		FdUsed   int64 `json:"fd_used"`
		FdTotal  int64 `json:"fd_total"`
		ProcUsed int64 `json:"proc_used"`
	}
	if err := p.get(ctx, "/api/overview", &overview); err != nil {
		return View{}, err
	}
	if err := p.get(ctx, "/api/queues/"+url.PathEscape(p.cfg.Vhost)+"/"+rmq.RequestsQueue, &queue); err != nil {
		return View{}, err
	}
	if err := p.get(ctx, "/api/nodes", &nodes); err != nil {
		return View{}, err
	}
	v := View{
		Available:    true,
		Connections:  overview.ObjectTotals.Connections,
		Queues:       overview.ObjectTotals.Queues,
		PublishRate:  overview.MessageStats.Publish.Rate,
		DeliverRate:  overview.MessageStats.DeliverGet.Rate,
		RequestQueue: RequestQueue{queue.Messages, queue.Consumers, queue.MessageStats.Ack.Rate},
	}
	for _, n := range nodes {
		v.MemoryBytes += n.MemUsed
		v.MemoryLimitBytes += n.MemLimit
		v.FdUsed += n.FdUsed
		v.FdTotal += n.FdTotal
		v.ErlangProcesses += n.ProcUsed
	}
	return v, nil
}
