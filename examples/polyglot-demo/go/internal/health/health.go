// Package health serves the liveness and readiness probes used by the
// orchestrator to route traffic and to pace rolling deployments.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// Check reports whether a dependency is usable.
type Check func(ctx context.Context) error

type Health struct {
	ready  atomic.Bool
	checks map[string]Check
}

func New(checks map[string]Check) *Health { return &Health{checks: checks} }

// SetReady flips readiness: true once consuming, false as soon as shutdown
// starts so that no new HTTP traffic is routed here while draining.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// Register adds GET /healthz/live and GET /healthz/ready.
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("UP\n"))
	})
	mux.HandleFunc("GET /healthz/ready", func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			http.Error(w, "DRAINING or STARTING", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		for name, check := range h.checks {
			if err := check(ctx); err != nil {
				http.Error(w, name+": "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("UP\n"))
	})
}
