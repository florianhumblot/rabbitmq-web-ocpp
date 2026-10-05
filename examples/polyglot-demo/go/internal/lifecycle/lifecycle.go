// Package lifecycle holds the start and shutdown plumbing shared by the
// services: fatal errors, bounded waits and the AMQP health check.
package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Fatal logs and exits; the orchestrator restarts the process.
func Fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

// WaitTimeout waits for wg, reporting false if the timeout expired first.
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// AMQPConnected is a readiness check on the broker connection.
func AMQPConnected(conn *amqp.Connection) func(context.Context) error {
	return func(context.Context) error {
		if conn.IsClosed() {
			return errors.New("connection closed")
		}
		return nil
	}
}

// Serve runs an HTTP server in the background.
func Serve(server *http.Server) {
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			Fatal("http server", err)
		}
	}()
}

// Shutdown stops accepting connections and waits for in-flight requests.
func Shutdown(server *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("http shutdown", "err", err)
	}
}
