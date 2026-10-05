// Package env reads the configuration shared by all services from the
// environment, with the same variable names as the Java and Rust versions.
package env

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func Millis(key string, def int) time.Duration {
	return time.Duration(Int(key, def)) * time.Millisecond
}

// RabbitMQ holds the broker coordinates.
type RabbitMQ struct {
	Host, User, Password, Vhost, ManagementURL string
	Port                                       int
}

func LoadRabbitMQ() RabbitMQ {
	return RabbitMQ{
		Host:          String("RABBITMQ_HOST", "localhost"),
		Port:          Int("RABBITMQ_PORT", 5672),
		User:          String("RABBITMQ_USER", "csms"),
		Password:      String("RABBITMQ_PASS", "csms"),
		Vhost:         String("RABBITMQ_VHOST", "csms-go"),
		ManagementURL: String("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672"),
	}
}

func (r RabbitMQ) URI() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", url.QueryEscape(r.User), url.QueryEscape(r.Password),
		r.Host, r.Port, url.PathEscape(r.Vhost))
}
