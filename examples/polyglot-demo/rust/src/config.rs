use std::{env, time::Duration};

pub struct Config {
    pub amqp_uri: String,
    pub vhost: String,
    pub management_url: String,
    pub user: String,
    pub password: String,
    pub http_port: u16,
    pub heartbeat_interval: u64,
    pub prefetch: u16,
    pub worker_concurrency: usize,
    pub command_timeout: Duration,
}

fn var<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let host = var("RABBITMQ_HOST", "localhost".to_string());
        let port = var("RABBITMQ_PORT", 5672u16);
        let user = var("RABBITMQ_USER", "csms".to_string());
        let password = var("RABBITMQ_PASS", "csms".to_string());
        let vhost = var("RABBITMQ_VHOST", "csms-rust".to_string());
        Self {
            amqp_uri: format!("amqp://{user}:{password}@{host}:{port}/{}", encode(&vhost)),
            management_url: var(
                "RABBITMQ_MANAGEMENT_URL",
                "http://localhost:15672".to_string(),
            ),
            vhost,
            user,
            password,
            http_port: var("HTTP_PORT", 8080),
            heartbeat_interval: var("HEARTBEAT_INTERVAL", 60),
            prefetch: var("PREFETCH", 200),
            worker_concurrency: var("WORKER_CONCURRENCY", 8),
            command_timeout: Duration::from_millis(var("COMMAND_TIMEOUT_MS", 15_000)),
        }
    }
}

/// Percent-encodes a vhost or queue name for URIs and management API paths.
pub fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
