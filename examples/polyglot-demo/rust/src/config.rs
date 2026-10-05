use std::{env, time::Duration};

/// The parts of the CSMS a process runs. One binary, deployed once with all
/// roles for small setups, or as separate deployments scaled independently.
#[derive(Clone, Copy)]
pub struct Roles {
    /// Answers charger requests from the shared queue and records them.
    pub worker: bool,
    /// Command API.
    pub api: bool,
    /// Dashboard page and snapshot WebSocket.
    pub dashboard: bool,
}

pub struct Config {
    pub roles: Roles,
    pub amqp_uri: String,
    pub vhost: String,
    pub management_url: String,
    pub user: String,
    pub password: String,
    pub valkey_url: String,
    pub http_port: u16,
    pub heartbeat_interval: u64,
    pub prefetch: u16,
    pub worker_concurrency: usize,
    pub command_timeout: Duration,
    /// Where the dashboard forwards /api/ when this process has no API role.
    pub command_api_url: String,
    pub shutdown_timeout: Duration,
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
        let roles = var("CSMS_ROLES", "worker,api,dashboard".to_string());
        let has = |role: &str| roles.split(',').any(|r| r.trim() == role);
        Self {
            roles: Roles {
                worker: has("worker"),
                api: has("api"),
                dashboard: has("dashboard"),
            },
            amqp_uri: format!("amqp://{user}:{password}@{host}:{port}/{}", encode(&vhost)),
            management_url: var(
                "RABBITMQ_MANAGEMENT_URL",
                "http://localhost:15672".to_string(),
            ),
            valkey_url: format!(
                "redis://{}:{}/",
                var("VALKEY_HOST", "localhost".to_string()),
                var("VALKEY_PORT", 6379u16)
            ),
            vhost,
            user,
            password,
            http_port: var("HTTP_PORT", 8080),
            heartbeat_interval: var("HEARTBEAT_INTERVAL", 60),
            prefetch: var("PREFETCH", 200),
            worker_concurrency: var("WORKER_CONCURRENCY", 8),
            command_timeout: Duration::from_millis(var("COMMAND_TIMEOUT_MS", 15_000)),
            command_api_url: var("COMMAND_API_URL", "http://localhost:8081".to_string()),
            shutdown_timeout: Duration::from_millis(var("SHUTDOWN_TIMEOUT_MS", 25_000)),
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
