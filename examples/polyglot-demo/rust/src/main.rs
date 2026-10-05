//! OCPP CSMS on top of rabbitmq-web-ocpp, as a single async binary.
//!
//! One tokio runtime hosts every component as a task sharing one `App`:
//! * N OCPP workers answering charger requests from the shared queue
//! * the monitoring tap feeding the registry, statistics and command gateway
//! * the management API poller
//! * the HTTP server: command API, dashboard page and snapshot WebSocket

mod amqp;
mod broker;
mod commands;
mod config;
mod dashboard;
mod monitor;
mod ocpp;

use axum::{
    Router,
    routing::{get, post},
};
use lapin::{Channel, Connection, ConnectionProperties};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, oneshot};
use tracing::{info, warn};

pub struct App {
    cfg: config::Config,
    registry: Mutex<monitor::Registry>,
    stats: Mutex<monitor::Stats>,
    /// Commands waiting for the charger's answer, by (charge point id, message id).
    pending: Mutex<HashMap<(String, String), oneshot::Sender<Value>>>,
    publisher: Channel,
    broker: Mutex<broker::BrokerView>,
    snapshots: broadcast::Sender<Arc<str>>,
    started: Instant,
    instance: String,
}

async fn connect(uri: &str) -> Connection {
    loop {
        match Connection::connect(
            uri,
            ConnectionProperties::default().with_connection_name("csms-rust".into()),
        )
        .await
        {
            Ok(conn) => return conn,
            Err(e) => {
                warn!("RabbitMQ not reachable yet: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cfg = config::Config::from_env();

    let conn = Arc::new(connect(&cfg.amqp_uri).await);
    let publisher = conn.create_channel().await.expect("channel");
    amqp::declare_requests_queue(&publisher)
        .await
        .expect("declare topology");
    info!(vhost = cfg.vhost, "connected to RabbitMQ");

    let app = Arc::new(App {
        registry: Mutex::default(),
        stats: Mutex::default(),
        pending: Mutex::default(),
        publisher,
        broker: Mutex::default(),
        snapshots: broadcast::channel(4).0,
        started: Instant::now(),
        instance: std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into()),
        cfg,
    });

    for i in 0..app.cfg.worker_concurrency {
        tokio::spawn(amqp::supervise(
            "worker",
            amqp::run_worker(app.clone(), conn.clone(), i),
        ));
    }
    tokio::spawn(amqp::supervise(
        "tap",
        amqp::run_tap(app.clone(), conn.clone()),
    ));
    tokio::spawn(broker::poll(app.clone()));
    tokio::spawn(dashboard::publish_snapshots(app.clone()));

    let router = Router::new()
        .route("/", get(dashboard::index))
        .route("/ws", get(dashboard::ws))
        .route("/api/chargers", get(commands::list))
        .route("/api/chargers/{id}", get(commands::get))
        .route("/api/chargers/{id}/reset", post(commands::reset))
        .route(
            "/api/chargers/{id}/change-availability",
            post(commands::change_availability),
        )
        .route(
            "/api/chargers/{id}/trigger-message",
            post(commands::trigger_message),
        )
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", app.cfg.http_port))
        .await
        .expect("bind");
    info!(
        port = app.cfg.http_port,
        workers = app.cfg.worker_concurrency,
        "csms-rust listening"
    );
    axum::serve(listener, router).await.expect("http server");
}
