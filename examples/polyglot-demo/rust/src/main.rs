//! OCPP CSMS on top of rabbitmq-web-ocpp, as a single async binary.
//!
//! One tokio runtime hosts the components as tasks sharing one `App`, each
//! enabled by a role (`CSMS_ROLES`, all three by default):
//! * `worker`: N OCPP workers answering charger requests from the shared queue
//!   and recording them in the shared registry
//! * `api`: the command API and its two answer consumers
//! * `dashboard`: the monitoring tap, the management API poller, the page and
//!   the snapshot WebSocket
//!
//! All state shared between instances lives in RabbitMQ and Valkey, so any
//! role can run as many replicas as needed. On SIGTERM the process stops
//! taking work, finishes what it holds, and exits.

mod amqp;
mod broker;
mod commands;
mod config;
mod dashboard;
mod monitor;
mod ocpp;
mod registry;
mod web;

use axum::{
    Router,
    routing::{any, get, post},
};
use lapin::{Channel, Connection, ConnectionProperties};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct App {
    cfg: config::Config,
    /// Random id of this process; prefixes the message ids of its commands.
    instance: String,
    hostname: String,
    conn: Arc<Connection>,
    publisher: Channel,
    registry: registry::Registry,
    pending: commands::Pending,
    stats: Mutex<monitor::Stats>,
    broker: Mutex<broker::BrokerView>,
    snapshots: broadcast::Sender<Arc<str>>,
    http: reqwest::Client,
    started: Instant,
    ready: AtomicBool,
    /// Fired on SIGTERM: stop taking new work and drain.
    shutdown: CancellationToken,
}

async fn retry<T, E: std::fmt::Display, F: Future<Output = Result<T, E>>>(
    what: &str,
    mut f: impl FnMut() -> F,
) -> T {
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) => {
                warn!("{what} not reachable yet: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
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
    let roles = cfg.roles;
    let instance = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();

    let conn = Arc::new(
        retry("RabbitMQ", || {
            Connection::connect(
                &cfg.amqp_uri,
                ConnectionProperties::default()
                    .with_connection_name(format!("csms-rust-{instance}").into()),
            )
        })
        .await,
    );
    let publisher = conn.create_channel().await.expect("channel");
    amqp::declare_requests_queue(&publisher)
        .await
        .expect("declare topology");
    let registry = retry("Valkey", || {
        registry::Registry::connect(&cfg.valkey_url, &cfg.vhost)
    })
    .await;
    info!(
        vhost = cfg.vhost,
        instance, "connected to RabbitMQ and Valkey"
    );

    let app = Arc::new(App {
        instance,
        hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into()),
        conn,
        publisher,
        registry,
        pending: Default::default(),
        stats: Mutex::default(),
        broker: Mutex::default(),
        snapshots: broadcast::channel(4).0,
        http: reqwest::Client::new(),
        started: Instant::now(),
        ready: AtomicBool::new(false),
        shutdown: CancellationToken::new(),
        cfg,
    });

    // Tasks that must drain before the process exits.
    let mut draining = JoinSet::new();
    let mut router = Router::new()
        .route("/healthz/live", get(web::live))
        .route("/healthz/ready", get(web::ready));
    if roles.worker {
        for i in 0..app.cfg.worker_concurrency {
            draining.spawn(amqp::supervise(
                app.clone(),
                "worker",
                amqp::run_worker(app.clone(), i),
            ));
        }
    }
    if roles.api {
        draining.spawn(amqp::supervise(
            app.clone(),
            "responses consumer",
            commands::run_responses(app.clone()),
        ));
        // Not drained: keeps completing in-flight commands until the end.
        tokio::spawn(amqp::supervise(
            app.clone(),
            "replies consumer",
            commands::run_replies(app.clone()),
        ));
        router = router
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
            );
    }
    if roles.dashboard {
        draining.spawn(amqp::supervise(
            app.clone(),
            "tap",
            amqp::run_tap(app.clone()),
        ));
        tokio::spawn(broker::poll(app.clone()));
        tokio::spawn(dashboard::publish_snapshots(app.clone()));
        router = router
            .route("/", get(dashboard::index))
            .route("/ws", get(dashboard::ws));
        if !roles.api {
            router = router.route("/api/{*path}", any(web::proxy));
        }
    }
    let router = router.with_state(app.clone());

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", app.cfg.http_port))
        .await
        .expect("bind");
    app.ready.store(true, Ordering::Relaxed);
    info!(
        port = app.cfg.http_port,
        worker = roles.worker,
        api = roles.api,
        dashboard = roles.dashboard,
        "csms-rust ready"
    );

    let shutdown = app.shutdown.clone();
    let on_signal = {
        let app = app.clone();
        async move {
            signal().await;
            info!("draining");
            // Fail readiness first so no new traffic is routed here.
            app.ready.store(false, Ordering::Relaxed);
            shutdown.cancel();
        }
    };
    // Waits for the requests in flight, which may still need the replies
    // consumer to complete.
    axum::serve(listener, router)
        .with_graceful_shutdown(on_signal)
        .await
        .expect("http server");
    if tokio::time::timeout(app.cfg.shutdown_timeout, draining.join_all())
        .await
        .is_err()
    {
        warn!("drain timed out");
    }
    let _ = app.conn.close(200, "shutdown".into()).await;
    info!("stopped");
}
