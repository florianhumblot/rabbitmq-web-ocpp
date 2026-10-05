//! Dashboard: the shared page at `/` and one JSON snapshot per second on `/ws`.

use crate::{App, amqp::now_ms, broker::BrokerView, monitor, registry};
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tracing::warn;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot<'a> {
    implementation: &'static str,
    instance: &'a str,
    uptime_seconds: u64,
    timestamp: u64,
    chargers: registry::Summary,
    connectors: BTreeMap<String, u64>,
    traffic: monitor::Traffic,
    latency: monitor::Latency,
    commands: monitor::Commands,
    broker: BrokerView,
    recent: Vec<monitor::RecentFrame>,
}

pub async fn index() -> Html<&'static str> {
    Html(include_str!("../../dashboard/index.html"))
}

/// Builds a snapshot every second and fans it out through a broadcast channel.
pub async fn publish_snapshots(app: Arc<App>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let now = now_ms();
        // Close the statistics window even without viewers, so rates stay per second.
        let window = app.stats.lock().unwrap().window(now);
        if app.snapshots.receiver_count() == 0 {
            continue;
        }
        let (chargers, connectors) = app.registry.summary().await.unwrap_or_else(|e| {
            warn!("registry summary failed: {e}");
            Default::default()
        });
        let snapshot = Snapshot {
            implementation: "rust",
            instance: &app.hostname,
            uptime_seconds: app.started.elapsed().as_secs(),
            timestamp: now,
            chargers,
            connectors,
            traffic: window.traffic,
            latency: window.latency,
            commands: window.commands,
            broker: app.broker.lock().unwrap().clone(),
            recent: window.recent,
        };
        if let Ok(json) = serde_json::to_string(&snapshot) {
            let _ = app.snapshots.send(Arc::from(json));
        }
    }
}

pub async fn ws(State(app): State<Arc<App>>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| stream(app, socket))
}

async fn stream(app: Arc<App>, mut socket: WebSocket) {
    let mut snapshots = app.snapshots.subscribe();
    loop {
        tokio::select! {
            snapshot = snapshots.recv() => match snapshot {
                Ok(json) => {
                    if socket.send(Message::Text(json.as_ref().into())).await.is_err() {
                        return;
                    }
                }
                // A slow viewer just skips snapshots.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => return,
            },
            incoming = socket.recv() => if !matches!(incoming, Some(Ok(_))) { return },
            // On shutdown the browser reconnects to another replica.
            _ = app.shutdown.cancelled() => return,
        }
    }
}
