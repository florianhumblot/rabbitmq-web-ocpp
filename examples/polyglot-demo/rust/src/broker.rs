//! Polls the RabbitMQ management API for broker-wide figures.

use crate::{App, amqp::REQUESTS_QUEUE, config::encode};
use serde::Serialize;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tracing::debug;

#[derive(Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RequestQueue {
    messages: u64,
    consumers: u64,
    ack_rate: f64,
}

#[derive(Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerView {
    available: bool,
    connections: u64,
    queues: u64,
    publish_rate: f64,
    deliver_rate: f64,
    request_queue: RequestQueue,
    memory_bytes: u64,
    memory_limit_bytes: u64,
    fd_used: u64,
    fd_total: u64,
    erlang_processes: u64,
}

fn num(v: &Value, path: &[&str]) -> f64 {
    path.iter()
        .try_fold(v, |v, k| v.get(k))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

pub async fn poll(app: Arc<App>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("http client");
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tick.tick().await;
        let view = fetch(&app, &client).await.unwrap_or_else(|e| {
            debug!("management API poll failed: {e}");
            BrokerView::default()
        });
        *app.broker.lock().unwrap() = view;
    }
}

async fn fetch(app: &App, client: &reqwest::Client) -> reqwest::Result<BrokerView> {
    let get = |path: String| {
        client
            .get(format!("{}{}", app.cfg.management_url, path))
            .basic_auth(&app.cfg.user, Some(&app.cfg.password))
            .send()
    };
    let overview: Value = get("/api/overview".into())
        .await?
        .error_for_status()?
        .json()
        .await?;
    let queue: Value = get(format!(
        "/api/queues/{}/{}",
        encode(&app.cfg.vhost),
        REQUESTS_QUEUE
    ))
    .await?
    .error_for_status()?
    .json()
    .await?;
    let nodes: Value = get("/api/nodes".into())
        .await?
        .error_for_status()?
        .json()
        .await?;
    let nodes = nodes.as_array().cloned().unwrap_or_default();
    let sum = |key: &str| nodes.iter().map(|n| num(n, &[key]) as u64).sum::<u64>();
    Ok(BrokerView {
        available: true,
        connections: num(&overview, &["object_totals", "connections"]) as u64,
        queues: num(&overview, &["object_totals", "queues"]) as u64,
        publish_rate: num(&overview, &["message_stats", "publish_details", "rate"]),
        deliver_rate: num(&overview, &["message_stats", "deliver_get_details", "rate"]),
        request_queue: RequestQueue {
            messages: num(&queue, &["messages"]) as u64,
            consumers: num(&queue, &["consumers"]) as u64,
            ack_rate: num(&queue, &["message_stats", "ack_details", "rate"]),
        },
        memory_bytes: sum("mem_used"),
        memory_limit_bytes: sum("mem_limit"),
        fd_used: sum("fd_used"),
        fd_total: sum("fd_total"),
        erlang_processes: sum("proc_used"),
    })
}
