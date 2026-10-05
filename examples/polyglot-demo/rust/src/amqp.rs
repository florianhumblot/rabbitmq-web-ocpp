//! AMQP side: topology, the OCPP workers and the monitoring tap.
//!
//! The plugin publishes every charger frame to `amq.topic` with the routing key
//! `<protocol>.<Action>.<req|conf|error>` and consumes, per charger, a queue
//! bound with the charge point id as routing key.

use crate::{App, ocpp};
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, warn};

pub const EXCHANGE: &str = "amq.topic";
/// Shared work queue: every charger-initiated CALL, consumed by competing workers.
pub const REQUESTS_QUEUE: &str = "csms.requests";
/// Replies to a charger that went away are useless after a while.
const REPLY_TTL_MS: &str = "60000";

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

pub async fn declare_requests_queue(channel: &Channel) -> lapin::Result<()> {
    channel
        .queue_declare(
            REQUESTS_QUEUE.into(),
            QueueDeclareOptions::durable(),
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            REQUESTS_QUEUE.into(),
            EXCHANGE.into(),
            "*.*.req".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

pub fn json_properties() -> BasicProperties {
    BasicProperties::default()
        .with_content_type("application/json".into())
        .with_delivery_mode(1)
}

pub async fn publish(
    channel: &Channel,
    routing_key: &str,
    body: &[u8],
    properties: BasicProperties,
) -> lapin::Result<()> {
    // No publisher confirms: same semantics as the other implementations.
    channel
        .basic_publish(
            EXCHANGE.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            body,
            properties,
        )
        .await?;
    Ok(())
}

/// One stateless OCPP worker: its own channel and consumer on the shared queue.
/// Run several of them to use all cores; run several processes to scale out.
pub async fn run_worker(app: Arc<App>, conn: Arc<Connection>, index: usize) -> lapin::Result<()> {
    let channel = conn.create_channel().await?;
    channel
        .basic_qos(app.cfg.prefetch, BasicQosOptions::default())
        .await?;
    let mut consumer = channel
        .basic_consume(
            REQUESTS_QUEUE.into(),
            format!("csms-rust-worker-{index}").into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        if let Some((charger_id, reply)) = answer(&app, &delivery) {
            publish(&channel, &charger_id, &reply.0, reply.1).await?;
        }
        // Acknowledge only after the answer was handed to the broker.
        delivery.ack(BasicAckOptions::default()).await?;
    }
    Ok(())
}

/// Builds the answer to a charger CALL, or `None` when nothing must be sent
/// back (SEND frames, malformed frames, the synthetic offline notification).
fn answer(
    app: &App,
    delivery: &lapin::message::Delivery,
) -> Option<(String, (Vec<u8>, BasicProperties))> {
    let charger_id = delivery
        .properties
        .reply_to()
        .as_ref()?
        .as_str()
        .to_string();
    let key = ocpp::parse_routing_key(delivery.routing_key.as_str())?;
    let frame: Value = match serde_json::from_slice(&delivery.data) {
        Ok(frame) => frame,
        Err(e) => {
            warn!(charger_id, "dropping malformed frame: {e}");
            return None;
        }
    };
    let frame = frame.as_array()?;
    if frame.len() < 4 || frame[0].as_u64() != Some(ocpp::CALL) {
        return None;
    }
    let (message_id, action, payload) = (frame[1].as_str()?, frame[2].as_str()?, &frame[3]);
    if ocpp::is_synthetic_offline(action, payload) {
        return None;
    }
    let reply = ocpp::reply(
        key.version,
        message_id,
        action,
        payload,
        app.cfg.heartbeat_interval,
    );
    let properties = json_properties()
        .with_correlation_id(message_id.into())
        .with_expiration(REPLY_TTL_MS.into());
    Some((charger_id, (serde_json::to_vec(&reply).ok()?, properties)))
}

/// Per-instance monitoring tap: an exclusive, auto-deleted copy of all OCPP
/// traffic in the vhost (both directions), bounded so a slow consumer can
/// never hurt the broker. Feeds the registry, the statistics and the command
/// gateway.
pub async fn run_tap(app: Arc<App>, conn: Arc<Connection>) -> lapin::Result<()> {
    let channel = conn.create_channel().await?;
    let mut args = FieldTable::default();
    args.insert("x-max-length".into(), AMQPValue::LongInt(50_000));
    args.insert(
        "x-overflow".into(),
        AMQPValue::LongString("drop-head".into()),
    );
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            args,
        )
        .await?;
    channel
        .queue_bind(
            queue.name().clone(),
            EXCHANGE.into(),
            "#".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    channel.basic_qos(1000, BasicQosOptions::default()).await?;
    let mut consumer = channel
        .basic_consume(
            queue.name().clone(),
            "csms-rust-tap".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        on_tap_frame(&app, &delivery);
    }
    Ok(())
}

/// Two kinds of frames cross `amq.topic`:
/// * charger → CSMS: routing key `ocpp16.Heartbeat.req`, reply_to = charge
///   point id, correlation_id = OCPP message id
/// * CSMS → charger: routing key = charge point id
fn on_tap_frame(app: &App, delivery: &lapin::message::Delivery) {
    let Ok(Value::Array(frame)) = serde_json::from_slice::<Value>(&delivery.data) else {
        return;
    };
    if frame.len() < 3 {
        return;
    }
    let now = now_ms();
    let message_type = frame[0].as_u64().unwrap_or(0);
    let message_id = frame[1].as_str().unwrap_or_default();
    let kind = ocpp::kind_name(message_type);
    let routing_key = delivery.routing_key.as_str();

    match ocpp::parse_routing_key(routing_key) {
        Some(key) => {
            let Some(charger_id) = delivery.properties.reply_to().as_ref().map(|s| s.as_str())
            else {
                return;
            };
            if key.direction == "req" {
                let payload = frame.get(3).unwrap_or(&Value::Null);
                app.registry.lock().unwrap().on_charger_request(
                    charger_id,
                    key.version,
                    key.action,
                    payload,
                    now,
                );
                let expects_reply =
                    message_type == ocpp::CALL && !ocpp::is_synthetic_offline(key.action, payload);
                app.stats.lock().unwrap().charger_request(
                    charger_id,
                    message_id,
                    key.action,
                    kind,
                    expects_reply,
                    now,
                );
            } else {
                app.registry
                    .lock()
                    .unwrap()
                    .on_charger_response(charger_id, key.version, now);
                let status = (message_type == ocpp::CALLRESULT)
                    .then(|| frame[2].get("status").and_then(Value::as_str))
                    .flatten();
                app.stats
                    .lock()
                    .unwrap()
                    .charger_response(charger_id, message_id, kind, status, now);
                let waiter = app
                    .pending
                    .lock()
                    .unwrap()
                    .remove(&(charger_id.to_string(), message_id.to_string()));
                if let Some(waiter) = waiter {
                    let _ = waiter.send(Value::Array(frame));
                }
            }
        }
        None if message_type == ocpp::CALL => {
            let action = frame[2].as_str().unwrap_or_default();
            app.stats
                .lock()
                .unwrap()
                .csms_call(routing_key, message_id, action, now);
        }
        None => app
            .stats
            .lock()
            .unwrap()
            .csms_reply(routing_key, message_id, kind, now),
    }
}

/// Exit when a long-running AMQP task stops: the container restarts us with a
/// clean connection (simplest correct recovery strategy).
pub async fn supervise(name: &'static str, task: impl Future<Output = lapin::Result<()>>) {
    match task.await {
        Ok(()) => error!("{name} stopped"),
        Err(e) => error!("{name} failed: {e}"),
    }
    std::process::exit(1);
}
