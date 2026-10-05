//! AMQP side: topology, the OCPP workers and the dashboard's monitoring tap.
//!
//! The plugin publishes every charger frame to `amq.topic` with the routing key
//! `<protocol>.<Action>.<req|conf|error>` and consumes, per charger, a queue
//! bound with the charge point id as routing key.

use crate::{App, ocpp};
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel,
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions,
        BasicPublishOptions, BasicQosOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};

pub const EXCHANGE: &str = "amq.topic";
/// Shared work queue: every charger-initiated CALL, consumed by competing workers.
pub const REQUESTS_QUEUE: &str = "csms.requests";
/// Charger answers to CSMS commands, consumed by competing command APIs.
pub const RESPONSES_QUEUE: &str = "csms.responses";
/// Replies to a charger that went away are useless after a while.
const REPLY_TTL_MS: &str = "60000";

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Declares a shared, durable classic queue (the plugin publishes into it
/// directly and does not yet keep the client state quorum queues need).
async fn declare_shared(channel: &Channel, queue: &str, keys: &[&str]) -> lapin::Result<()> {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions::durable(),
            FieldTable::default(),
        )
        .await?;
    for key in keys {
        channel
            .queue_bind(
                queue.into(),
                EXCHANGE.into(),
                (*key).into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
    }
    Ok(())
}

pub async fn declare_requests_queue(channel: &Channel) -> lapin::Result<()> {
    declare_shared(channel, REQUESTS_QUEUE, &["*.*.req"]).await
}

pub async fn declare_responses_queue(channel: &Channel) -> lapin::Result<()> {
    declare_shared(
        channel,
        RESPONSES_QUEUE,
        &["*.response.conf", "*.response.error"],
    )
    .await
}

pub fn json_properties() -> BasicProperties {
    BasicProperties::default()
        .with_content_type("application/json".into())
        .with_delivery_mode(1)
}

pub async fn publish_to(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
    properties: BasicProperties,
) -> lapin::Result<()> {
    // No publisher confirms: same semantics as the other implementations.
    channel
        .basic_publish(
            exchange.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            body,
            properties,
        )
        .await?;
    Ok(())
}

pub async fn publish(
    channel: &Channel,
    routing_key: &str,
    body: &[u8],
    properties: BasicProperties,
) -> lapin::Result<()> {
    publish_to(channel, EXCHANGE, routing_key, body, properties).await
}

/// Consumes `queue` until the shutdown token fires, then cancels the consumer
/// and keeps handling the deliveries already received until the broker
/// confirms the cancellation: nothing in flight is dropped on a rolling
/// deployment.
pub async fn consume_until_shutdown<F, Fut>(
    app: &App,
    channel: &Channel,
    queue: &str,
    tag: &str,
    no_ack: bool,
    mut handle: F,
) -> lapin::Result<()>
where
    F: FnMut(lapin::message::Delivery) -> Fut,
    Fut: Future<Output = lapin::Result<()>>,
{
    let mut consumer = channel
        .basic_consume(
            queue.into(),
            tag.into(),
            BasicConsumeOptions {
                no_ack,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    let mut draining = false;
    loop {
        tokio::select! {
            _ = app.shutdown.cancelled(), if !draining => {
                draining = true;
                channel.basic_cancel(tag.into(), BasicCancelOptions::default()).await?;
            }
            next = consumer.next() => match next {
                Some(delivery) => handle(delivery?).await?,
                None => {
                    let _ = channel.close(200, "drained".into()).await;
                    return Ok(());
                }
            },
        }
    }
}

/// One stateless OCPP worker: its own channel and consumer on the shared queue.
/// Run several of them to use all cores; run several processes to scale out.
pub async fn run_worker(app: Arc<App>, index: usize) -> lapin::Result<()> {
    let channel = app.conn.create_channel().await?;
    channel
        .basic_qos(app.cfg.prefetch, BasicQosOptions::default())
        .await?;
    let tag = format!("csms-rust-worker-{}-{index}", app.instance);
    consume_until_shutdown(&app, &channel, REQUESTS_QUEUE, &tag, false, |delivery| {
        let (app, channel) = (app.clone(), channel.clone());
        async move {
            match handle_request(&app, &channel, &delivery).await {
                // Acknowledge only after the answer was handed to the broker.
                Ok(()) => delivery.ack(BasicAckOptions::default()).await.map(drop),
                Err(e) => {
                    error!("publish failed: {e}");
                    delivery
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await
                        .map(drop)
                }
            }
        }
    })
    .await
}

/// Answers a charger CALL and records it in the registry. SEND frames,
/// malformed frames and the synthetic offline notification get no answer.
async fn handle_request(
    app: &App,
    channel: &Channel,
    delivery: &lapin::message::Delivery,
) -> lapin::Result<()> {
    let Some(charger_id) = delivery.properties.reply_to().as_ref().map(|s| s.as_str()) else {
        return Ok(());
    };
    let Some(key) = ocpp::parse_routing_key(delivery.routing_key.as_str()) else {
        return Ok(());
    };
    let frame: Value = match serde_json::from_slice(&delivery.data) {
        Ok(frame) => frame,
        Err(e) => {
            warn!(charger_id, "dropping malformed frame: {e}");
            return Ok(());
        }
    };
    let Some(
        [
            Value::Number(message_type),
            Value::String(message_id),
            Value::String(action),
            payload,
            ..,
        ],
    ) = frame.as_array().map(Vec::as_slice)
    else {
        return Ok(());
    };
    let message_type = message_type.as_u64().unwrap_or(0);
    if message_type != ocpp::CALL && message_type != ocpp::SEND {
        return Ok(());
    }
    if message_type == ocpp::CALL && !ocpp::is_synthetic_offline(action, payload) {
        let reply = ocpp::reply(
            key.version,
            message_id,
            action,
            payload,
            app.cfg.heartbeat_interval,
        );
        let properties = json_properties()
            .with_correlation_id(message_id.as_str().into())
            .with_expiration(REPLY_TTL_MS.into());
        let body = serde_json::to_vec(&reply).unwrap_or_default();
        publish(channel, charger_id, &body, properties).await?;
    }
    // The registry is a projection: if Valkey is unavailable the charger still
    // gets its answer, and the state converges with its next frames.
    if let Err(e) = app
        .registry
        .observe(charger_id, key.version, action, payload, || {
            connected(app, charger_id)
        })
        .await
    {
        warn!(charger_id, "registry update failed: {e}");
    }
    Ok(())
}

/// Whether a charger is connected right now: the plugin consumes the charger's
/// queue (`ocpp.<id>`) for as long as its WebSocket is open, on whichever
/// broker node it is connected to. A passive declare returns the consumer
/// count without changing anything; it runs on a throwaway channel because
/// the broker closes the channel when the queue does not exist.
async fn connected(app: &App, charger_id: &str) -> Result<bool, String> {
    let channel = app.conn.create_channel().await.map_err(|e| e.to_string())?;
    let result = channel
        .queue_declare(
            format!("ocpp.{charger_id}").into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    match result {
        Ok(queue) => {
            let _ = channel.close(200, "done".into()).await;
            Ok(queue.consumer_count() > 0)
        }
        Err(e) if e.to_string().contains("NOT_FOUND") => Ok(false),
        Err(e) => Err(e.to_string()),
    }
}

/// Per-instance monitoring tap of the dashboard: an exclusive, auto-deleted
/// copy of all OCPP traffic in the vhost (both directions), bounded so a slow
/// consumer can never hurt the broker.
pub async fn run_tap(app: Arc<App>) -> lapin::Result<()> {
    let channel = app.conn.create_channel().await?;
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
    let tag = format!("csms-rust-tap-{}", app.instance);
    consume_until_shutdown(
        &app,
        &channel,
        queue.name().as_str(),
        &tag,
        true,
        |delivery| {
            on_tap_frame(&app, &delivery);
            async { Ok(()) }
        },
    )
    .await
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
    let mut stats = app.stats.lock().unwrap();

    match ocpp::parse_routing_key(routing_key) {
        Some(key) => {
            let Some(charger_id) = delivery.properties.reply_to().as_ref().map(|s| s.as_str())
            else {
                return;
            };
            if key.direction == "req" {
                let payload = frame.get(3).unwrap_or(&Value::Null);
                let expects_reply =
                    message_type == ocpp::CALL && !ocpp::is_synthetic_offline(key.action, payload);
                stats.charger_request(charger_id, message_id, key.action, kind, expects_reply, now);
            } else {
                let status = (message_type == ocpp::CALLRESULT)
                    .then(|| frame[2].get("status").and_then(Value::as_str))
                    .flatten();
                stats.charger_response(charger_id, message_id, kind, status, now);
            }
        }
        None if message_type == ocpp::CALL => {
            let action = frame[2].as_str().unwrap_or_default();
            stats.csms_call(routing_key, message_id, action, now);
        }
        None => stats.csms_reply(routing_key, message_id, kind, now),
    }
}

/// A long-running AMQP task ended. During a shutdown that is the plan;
/// otherwise exit, and the orchestrator restarts us with a clean connection
/// (simplest correct recovery strategy).
pub async fn supervise(
    app: Arc<App>,
    name: &'static str,
    task: impl Future<Output = lapin::Result<()>>,
) {
    let result = task.await;
    if app.shutdown.is_cancelled() {
        info!("{name} drained");
        return;
    }
    match result {
        Ok(()) => error!("{name} stopped"),
        Err(e) => error!("{name} failed: {e}"),
    }
    std::process::exit(1);
}
