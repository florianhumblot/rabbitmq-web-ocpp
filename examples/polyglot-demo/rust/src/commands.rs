//! Version-neutral command API. A request is translated to the OCPP 1.6 or 2.x
//! payload based on the protocol recorded in the shared registry, or on an
//! explicit `ocppVersion` field in the body.
//!
//! The CALL is published to `amq.topic` with the charge point id as routing
//! key; the plugin queues it in the charger's own queue. Any number of
//! instances can run behind a load balancer: the charger's answer
//! (`<protocol>.response.conf|error`) lands in the shared `csms.responses`
//! queue and may be consumed by any instance. The OCPP message id starts with
//! the id of the instance that sent the command, so an instance receiving
//! somebody else's answer forwards it to that instance's private queue: at
//! most one extra hop, and no instance sees all answers.

use crate::{App, amqp, ocpp};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use lapin::{
    options::{BasicAckOptions, BasicNackOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::Mutex;
use std::{sync::Arc, time::Instant};
use tokio::sync::oneshot;
use tracing::error;

const VERSIONS: [&str; 3] = ["ocpp16", "ocpp201", "ocpp21"];
const TRIGGERABLE: [&str; 4] = [
    "BootNotification",
    "Heartbeat",
    "MeterValues",
    "StatusNotification",
];

const REPLY_QUEUE_PREFIX: &str = "csms.replies.";

type Key = (String, String);

/// Commands waiting for the charger's answer, by (charge point id, message id).
#[derive(Default)]
pub struct Pending(Mutex<HashMap<Key, oneshot::Sender<Value>>>);

impl Pending {
    fn complete(&self, delivery: &lapin::message::Delivery) {
        let (Some(charger), Some(message_id)) = (
            delivery.properties.reply_to(),
            delivery.properties.correlation_id(),
        ) else {
            return;
        };
        let key = (charger.to_string(), message_id.to_string());
        let waiter = self.0.lock().unwrap().remove(&key);
        if let (Some(waiter), Ok(frame)) = (waiter, serde_json::from_slice(&delivery.data)) {
            let _ = waiter.send(frame);
        }
    }
}

/// "<instance>-<random>": 36 characters, the maximum length of an OCPP
/// message id.
fn new_message_id(instance: &str) -> String {
    let random = uuid::Uuid::new_v4().simple().to_string();
    format!("{instance}-{}", &random[..27])
}

/// The instance id at the start of a message id made by `new_message_id`.
fn owner_of(message_id: &str) -> Option<&str> {
    (message_id.len() == 36 && message_id.as_bytes()[8] == b'-').then(|| &message_id[..8])
}

/// Consumes the shared queue of answers until shutdown: completes our own
/// pending commands and forwards the others to their owner's private queue.
/// If that instance is gone, the forward is unroutable and dropped, like its
/// HTTP request.
pub async fn run_responses(app: Arc<App>) -> lapin::Result<()> {
    let channel = app.conn.create_channel().await?;
    amqp::declare_responses_queue(&channel).await?;
    channel.basic_qos(200, BasicQosOptions::default()).await?;
    let tag = format!("csms-rust-api-{}", app.instance);
    amqp::consume_until_shutdown(
        &app,
        &channel,
        amqp::RESPONSES_QUEUE,
        &tag,
        false,
        |delivery| {
            let (app, channel) = (app.clone(), channel.clone());
            async move {
                let message_id = delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|s| s.to_string());
                match message_id.as_deref().and_then(owner_of) {
                    Some(owner) if owner == app.instance => app.pending.complete(&delivery),
                    Some(owner) => {
                        let properties = delivery.properties.clone();
                        let queue = format!("{REPLY_QUEUE_PREFIX}{owner}");
                        if let Err(e) =
                            amqp::publish_to(&channel, "", &queue, &delivery.data, properties).await
                        {
                            error!("forward to {owner} failed: {e}");
                            return delivery
                                .nack(BasicNackOptions {
                                    requeue: true,
                                    ..Default::default()
                                })
                                .await
                                .map(drop);
                        }
                    }
                    None => {}
                }
                delivery.ack(BasicAckOptions::default()).await.map(drop)
            }
        },
    )
    .await
}

/// Consumes this instance's private queue of forwarded answers. It runs until
/// the process exits, so commands in flight during a shutdown still complete.
pub async fn run_replies(app: Arc<App>) -> lapin::Result<()> {
    use futures_util::StreamExt;
    let channel = app.conn.create_channel().await?;
    let queue = format!("{REPLY_QUEUE_PREFIX}{}", app.instance);
    channel
        .queue_declare(
            queue.as_str().into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    let mut consumer = channel
        .basic_consume(
            queue.as_str().into(),
            "".into(),
            lapin::options::BasicConsumeOptions {
                no_ack: true,
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    while let Some(delivery) = consumer.next().await {
        app.pending.complete(&delivery?);
    }
    Ok(())
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

#[derive(Deserialize)]
pub struct ListParams {
    limit: Option<usize>,
}

pub async fn list(State(app): State<Arc<App>>, Query(params): Query<ListParams>) -> Response {
    match app.registry.list(params.limit.unwrap_or(100)).await {
        Ok(views) => Json(views).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

pub async fn get(State(app): State<Arc<App>>, Path(id): Path<String>) -> Response {
    match app.registry.get(&id).await {
        Ok(Some(view)) => Json(view).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, format!("unknown charge point {id}")),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

/// Lenient body parsing: an empty body means "all defaults".
fn parse_body(body: &Bytes) -> Option<Value> {
    if body.is_empty() {
        return Some(json!({}));
    }
    serde_json::from_slice::<Value>(body)
        .ok()
        .filter(Value::is_object)
}

fn bad_body() -> Response {
    error(StatusCode::BAD_REQUEST, "body must be a JSON object")
}

fn str_field<'a>(body: &'a Value, key: &str, default: &'a str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or(default)
}

fn connector_field(body: &Value) -> Option<u64> {
    match body.get("connectorId") {
        None => Some(0),
        Some(v) => v.as_u64(),
    }
}

pub async fn reset(State(app): State<Arc<App>>, Path(id): Path<String>, body: Bytes) -> Response {
    let Some(body) = parse_body(&body) else {
        return bad_body();
    };
    let kind = str_field(&body, "type", "Soft").to_string();
    if kind != "Soft" && kind != "Hard" {
        return error(StatusCode::BAD_REQUEST, "type must be Soft or Hard");
    }
    send(app, id, &body, "Reset", |v2| {
        if v2 {
            json!({ "type": if kind == "Hard" { "Immediate" } else { "OnIdle" } })
        } else {
            json!({ "type": kind })
        }
    })
    .await
}

pub async fn change_availability(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Some(body) = parse_body(&body) else {
        return bad_body();
    };
    let kind = str_field(&body, "type", "Inoperative").to_string();
    let (Some(connector), true) = (
        connector_field(&body),
        kind == "Operative" || kind == "Inoperative",
    ) else {
        return error(
            StatusCode::BAD_REQUEST,
            "type must be Operative or Inoperative, connectorId >= 0",
        );
    };
    send(app, id, &body, "ChangeAvailability", |v2| {
        if v2 {
            let mut p = Map::new();
            p.insert("operationalStatus".into(), json!(kind));
            if connector > 0 {
                p.insert("evse".into(), json!({ "id": connector }));
            }
            Value::Object(p)
        } else {
            json!({ "connectorId": connector, "type": kind })
        }
    })
    .await
}

pub async fn trigger_message(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Some(body) = parse_body(&body) else {
        return bad_body();
    };
    let requested = str_field(&body, "requestedMessage", "StatusNotification").to_string();
    let (Some(connector), true) = (
        connector_field(&body),
        TRIGGERABLE.contains(&requested.as_str()),
    ) else {
        return error(
            StatusCode::BAD_REQUEST,
            format!("requestedMessage must be one of {TRIGGERABLE:?}"),
        );
    };
    send(app, id, &body, "TriggerMessage", |v2| {
        let mut p = Map::new();
        p.insert("requestedMessage".into(), json!(requested));
        if connector > 0 {
            if v2 {
                p.insert("evse".into(), json!({ "id": connector }));
            } else {
                p.insert("connectorId".into(), json!(connector));
            }
        }
        Value::Object(p)
    })
    .await
}

async fn send(
    app: Arc<App>,
    id: String,
    body: &Value,
    action: &str,
    build: impl FnOnce(bool) -> Value,
) -> Response {
    let version = match body.get("ocppVersion").and_then(Value::as_str) {
        Some(v) if VERSIONS.contains(&v) => v.to_string(),
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("ocppVersion must be one of {VERSIONS:?}"),
            );
        }
        None => match app.registry.lookup(&id).await {
            Err(e) => return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            Ok(None) => {
                return error(
                    StatusCode::NOT_FOUND,
                    format!("unknown charge point {id} (pass ocppVersion to force)"),
                );
            }
            Ok(Some((_, false))) => {
                return error(
                    StatusCode::CONFLICT,
                    format!("charge point {id} is offline"),
                );
            }
            Ok(Some((version, true))) => version,
        },
    };
    let payload = build(ocpp::is_v2(&version));
    let message_id = new_message_id(&app.instance);
    let mut result = json!({
        "chargerId": id, "action": action, "ocppVersion": version, "request": payload, "messageId": message_id,
    });

    let key = (id.clone(), message_id.clone());
    let (tx, rx) = oneshot::channel();
    app.pending.0.lock().unwrap().insert(key.clone(), tx);

    let frame = json!([ocpp::CALL, message_id, action, payload]);
    let properties = amqp::json_properties()
        .with_correlation_id(message_id.as_str().into())
        // A command nobody picked up in time must not reach the charger later.
        .with_expiration(app.cfg.command_timeout.as_millis().to_string().into());
    let start = Instant::now();
    if let Err(e) = amqp::publish(
        &app.publisher,
        &id,
        &serde_json::to_vec(&frame).unwrap(),
        properties,
    )
    .await
    {
        app.pending.0.lock().unwrap().remove(&key);
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("publish failed: {e}"),
        );
    }

    let answer = tokio::time::timeout(app.cfg.command_timeout, rx).await;
    app.pending.0.lock().unwrap().remove(&key);
    result["latencyMs"] = json!(start.elapsed().as_millis() as u64);
    match answer {
        Ok(Ok(answer)) if answer[0].as_u64() == Some(ocpp::CALLRESULT) => {
            result["status"] = answer[2].get("status").cloned().unwrap_or(Value::Null);
            result["response"] = answer[2].clone();
            (StatusCode::OK, Json(result)).into_response()
        }
        Ok(Ok(answer)) => {
            result["error"] = json!({ "code": answer[2], "description": answer[3] });
            (StatusCode::BAD_GATEWAY, Json(result)).into_response()
        }
        _ => {
            result["error"] = json!("no answer from the charge point in time");
            (StatusCode::GATEWAY_TIMEOUT, Json(result)).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_carry_their_owner() {
        let id = new_message_id("0a1b2c3d");
        assert_eq!(id.len(), 36);
        assert_eq!(owner_of(&id), Some("0a1b2c3d"));
        assert_eq!(owner_of("m1"), None);
    }
}
