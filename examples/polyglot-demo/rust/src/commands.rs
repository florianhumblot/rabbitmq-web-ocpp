//! Version-neutral command API. A request is translated to the OCPP 1.6 or 2.x
//! payload based on the protocol the charger connected with (learned from its
//! traffic), or on an explicit `ocppVersion` field in the body.
//!
//! The CALL is published to `amq.topic` with the charge point id as routing
//! key; the plugin queues it in the charger's own queue. The answer comes back
//! as `<protocol>.response.conf|error` and is handed over by the tap. Every
//! instance taps all answers, so only the one holding the pending call
//! completes it.

use crate::{App, amqp, ocpp};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{sync::Arc, time::Instant};
use tokio::sync::oneshot;

const VERSIONS: [&str; 3] = ["ocpp16", "ocpp201", "ocpp21"];
const TRIGGERABLE: [&str; 4] = [
    "BootNotification",
    "Heartbeat",
    "MeterValues",
    "StatusNotification",
];

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

#[derive(Deserialize)]
pub struct ListParams {
    limit: Option<usize>,
}

pub async fn list(State(app): State<Arc<App>>, Query(params): Query<ListParams>) -> Response {
    Json(
        app.registry
            .lock()
            .unwrap()
            .list(params.limit.unwrap_or(100)),
    )
    .into_response()
}

pub async fn get(State(app): State<Arc<App>>, Path(id): Path<String>) -> Response {
    match app.registry.lock().unwrap().view_of(&id) {
        Some(view) => Json(view).into_response(),
        None => error(StatusCode::NOT_FOUND, format!("unknown charge point {id}")),
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
        None => match app.registry.lock().unwrap().get(&id) {
            None => {
                return error(
                    StatusCode::NOT_FOUND,
                    format!("unknown charge point {id} (pass ocppVersion to force)"),
                );
            }
            Some(c) if !c.online => {
                return error(
                    StatusCode::CONFLICT,
                    format!("charge point {id} is offline"),
                );
            }
            Some(c) => c.version.clone(),
        },
    };
    let payload = build(ocpp::is_v2(&version));
    let message_id = uuid::Uuid::new_v4().to_string();
    let mut result = json!({
        "chargerId": id, "action": action, "ocppVersion": version, "request": payload, "messageId": message_id,
    });

    let key = (id.clone(), message_id.clone());
    let (tx, rx) = oneshot::channel();
    app.pending.lock().unwrap().insert(key.clone(), tx);

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
        app.pending.lock().unwrap().remove(&key);
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("publish failed: {e}"),
        );
    }

    let answer = tokio::time::timeout(app.cfg.command_timeout, rx).await;
    app.pending.lock().unwrap().remove(&key);
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
