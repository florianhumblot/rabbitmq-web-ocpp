//! OCPP-J framing helpers and the CSMS answers to charger-initiated CALLs.

use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use time::{OffsetDateTime, macros::format_description};

pub const CALL: u64 = 2;
pub const CALLRESULT: u64 = 3;
pub const CALLERROR: u64 = 4;
pub const CALLRESULTERROR: u64 = 5;
pub const SEND: u64 = 6;

/// Routing key of a frame published by a charger: `ocpp16.Heartbeat.req`.
pub struct ChargerKey<'a> {
    pub version: &'a str,
    pub action: &'a str,
    pub direction: &'a str,
}

/// Returns `None` for frames published by the CSMS (routing key = charge point id).
pub fn parse_routing_key(key: &str) -> Option<ChargerKey<'_>> {
    let mut parts = key.split('.');
    let (version, action, direction) = (parts.next()?, parts.next()?, parts.next()?);
    let valid = parts.next().is_none()
        && version.len() > 4
        && version.starts_with("ocpp")
        && version[4..].bytes().all(|b| b.is_ascii_digit())
        && !action.is_empty()
        && action.bytes().all(|b| b.is_ascii_alphanumeric())
        && matches!(direction, "req" | "conf" | "error");
    valid.then_some(ChargerKey {
        version,
        action,
        direction,
    })
}

/// OCPP 2.x (ocpp20, ocpp201, ocpp21) versus the 1.x payload family.
pub fn is_v2(version: &str) -> bool {
    version.starts_with("ocpp2")
}

/// The demo encodes the security profile in the charge point id (`...-sp3`).
pub fn security_profile(charger_id: &str) -> Option<String> {
    let (_, tail) = charger_id.rsplit_once("-sp")?;
    (tail.len() == 1 && tail.as_bytes()[0].is_ascii_digit()).then(|| tail.to_string())
}

pub fn kind_name(message_type: u64) -> &'static str {
    match message_type {
        CALL => "CALL",
        CALLRESULT => "CALLRESULT",
        CALLERROR => "CALLERROR",
        CALLRESULTERROR => "CALLRESULTERROR",
        SEND => "SEND",
        _ => "UNKNOWN",
    }
}

/// The plugin publishes one synthetic StatusNotification when a charger
/// disconnects. It must not be answered: the charger is gone.
pub fn is_synthetic_offline(action: &str, payload: &Value) -> bool {
    if action != "StatusNotification" {
        return false;
    }
    let source = payload.get("customData").unwrap_or(payload);
    source.get("vendorId").and_then(Value::as_str) == Some("rabbitmq")
        && source.get("vendorErrorCode").and_then(Value::as_str) == Some("Offline")
}

pub fn now() -> String {
    OffsetDateTime::now_utc()
        .format(format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))
        .unwrap_or_default()
}

static TRANSACTION_IDS: AtomicU64 = AtomicU64::new(0);

/// Builds the complete CALLRESULT or CALLERROR frame answering a charger CALL.
pub fn reply(
    version: &str,
    message_id: &str,
    action: &str,
    payload: &Value,
    heartbeat_interval: u64,
) -> Value {
    let accepted = json!({ "status": "Accepted" });
    let result = match action {
        "BootNotification" => Some(json!({
            "status": "Accepted", "currentTime": now(), "interval": heartbeat_interval
        })),
        "Heartbeat" => Some(json!({ "currentTime": now() })),
        "Authorize" if is_v2(version) => Some(json!({ "idTokenInfo": accepted })),
        "Authorize" => Some(json!({ "idTagInfo": accepted })),
        "StartTransaction" => Some(json!({
            "transactionId": TRANSACTION_IDS.fetch_add(1, Ordering::Relaxed) + 1,
            "idTagInfo": accepted
        })),
        "StopTransaction" => Some(json!({ "idTagInfo": accepted })),
        "TransactionEvent" if payload.get("idToken").is_some() => {
            Some(json!({ "idTokenInfo": accepted }))
        }
        "TransactionEvent" => Some(json!({})),
        "DataTransfer" => Some(accepted),
        "StatusNotification"
        | "MeterValues"
        | "DiagnosticsStatusNotification"
        | "FirmwareStatusNotification"
        | "SecurityEventNotification"
        | "LogStatusNotification"
        | "NotifyReport"
        | "NotifyEvent"
        | "NotifyMonitoringReport" => Some(json!({})),
        _ => None,
    };
    match result {
        Some(result) => json!([CALLRESULT, message_id, result]),
        None => json!([
            CALLERROR,
            message_id,
            "NotImplemented",
            format!("Action {action} is not supported by this CSMS"),
            {}
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_keys() {
        let k = parse_routing_key("ocpp201.BootNotification.req").unwrap();
        assert_eq!(
            (k.version, k.action, k.direction),
            ("ocpp201", "BootNotification", "req")
        );
        assert!(parse_routing_key("cp00001-v16-sp1").is_none());
        assert!(parse_routing_key("ocpp16.response.conf").is_some());
        assert!(parse_routing_key("ocpp16.a.b.req").is_none());
    }

    #[test]
    fn profiles_and_offline() {
        assert_eq!(security_profile("cp00001-v16-sp3").as_deref(), Some("3"));
        assert_eq!(security_profile("charger"), None);
        let v2 = json!({"customData": {"vendorId": "rabbitmq", "vendorErrorCode": "Offline"}});
        assert!(is_synthetic_offline("StatusNotification", &v2));
        assert!(!is_synthetic_offline(
            "StatusNotification",
            &json!({"status": "Available"})
        ));
    }

    #[test]
    fn replies() {
        let r = reply("ocpp21", "m1", "BootNotification", &json!({}), 60);
        assert_eq!(r[0], 3);
        assert_eq!(r[2]["interval"], 60);
        assert_eq!(
            reply("ocpp16", "m2", "Bogus", &json!({}), 60)[2],
            "NotImplemented"
        );
        assert!(now().ends_with('Z') && now().len() == 24);
    }
}
