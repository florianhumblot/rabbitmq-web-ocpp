//! Charger registry and traffic statistics, both fed by the tap on all OCPP
//! traffic of the vhost.

use crate::ocpp;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

pub struct Charger {
    pub version: String,
    pub online: bool,
    last_seen: u64,
    security_profile: Option<String>,
    connectors: BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChargerView {
    id: String,
    ocpp_version: String,
    security_profile: Option<String>,
    online: bool,
    last_seen: u64,
    connectors: BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChargerSummary {
    pub known: usize,
    pub online: usize,
    pub by_version: BTreeMap<String, u64>,
    pub by_security_profile: BTreeMap<String, u64>,
}

/// What the CSMS knows about every charge point, learned purely from the OCPP
/// traffic seen on the broker.
#[derive(Default)]
pub struct Registry {
    chargers: HashMap<String, Charger>,
}

impl Registry {
    fn entry(&mut self, id: &str, version: &str, now: u64) -> &mut Charger {
        let c = self
            .chargers
            .entry(id.to_string())
            .or_insert_with(|| Charger {
                version: String::new(),
                online: false,
                last_seen: 0,
                security_profile: ocpp::security_profile(id),
                connectors: BTreeMap::new(),
            });
        if c.version != version {
            c.version = version.to_string();
        }
        c.last_seen = now;
        c
    }

    /// A charger-initiated CALL or SEND.
    pub fn on_charger_request(
        &mut self,
        id: &str,
        version: &str,
        action: &str,
        payload: &Value,
        now: u64,
    ) {
        let c = self.entry(id, version, now);
        if ocpp::is_synthetic_offline(action, payload) {
            c.online = false;
            return;
        }
        c.online = true;
        if action == "StatusNotification" {
            // 1.6: connectorId + status. 2.x: evseId + connectorStatus (one connector per EVSE).
            let (connector, status) = if ocpp::is_v2(version) {
                (payload.get("evseId"), payload.get("connectorStatus"))
            } else {
                (payload.get("connectorId"), payload.get("status"))
            };
            if let (Some(connector), Some(status)) = (
                connector.and_then(Value::as_u64),
                status.and_then(Value::as_str),
            ) && connector > 0
            {
                c.connectors
                    .insert(connector.to_string(), status.to_string());
            }
        }
    }

    /// A CALLRESULT/CALLERROR from the charger proves it is connected.
    pub fn on_charger_response(&mut self, id: &str, version: &str, now: u64) {
        self.entry(id, version, now).online = true;
    }

    pub fn get(&self, id: &str) -> Option<&Charger> {
        self.chargers.get(id)
    }

    fn view(id: &str, c: &Charger) -> ChargerView {
        ChargerView {
            id: id.to_string(),
            ocpp_version: c.version.clone(),
            security_profile: c.security_profile.clone(),
            online: c.online,
            last_seen: c.last_seen,
            connectors: c.connectors.clone(),
        }
    }

    pub fn view_of(&self, id: &str) -> Option<ChargerView> {
        self.chargers.get(id).map(|c| Self::view(id, c))
    }

    pub fn list(&self, limit: usize) -> Vec<ChargerView> {
        let mut ids: Vec<(&String, &Charger)> = self.chargers.iter().collect();
        ids.sort_by(|a, b| b.1.online.cmp(&a.1.online).then_with(|| a.0.cmp(b.0)));
        ids.into_iter()
            .take(limit)
            .map(|(id, c)| Self::view(id, c))
            .collect()
    }

    pub fn summary(&self) -> (ChargerSummary, BTreeMap<String, u64>) {
        let mut s = ChargerSummary {
            known: self.chargers.len(),
            online: 0,
            by_version: BTreeMap::new(),
            by_security_profile: BTreeMap::new(),
        };
        let mut connectors = BTreeMap::new();
        for c in self.chargers.values().filter(|c| c.online) {
            s.online += 1;
            *s.by_version.entry(c.version.clone()).or_default() += 1;
            if let Some(sp) = &c.security_profile {
                *s.by_security_profile.entry(sp.clone()).or_default() += 1;
            }
            for status in c.connectors.values() {
                *connectors.entry(status.clone()).or_default() += 1;
            }
        }
        (s, connectors)
    }
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RecentFrame {
    ts: u64,
    direction: &'static str,
    charger_id: String,
    kind: &'static str,
    action: Option<String>,
    message_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionRate {
    action: String,
    direction: String,
    per_sec: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Traffic {
    in_per_sec: f64,
    out_per_sec: f64,
    by_action: Vec<ActionRate>,
}

#[derive(Serialize)]
pub struct Latency {
    samples: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Serialize)]
pub struct Commands {
    sent: u64,
    accepted: u64,
    rejected: u64,
    failed: u64,
    pending: usize,
}

pub struct Window {
    pub traffic: Traffic,
    pub latency: Latency,
    pub commands: Commands,
    pub recent: Vec<RecentFrame>,
}

const RECENT_SIZE: usize = 30;
const PENDING_EXPIRY_MS: u64 = 60_000;

/// Traffic counters: message rates, CSMS reply latency (charger request seen
/// on the broker until the CSMS answer is seen on the broker) and commands.
pub struct Stats {
    inbound: u64,
    outbound: u64,
    actions: HashMap<(&'static str, String), u64>,
    awaiting_reply: HashMap<(String, String), u64>,
    commands_in_flight: HashMap<(String, String), (u64, String)>,
    sent: u64,
    accepted: u64,
    rejected: u64,
    failed: u64,
    latencies: Vec<f64>,
    recent: VecDeque<RecentFrame>,
    window_start: Instant,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            inbound: 0,
            outbound: 0,
            actions: HashMap::new(),
            awaiting_reply: HashMap::new(),
            commands_in_flight: HashMap::new(),
            sent: 0,
            accepted: 0,
            rejected: 0,
            failed: 0,
            latencies: Vec::new(),
            recent: VecDeque::with_capacity(RECENT_SIZE),
            window_start: Instant::now(),
        }
    }
}

impl Stats {
    fn remember(&mut self, frame: RecentFrame) {
        if self.recent.len() == RECENT_SIZE {
            self.recent.pop_back();
        }
        self.recent.push_front(frame);
    }

    /// CALL or SEND from a charger. CALLs are timed until the CSMS answers.
    pub fn charger_request(
        &mut self,
        id: &str,
        msg_id: &str,
        action: &str,
        kind: &'static str,
        expects_reply: bool,
        now: u64,
    ) {
        self.inbound += 1;
        *self.actions.entry(("in", action.to_string())).or_default() += 1;
        if expects_reply {
            self.awaiting_reply
                .insert((id.to_string(), msg_id.to_string()), now);
        }
        self.remember(RecentFrame {
            ts: now,
            direction: "in",
            charger_id: id.to_string(),
            kind,
            action: Some(action.to_string()),
            message_id: msg_id.to_string(),
        });
    }

    /// CALLRESULT/CALLERROR from a charger, answering a CSMS command.
    pub fn charger_response(
        &mut self,
        id: &str,
        msg_id: &str,
        kind: &'static str,
        status: Option<&str>,
        now: u64,
    ) {
        self.inbound += 1;
        let command = self
            .commands_in_flight
            .remove(&(id.to_string(), msg_id.to_string()));
        if command.is_some() {
            match (kind, status) {
                ("CALLRESULT", None | Some("Accepted") | Some("Scheduled")) => self.accepted += 1,
                ("CALLRESULT", _) => self.rejected += 1,
                _ => self.failed += 1,
            }
        }
        self.remember(RecentFrame {
            ts: now,
            direction: "in",
            charger_id: id.to_string(),
            kind,
            action: command.map(|(_, action)| action),
            message_id: msg_id.to_string(),
        });
    }

    /// CALL from the CSMS to a charger (a command).
    pub fn csms_call(&mut self, id: &str, msg_id: &str, action: &str, now: u64) {
        self.outbound += 1;
        self.sent += 1;
        *self.actions.entry(("out", action.to_string())).or_default() += 1;
        self.commands_in_flight.insert(
            (id.to_string(), msg_id.to_string()),
            (now, action.to_string()),
        );
        self.remember(RecentFrame {
            ts: now,
            direction: "out",
            charger_id: id.to_string(),
            kind: "CALL",
            action: Some(action.to_string()),
            message_id: msg_id.to_string(),
        });
    }

    /// CALLRESULT/CALLERROR from the CSMS, answering a charger request.
    pub fn csms_reply(&mut self, id: &str, msg_id: &str, kind: &'static str, now: u64) {
        self.outbound += 1;
        if let Some(requested) = self
            .awaiting_reply
            .remove(&(id.to_string(), msg_id.to_string()))
        {
            self.latencies.push(now.saturating_sub(requested) as f64);
        }
        self.remember(RecentFrame {
            ts: now,
            direction: "out",
            charger_id: id.to_string(),
            kind,
            action: None,
            message_id: msg_id.to_string(),
        });
    }

    /// Closes the current window: rates since the previous call, its latency
    /// distribution, and resets the per-window counters.
    pub fn window(&mut self, now: u64) -> Window {
        let seconds = self.window_start.elapsed().as_secs_f64().max(0.001);
        self.window_start = Instant::now();

        let by_action = self
            .actions
            .drain()
            .map(|((direction, action), count)| ActionRate {
                action,
                direction: direction.to_string(),
                per_sec: count as f64 / seconds,
            })
            .collect();
        let traffic = Traffic {
            in_per_sec: std::mem::take(&mut self.inbound) as f64 / seconds,
            out_per_sec: std::mem::take(&mut self.outbound) as f64 / seconds,
            by_action,
        };

        let mut samples = std::mem::take(&mut self.latencies);
        samples.sort_by(f64::total_cmp);
        let pct = |p: f64| -> f64 {
            if samples.is_empty() {
                return 0.0;
            }
            let i = ((p * samples.len() as f64).ceil() as usize).clamp(1, samples.len()) - 1;
            samples[i]
        };
        let latency = Latency {
            samples: samples.len(),
            p50: pct(0.50),
            p95: pct(0.95),
            p99: pct(0.99),
            max: samples.last().copied().unwrap_or(0.0),
        };

        let cutoff = now.saturating_sub(PENDING_EXPIRY_MS);
        self.awaiting_reply.retain(|_, ts| *ts >= cutoff);
        self.commands_in_flight.retain(|_, (ts, _)| *ts >= cutoff);

        Window {
            traffic,
            latency,
            commands: Commands {
                sent: self.sent,
                accepted: self.accepted,
                rejected: self.rejected,
                failed: self.failed,
                pending: self.commands_in_flight.len(),
            },
            recent: self.recent.iter().cloned().collect(),
        }
    }
}
