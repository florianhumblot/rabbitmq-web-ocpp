//! Traffic statistics for the dashboard, fed by its tap on all OCPP traffic
//! of the vhost. The charger registry is shared state in Valkey, see
//! `registry`.

use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

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
