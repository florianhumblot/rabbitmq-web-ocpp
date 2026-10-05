//! The charger registry shared by every instance, kept in Valkey. Workers
//! record what chargers send; the command API and the dashboard read it. The
//! update logic is `valkey/registry.lua`, shared with the Java and Go
//! implementations.

use crate::ocpp;
use redis::{RedisResult, Script, aio::ConnectionManager};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

const VERSIONS: [&str; 3] = ["ocpp16", "ocpp201", "ocpp21"];
const PROFILES: [&str; 3] = ["1", "2", "3"];

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

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub known: u64,
    pub online: u64,
    pub by_version: BTreeMap<String, u64>,
    pub by_security_profile: BTreeMap<String, u64>,
}

#[derive(Clone)]
pub struct Registry {
    con: ConnectionManager,
    script: Script,
    prefix: String,
}

impl Registry {
    pub async fn connect(url: &str, vhost: &str) -> RedisResult<Self> {
        let con = redis::Client::open(url)?.get_connection_manager().await?;
        Ok(Self {
            con,
            script: Script::new(include_str!("../../valkey/registry.lua")),
            // One hash tag per vhost: everything of an implementation on one shard.
            prefix: format!("csms:{{{vhost}}}:"),
        })
    }

    fn key(&self, id: &str) -> String {
        format!("{}c:{id}", self.prefix)
    }

    async fn run(&self, id: &str, op: &str, args: &[&str]) -> RedisResult<String> {
        let now = crate::amqp::now_ms().to_string();
        let key = self.key(id);
        let mut invocation = self.script.key(&key);
        invocation.arg(&self.prefix).arg(op).arg(id).arg(&now);
        for a in args {
            invocation.arg(*a);
        }
        invocation.invoke_async(&mut self.con.clone()).await
    }

    /// Records a CALL or SEND a worker processed. When an online/offline
    /// transition looks due, `presence` asks the broker whether the charger is
    /// really connected, as competing workers may process frames out of order.
    pub async fn observe<F, Fut>(
        &self,
        id: &str,
        version: &str,
        action: &str,
        payload: &Value,
        presence: F,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, String>>,
    {
        let result = if ocpp::is_synthetic_offline(action, payload) {
            self.run(id, "offline", &[]).await
        } else {
            let (connector, status, ts) = status_of(version, action, payload);
            let sp = ocpp::security_profile(id).unwrap_or_default();
            self.run(id, "event", &[version, &sp, &connector, &status, &ts])
                .await
        }
        .map_err(|e| e.to_string())?;
        if result != "check" {
            return Ok(());
        }
        let connected = presence().await?;
        self.run(id, "presence", &[if connected { "1" } else { "0" }])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Protocol of a charger and whether it is connected; `None` if unknown.
    pub async fn lookup(&self, id: &str) -> RedisResult<Option<(String, bool)>> {
        let v: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.key(id))
            .arg("v")
            .arg("on")
            .query_async(&mut self.con.clone())
            .await?;
        Ok(match v.as_slice() {
            [Some(version), on] => Some((version.clone(), on.as_deref() == Some("1"))),
            _ => None,
        })
    }

    fn view(id: &str, h: HashMap<String, String>) -> ChargerView {
        ChargerView {
            id: id.to_string(),
            ocpp_version: h.get("v").cloned().unwrap_or_default(),
            security_profile: h.get("sp").filter(|s| !s.is_empty()).cloned(),
            online: h.get("on").map(String::as_str) == Some("1"),
            last_seen: h.get("seen").and_then(|s| s.parse().ok()).unwrap_or(0),
            connectors: h
                .iter()
                .filter_map(|(k, v)| k.strip_prefix("c:").map(|c| (c.to_string(), v.clone())))
                .collect(),
        }
    }

    pub async fn get(&self, id: &str) -> RedisResult<Option<ChargerView>> {
        let h: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(self.key(id))
            .query_async(&mut self.con.clone())
            .await?;
        Ok((!h.is_empty()).then(|| Self::view(id, h)))
    }

    /// Up to `limit` connected chargers.
    pub async fn list(&self, limit: usize) -> RedisResult<Vec<ChargerView>> {
        let mut con = self.con.clone();
        let mut ids: Vec<String> = redis::cmd("SRANDMEMBER")
            .arg(format!("{}online", self.prefix))
            .arg(limit)
            .query_async(&mut con)
            .await?;
        ids.sort();
        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.cmd("HGETALL").arg(self.key(id));
        }
        let hashes: Vec<HashMap<String, String>> = if ids.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut con).await?
        };
        Ok(ids
            .iter()
            .zip(hashes)
            .map(|(id, h)| Self::view(id, h))
            .collect())
    }

    /// Connected chargers by protocol and security profile, and their
    /// connectors by status, in one round trip.
    pub async fn summary(&self) -> RedisResult<(Summary, BTreeMap<String, u64>)> {
        let p = &self.prefix;
        let mut pipe = redis::pipe();
        pipe.cmd("SCARD").arg(format!("{p}known"));
        pipe.cmd("SCARD").arg(format!("{p}online"));
        for v in VERSIONS {
            pipe.cmd("SCARD").arg(format!("{p}online:v:{v}"));
        }
        for sp in PROFILES {
            pipe.cmd("SCARD").arg(format!("{p}online:sp:{sp}"));
        }
        pipe.cmd("HGETALL").arg(format!("{p}status"));
        #[allow(clippy::type_complexity)]
        let (known, online, v16, v201, v21, sp1, sp2, sp3, statuses): (
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
            u64,
            HashMap<String, i64>,
        ) = pipe.query_async(&mut self.con.clone()).await?;
        let counts = [known, online, v16, v201, v21, sp1, sp2, sp3];
        let nonzero = |names: &[&str], values: &[u64]| -> BTreeMap<String, u64> {
            names
                .iter()
                .zip(values)
                .filter(|(_, n)| **n > 0)
                .map(|(k, n)| (k.to_string(), *n))
                .collect()
        };
        let summary = Summary {
            known: counts[0],
            online: counts[1],
            by_version: nonzero(&VERSIONS, &counts[2..5]),
            by_security_profile: nonzero(&PROFILES, &counts[5..8]),
        };
        let connectors = statuses
            .into_iter()
            .filter(|(_, n)| *n > 0)
            .map(|(k, n)| (k, n as u64))
            .collect();
        Ok((summary, connectors))
    }

    pub async fn ping(&self) -> RedisResult<()> {
        redis::cmd("PING")
            .query_async::<String>(&mut self.con.clone())
            .await
            .map(|_| ())
    }
}

/// Connector status of a StatusNotification: 1.6 connectorId + status, 2.x
/// evseId + connectorStatus (one connector per EVSE), plus its timestamp.
fn status_of(version: &str, action: &str, payload: &Value) -> (String, String, String) {
    if action != "StatusNotification" {
        return Default::default();
    }
    let (connector, status) = if ocpp::is_v2(version) {
        ("evseId", "connectorStatus")
    } else {
        ("connectorId", "status")
    };
    match (
        payload.get(connector).and_then(Value::as_u64),
        payload.get(status).and_then(Value::as_str),
    ) {
        (Some(n), Some(s)) if n > 0 => (
            n.to_string(),
            s.to_string(),
            payload
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ),
        _ => Default::default(),
    }
}
