//! HTTP plumbing shared by the roles: probes for the orchestrator, and the
//! `/api/` forwarder used when the dashboard runs without the API role.

use crate::App;
use axum::{
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::{Arc, atomic::Ordering};

/// Liveness: the process is up.
pub async fn live() -> &'static str {
    "UP\n"
}

/// Readiness: started, not draining, and its dependencies answer. A rolling
/// deployment only proceeds once new instances are ready, and stops routing
/// to an instance as soon as it starts draining.
pub async fn ready(State(app): State<Arc<App>>) -> Response {
    if !app.ready.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "DRAINING or STARTING\n").into_response();
    }
    if !app.conn.status().connected() {
        return (StatusCode::SERVICE_UNAVAILABLE, "rabbitmq: not connected\n").into_response();
    }
    if let Err(e) = app.registry.ping().await {
        return (StatusCode::SERVICE_UNAVAILABLE, format!("valkey: {e}\n")).into_response();
    }
    "UP\n".into_response()
}

/// Forwards /api/... to the command API service.
pub async fn proxy(
    State(app): State<Arc<App>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let url = format!(
        "{}{}",
        app.cfg.command_api_url.trim_end_matches('/'),
        uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
    );
    let mut request = app.http.request(method, url).body(body);
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
            let body = response.bytes().await.unwrap_or_default();
            let mut out = (status, body).into_response();
            if let Some(content_type) = content_type {
                out.headers_mut().insert(header::CONTENT_TYPE, content_type);
            }
            out
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("command API unreachable: {e}\n"),
        )
            .into_response(),
    }
}
