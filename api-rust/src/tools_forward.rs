//! Reverse-proxy routes that forward `/v2/sessions*` HTTP calls to an
//! upstream tools-rust daemon (`TOOLS_DAEMON_URL`).
//!
//! Minimum-viable shape: a single upstream daemon, no per-template
//! container lifecycle yet. The api-rust process is only the routing
//! glue; the daemon itself manages sessions/cwd/tool dispatch.
//!
//! Later phases will:
//! - lazy-spawn a tools-rust container per kind=tools template
//! - record session_id → (template, daemon URL) so multiple daemons can
//!   coexist behind /v2 routes
//! - integrate this into PagedRegistry so warm/hot tiering applies

use axum::{
    body::{to_bytes, Body},
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{any, get, post},
    Router,
};
use std::sync::Arc;

pub struct ToolsForwardState {
    pub upstream: Option<String>, // e.g. http://127.0.0.1:8002
    pub client: reqwest::Client,
}

impl ToolsForwardState {
    pub fn from_env() -> Self {
        let upstream = std::env::var("TOOLS_DAEMON_URL").ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.trim_end_matches('/').to_string())
            }
        });
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .build()
            .expect("reqwest client");
        Self { upstream, client }
    }
}

async fn forward(
    State(st): State<Arc<ToolsForwardState>>,
    method: axum::http::Method,
    path: &str,
    body: Body,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let Some(upstream) = st.upstream.as_deref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "TOOLS_DAEMON_URL not set; /v2 routes disabled".into(),
        ));
    };
    let url = format!("{}{}", upstream, path);
    let body_bytes = to_bytes(body, 8 * 1024 * 1024)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("read body: {e}")))?;
    let resp = st
        .client
        .request(method, &url)
        .header("content-type", "application/json")
        .body(body_bytes)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream: {e}")))?;
    let status = resp.status();
    let mut headers = HeaderMap::new();
    if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
        if let Ok(s) = ct.to_str() {
            if let Ok(v) = s.parse() {
                headers.insert(axum::http::header::CONTENT_TYPE, v);
            }
        }
    }
    let bytes = resp.bytes().await.unwrap_or_default();
    let axum_status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    Ok((axum_status, headers, bytes))
}

async fn v2_health(
    State(st): State<Arc<ToolsForwardState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    forward(State(st), axum::http::Method::GET, "/health", Body::empty()).await
}

async fn v2_create_session(
    State(st): State<Arc<ToolsForwardState>>,
    body: Body,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    forward(State(st), axum::http::Method::POST, "/sessions", body).await
}

async fn v2_delete_session(
    State(st): State<Arc<ToolsForwardState>>,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    forward(
        State(st),
        axum::http::Method::DELETE,
        &format!("/sessions/{sid}"),
        Body::empty(),
    )
    .await
}

async fn v2_tool_call(
    State(st): State<Arc<ToolsForwardState>>,
    Path((sid, tool)): Path<(String, String)>,
    body: Body,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    forward(
        State(st),
        axum::http::Method::POST,
        &format!("/sessions/{sid}/tools/{tool}"),
        body,
    )
    .await
}

pub fn router(state: Arc<ToolsForwardState>) -> Router {
    Router::new()
        .route("/v2/health", get(v2_health))
        .route("/v2/sessions", post(v2_create_session))
        .route("/v2/sessions/:sid", any(v2_delete_session))
        .route("/v2/sessions/:sid/tools/:tool", post(v2_tool_call))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_trims_trailing_slash() {
        std::env::set_var("TOOLS_DAEMON_URL", "http://localhost:8002/");
        let st = ToolsForwardState::from_env();
        assert_eq!(st.upstream.as_deref(), Some("http://localhost:8002"));
        std::env::remove_var("TOOLS_DAEMON_URL");
    }

    #[test]
    fn from_env_treats_empty_as_unset() {
        std::env::set_var("TOOLS_DAEMON_URL", "");
        let st = ToolsForwardState::from_env();
        assert!(st.upstream.is_none());
        std::env::remove_var("TOOLS_DAEMON_URL");
    }

    #[test]
    fn from_env_unset_when_missing() {
        std::env::remove_var("TOOLS_DAEMON_URL");
        let st = ToolsForwardState::from_env();
        assert!(st.upstream.is_none());
    }
}
