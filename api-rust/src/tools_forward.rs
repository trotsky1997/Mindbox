//! /v2 routes that dispatch to a tools-rust daemon picked by the
//! PagedRegistry. The daemon URL is resolved per request by acquiring the
//! template — so a tools template gets lazy-spawned the first time it's
//! used, with the same hot/warm/cold tier + LRU machinery as every tools
//! template.
//!
//! Backwards compat: if PagedRegistry has no `tools-*` template configured
//! but TOOLS_DAEMON_URL env is set, we fall back to direct forward to
//! that single daemon (the phase 2 shape). This keeps simple single-daemon
//! deployments working without writing a template.toml.

use axum::{
    body::{to_bytes, Body},
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::{any, get, post},
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::AppState;

const FALLBACK_TEMPLATE: &str = "__fallback_daemon__";

pub struct ToolsForwardState {
    pub app: Arc<AppState>,
    pub fallback_daemon: Option<String>,
    pub client: reqwest::Client,
    /// session_id → (template name, sticky daemon URL).
    ///
    /// The daemon URL is captured at session-create time. Subsequent tool
    /// calls for that sid go directly to the same daemon URL, NOT through
    /// PagedRegistry round-robin. This matters when a template has
    /// containers > 1: a session lives on exactly one of the daemon
    /// replicas, and routing later requests to a sibling daemon would
    /// return "session not found".
    pub sessions: dashmap::DashMap<String, (String, String)>,
}

impl ToolsForwardState {
    pub fn new(app: Arc<AppState>) -> Self {
        let fallback = std::env::var("TOOLS_DAEMON_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .build()
            .expect("reqwest client");
        Self {
            app,
            fallback_daemon: fallback,
            client,
            sessions: dashmap::DashMap::new(),
        }
    }

    /// Resolve a daemon URL for the given template name. Order:
    ///   1. Acquire the template in PagedRegistry; if it's a Tools backend,
    ///      pick a daemon round-robin.
    ///   2. Else fallback to TOOLS_DAEMON_URL.
    async fn daemon_for(&self, template: &str) -> Result<String, (StatusCode, String)> {
        if template != FALLBACK_TEMPLATE {
            match self.app.registry.acquire(template).await {
                Ok(rt) => {
                    if let Some(u) = rt.pick_daemon_url() {
                        return Ok(u.to_string());
                    }
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("template '{}' is not backed by a tools daemon", template),
                    ));
                }
                Err(e) => {
                    // Template not configured — fall through to fallback if any.
                    if self.fallback_daemon.is_none() {
                        return Err((
                            StatusCode::NOT_FOUND,
                            format!("template '{}' not found: {}", template, e),
                        ));
                    }
                }
            }
        }
        self.fallback_daemon.clone().ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "no template registered and TOOLS_DAEMON_URL unset".into(),
        ))
    }
}

async fn forward_to(
    st: &ToolsForwardState,
    template: &str,
    method: Method,
    path: &str,
    body: Body,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let upstream = st.daemon_for(template).await?;
    forward_to_url(st, &upstream, method, path, body).await
}

async fn forward_to_url(
    st: &ToolsForwardState,
    upstream: &str,
    method: Method,
    path: &str,
    body: Body,
) -> Result<axum::response::Response, (StatusCode, String)> {
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
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("upstream {}: {e}", upstream),
            )
        })?;
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
    Ok((axum_status, headers, bytes).into_response())
}

async fn v2_health(State(st): State<Arc<ToolsForwardState>>) -> impl IntoResponse {
    let template = FALLBACK_TEMPLATE.to_string();
    match forward_to(&st, &template, Method::GET, "/health", Body::empty()).await {
        Ok(r) => r,
        Err((status, msg)) => (status, msg).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateSessionReq {
    #[serde(default)]
    template: String,
}

async fn v2_create_session(
    State(st): State<Arc<ToolsForwardState>>,
    body: Body,
) -> impl IntoResponse {
    // Read body once, parse for template, then forward.
    let body_bytes = match to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("read body: {e}")).into_response(),
    };
    let req: CreateSessionReq = serde_json::from_slice(&body_bytes).unwrap_or(CreateSessionReq {
        template: String::new(),
    });
    let template = if req.template.is_empty() {
        FALLBACK_TEMPLATE.to_string()
    } else {
        req.template.clone()
    };
    // Resolve daemon URL *once* at session-create time. The same URL is
    // remembered on the sticky map and used for every follow-up tool call
    // on this sid, so multi-instance templates do not bounce a session
    // between sibling daemons.
    let upstream = match st.daemon_for(&template).await {
        Ok(u) => u,
        Err((s, m)) => return (s, m).into_response(),
    };
    let forwarded = forward_to_url(
        &st,
        &upstream,
        Method::POST,
        "/sessions",
        Body::from(body_bytes.clone()),
    )
    .await;
    let resp = match forwarded {
        Ok(r) => r,
        Err((s, m)) => return (s, m).into_response(),
    };
    // To dispatch follow-up calls on this session id, peek the returned
    // body, extract session_id, and remember which (template, daemon URL)
    // it belongs to. (resp.into_body() consumes resp so do that last.)
    let (parts, body) = resp.into_parts();
    let body_bytes = match to_bytes(body, 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("read body: {e}")).into_response(),
    };
    if parts.status.is_success() {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
                st.sessions.insert(sid.to_string(), (template, upstream));
            }
        }
    }
    axum::response::Response::from_parts(parts, Body::from(body_bytes))
}

async fn v2_delete_session(
    State(st): State<Arc<ToolsForwardState>>,
    Path(sid): Path<String>,
) -> impl IntoResponse {
    // Use the sticky daemon URL captured at create time so we forward
    // DELETE to the exact same daemon that owns the session state.
    let url_opt = st.sessions.remove(&sid).map(|(_, v)| v.1);
    let upstream = match url_opt {
        Some(u) => u,
        None => match st.fallback_daemon.clone() {
            Some(u) => u,
            None => {
                return (StatusCode::NOT_FOUND, format!("session {sid} not found")).into_response();
            }
        },
    };
    match forward_to_url(
        &st,
        &upstream,
        Method::DELETE,
        &format!("/sessions/{sid}"),
        Body::empty(),
    )
    .await
    {
        Ok(r) => r,
        Err((s, m)) => (s, m).into_response(),
    }
}

async fn v2_tool_call(
    State(st): State<Arc<ToolsForwardState>>,
    Path((sid, tool)): Path<(String, String)>,
    body: Body,
) -> impl IntoResponse {
    // Gate the eighth "process" tool: even though tools-rust may serve it,
    // the api-rust forward only exposes it to trusted callers (harness,
    // bridge, debug). Default: 403. Flip TOOLS_EXPOSE_PROCESS=1 to enable.
    if tool == "process" && !process_tool_exposed() {
        let body = r#"{"code":"process_forbidden","message":"process tool is not agent-facing; set TOOLS_EXPOSE_PROCESS=1 on api-rust to enable"}"#;
        return (
            StatusCode::FORBIDDEN,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response();
    }
    // Look up the sticky (template, daemon_url) captured at create time.
    // Use the URL directly so a session pinned to instance N never gets
    // bounced to instance M by round-robin.
    let upstream = match st.sessions.get(&sid).map(|r| r.clone()) {
        Some((_template, url)) => url,
        None => {
            // No sticky entry for this sid means either it never existed
            // or it was just deleted. Fall back to the legacy fallback daemon
            // only when one is configured; otherwise 404 immediately so the
            // caller observes "session and its process_ids are gone" instead
            // of a 503 about unconfigured fallback.
            match st.fallback_daemon.clone() {
                Some(u) => u,
                None => {
                    return (StatusCode::NOT_FOUND, format!("session {sid} not found"))
                        .into_response();
                }
            }
        }
    };
    match forward_to_url(
        &st,
        &upstream,
        Method::POST,
        &format!("/sessions/{sid}/tools/{tool}"),
        body,
    )
    .await
    {
        Ok(r) => r,
        Err((s, m)) => (s, m).into_response(),
    }
}

/// Whether the api-rust forward should expose the eighth `process` tool to
/// callers. Default off; flipped on per deployment by `TOOLS_EXPOSE_PROCESS`.
fn process_tool_exposed() -> bool {
    matches!(
        std::env::var("TOOLS_EXPOSE_PROCESS").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

async fn v2_templates(State(st): State<Arc<ToolsForwardState>>) -> impl IntoResponse {
    // List configured templates whose kind is "tools".
    let names: Vec<&String> = st
        .app
        .registry
        .configs_iter()
        .filter(|(_, c)| c.kind == "tools")
        .map(|(n, _)| n)
        .collect();
    let body = serde_json::json!({
        "templates": names,
        "fallback_daemon": st.fallback_daemon,
    });
    (StatusCode::OK, Json(body))
}

pub fn router(state: Arc<ToolsForwardState>) -> Router {
    Router::new()
        .route("/v2/health", get(v2_health))
        .route("/v2/templates", get(v2_templates))
        .route("/v2/sessions", post(v2_create_session))
        .route("/v2/sessions/:sid", any(v2_delete_session))
        .route("/v2/sessions/:sid/tools/:tool", post(v2_tool_call))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    // Tests don't need the module imports right now — they only touch
    // std::env. Keep #[cfg(test)] mod around for future tests.

    #[test]
    fn fallback_url_trims_trailing_slash() {
        // We can't construct AppState in a unit test without a docker daemon,
        // so just exercise the env-parse path:
        std::env::set_var("TOOLS_DAEMON_URL", "http://localhost:8002/");
        // Mimic the parsing the constructor does.
        let parsed = std::env::var("TOOLS_DAEMON_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        assert_eq!(parsed.as_deref(), Some("http://localhost:8002"));
        std::env::remove_var("TOOLS_DAEMON_URL");
    }

    #[test]
    fn fallback_empty_treated_as_unset() {
        std::env::set_var("TOOLS_DAEMON_URL", "");
        let parsed = std::env::var("TOOLS_DAEMON_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        assert!(parsed.is_none());
        std::env::remove_var("TOOLS_DAEMON_URL");
    }

    #[test]
    fn fallback_unset_when_missing() {
        std::env::remove_var("TOOLS_DAEMON_URL");
        let parsed = std::env::var("TOOLS_DAEMON_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        assert!(parsed.is_none());
    }

    #[test]
    fn process_tool_exposed_defaults_off() {
        // Make sure no neighbour test left the env set.
        std::env::remove_var("TOOLS_EXPOSE_PROCESS");
        assert!(!super::process_tool_exposed());
        std::env::set_var("TOOLS_EXPOSE_PROCESS", "1");
        assert!(super::process_tool_exposed());
        std::env::set_var("TOOLS_EXPOSE_PROCESS", "0");
        assert!(!super::process_tool_exposed());
        std::env::remove_var("TOOLS_EXPOSE_PROCESS");
    }
}
