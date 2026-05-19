//! tools-rust — minimal vertical of the 7-tool dispatcher.
//!
//! Sessions are just a uuid → cwd mapping on a shared sandbox root.
//! Tools run inline as Rust functions (no fork, no Python). Bash is
//! the only one that spawns a subprocess.
//!
//! Wire format: JSON. Routes:
//!   GET    /health
//!   POST   /sessions                                  → {session_id}
//!   DELETE /sessions/:id
//!   POST   /sessions/:id/tools/read    {path}         → {content, bytes}
//!   POST   /sessions/:id/tools/write   {path, content}→ {bytes}
//!   POST   /sessions/:id/tools/bash    {cmd, timeout?}→ {stdout, stderr, exit_code}
//!
//! Paths are resolved relative to the session's cwd; absolute paths
//! and `..`-traversal are rejected at the API boundary.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------------------

pub struct AppState {
    sandbox_root: PathBuf,
    sessions: DashMap<String, PathBuf>,
}

#[derive(Serialize)]
struct CreateSessionResp {
    session_id: String,
    cwd: String,
}

#[derive(Deserialize)]
struct ReadReq {
    path: String,
}

#[derive(Serialize)]
struct ReadResp {
    content: String,
    bytes: usize,
}

#[derive(Deserialize)]
struct WriteReq {
    path: String,
    content: String,
}

#[derive(Serialize)]
struct WriteResp {
    bytes: usize,
}

#[derive(Deserialize)]
struct BashReq {
    cmd: String,
    #[serde(default = "default_bash_timeout")]
    timeout: u64,
}

fn default_bash_timeout() -> u64 {
    30
}

#[derive(Serialize)]
struct BashResp {
    stdout: String,
    stderr: String,
    exit_code: i32,
    timed_out: bool,
}

// ---------------------------------------------------------------------------

/// Resolve `rel` against `cwd`, refusing anything that escapes `cwd`.
/// Absolute paths and `..` segments are rejected.
fn resolve_in(cwd: &StdPath, rel: &str) -> Result<PathBuf, (StatusCode, String)> {
    if rel.starts_with('/') {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("absolute path not allowed: {rel}"),
        ));
    }
    let p = cwd.join(rel);
    let normalized = p.components().fold(PathBuf::new(), |mut acc, c| {
        match c {
            std::path::Component::ParentDir => {
                acc.pop();
            }
            other => acc.push(other),
        }
        acc
    });
    if !normalized.starts_with(cwd) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("path escapes session cwd: {rel}"),
        ));
    }
    Ok(normalized)
}

fn resolve_session<'a>(
    state: &'a AppState,
    sid: &str,
) -> Result<dashmap::mapref::one::Ref<'a, String, PathBuf>, (StatusCode, String)> {
    state
        .sessions
        .get(sid)
        .ok_or((StatusCode::NOT_FOUND, format!("session {sid} not found")))
}

// ---------------------------------------------------------------------------

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CreateSessionResp>, (StatusCode, String)> {
    let id = Uuid::new_v4().simple().to_string();
    let cwd = state.sandbox_root.join(&id);
    tokio::fs::create_dir_all(&cwd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")))?;
    state.sessions.insert(id.clone(), cwd.clone());
    Ok(Json(CreateSessionResp {
        session_id: id,
        cwd: cwd.to_string_lossy().into_owned(),
    }))
}

async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some((_, cwd)) = state.sessions.remove(&id) else {
        return Err((StatusCode::NOT_FOUND, format!("session {id} not found")));
    };
    let _ = tokio::fs::remove_dir_all(&cwd).await; // best-effort cleanup
    Ok(StatusCode::NO_CONTENT)
}

async fn tool_read(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<ReadReq>,
) -> Result<Json<ReadResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    let bytes = tokio::fs::read(&target)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read {}: {e}", req.path)))?;
    let n = bytes.len();
    let content = String::from_utf8_lossy(&bytes).into_owned();
    Ok(Json(ReadResp { content, bytes: n }))
}

async fn tool_write(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<WriteReq>,
) -> Result<Json<WriteResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")))?;
    }
    tokio::fs::write(&target, &req.content)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;
    Ok(Json(WriteResp {
        bytes: req.content.len(),
    }))
}

async fn tool_bash(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<BashReq>,
) -> Result<Json<BashResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let mut cmd = Command::new("/bin/bash");
    cmd.arg("-c").arg(&req.cmd).current_dir(&cwd);
    let timeout = Duration::from_secs(req.timeout.clamp(1, 300));
    let fut = cmd.output();
    let (stdout, stderr, exit_code, timed_out) = match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(o)) => (
            String::from_utf8_lossy(&o.stdout).into_owned(),
            String::from_utf8_lossy(&o.stderr).into_owned(),
            o.status.code().unwrap_or(-1),
            false,
        ),
        Ok(Err(e)) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}"))),
        Err(_) => (
            String::new(),
            format!("timeout after {}s", timeout.as_secs()),
            124,
            true,
        ),
    };
    Ok(Json(BashResp {
        stdout,
        stderr,
        exit_code,
        timed_out,
    }))
}

// ---------------------------------------------------------------------------

// Public so an integration test (or a future external embedder) can
// build a Router against an in-memory AppState.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/sessions", post(create_session))
        .route("/sessions/:id", delete(delete_session))
        .route("/sessions/:id/tools/read", post(tool_read))
        .route("/sessions/:id/tools/write", post(tool_write))
        .route("/sessions/:id/tools/bash", post(tool_bash))
        .with_state(state)
}

fn make_state() -> anyhow::Result<Arc<AppState>> {
    let sandbox_root =
        PathBuf::from(std::env::var("TOOLS_SANDBOX_ROOT").unwrap_or_else(|_| "/sandboxes".into()));
    std::fs::create_dir_all(&sandbox_root)?;
    Ok(Arc::new(AppState {
        sandbox_root,
        sessions: DashMap::new(),
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let state = make_state()?;
    let app = build_router(state.clone());

    let port = std::env::var("TOOLS_PORT")
        .unwrap_or_else(|_| "8002".into())
        .parse::<u16>()?;
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(
        "tools-rust listening on 0.0.0.0:{port}, sandbox_root={}",
        state.sandbox_root.display()
    );
    axum::serve(listener, app).await?;
    Ok(())
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn tmp_state() -> (tempfile::TempDir, Arc<AppState>) {
        let td = tempfile::tempdir().unwrap();
        let state = Arc::new(AppState {
            sandbox_root: td.path().to_path_buf(),
            sessions: DashMap::new(),
        });
        (td, state)
    }

    #[test]
    fn resolve_in_normal_relative_path() {
        let cwd = Path::new("/sandboxes/a");
        let out = resolve_in(cwd, "foo/bar.txt").unwrap();
        assert_eq!(out, Path::new("/sandboxes/a/foo/bar.txt"));
    }

    #[test]
    fn resolve_in_rejects_absolute() {
        let cwd = Path::new("/sandboxes/a");
        assert!(resolve_in(cwd, "/etc/passwd").is_err());
    }

    #[test]
    fn resolve_in_rejects_parent_traversal() {
        let cwd = Path::new("/sandboxes/a");
        assert!(resolve_in(cwd, "../b/leak").is_err());
        assert!(resolve_in(cwd, "foo/../../leak").is_err());
    }

    #[test]
    fn resolve_in_allows_inner_parent_dir() {
        let cwd = Path::new("/sandboxes/a");
        // a/foo/../bar normalises to a/bar — stays inside cwd.
        let out = resolve_in(cwd, "foo/../bar").unwrap();
        assert_eq!(out, Path::new("/sandboxes/a/bar"));
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let (_td, state) = tmp_state();
        let sid = "test-sid".to_string();
        let cwd = state.sandbox_root.join(&sid);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        state.sessions.insert(sid.clone(), cwd);

        let resp = tool_write(
            State(state.clone()),
            Path(sid.clone()),
            Json(WriteReq {
                path: "hello.txt".into(),
                content: "hi there".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.bytes, 8);

        let read = tool_read(
            State(state),
            Path(sid),
            Json(ReadReq {
                path: "hello.txt".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(read.0.content, "hi there");
        assert_eq!(read.0.bytes, 8);
    }

    #[tokio::test]
    async fn bash_runs_in_session_cwd() {
        let (_td, state) = tmp_state();
        let sid = "bash-sid".to_string();
        let cwd = state.sandbox_root.join(&sid);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        tokio::fs::write(cwd.join("marker"), "x").await.unwrap();
        state.sessions.insert(sid.clone(), cwd);

        let resp = tool_bash(
            State(state),
            Path(sid),
            Json(BashReq {
                cmd: "ls".into(),
                timeout: 5,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.exit_code, 0);
        assert!(resp.0.stdout.contains("marker"));
        assert!(!resp.0.timed_out);
    }

    #[tokio::test]
    async fn bash_times_out() {
        let (_td, state) = tmp_state();
        let sid = "timeout-sid".to_string();
        let cwd = state.sandbox_root.join(&sid);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        state.sessions.insert(sid.clone(), cwd);

        let resp = tool_bash(
            State(state),
            Path(sid),
            Json(BashReq {
                cmd: "sleep 5".into(),
                timeout: 1,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.exit_code, 124);
        assert!(resp.0.timed_out);
    }

    #[tokio::test]
    async fn session_create_then_delete() {
        let (_td, state) = tmp_state();
        let resp = create_session(State(state.clone())).await.unwrap();
        let sid = resp.0.session_id.clone();
        assert!(state.sessions.contains_key(&sid));
        assert!(StdPath::new(&resp.0.cwd).exists());

        let status = delete_session(State(state.clone()), Path(sid.clone()))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(!state.sessions.contains_key(&sid));
        assert!(!StdPath::new(&resp.0.cwd).exists());
    }

    #[tokio::test]
    async fn delete_unknown_session_404s() {
        let (_td, state) = tmp_state();
        let err = delete_session(State(state), Path("nope".into())).await;
        assert!(matches!(err, Err((StatusCode::NOT_FOUND, _))));
    }
}
