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

#[derive(Deserialize)]
struct EditReq {
    path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Serialize)]
struct EditResp {
    replacements: usize,
}

#[derive(Deserialize)]
struct LsReq {
    #[serde(default = "default_ls_path")]
    path: String,
}

fn default_ls_path() -> String {
    ".".into()
}

#[derive(Serialize)]
struct LsEntry {
    name: String,
    kind: &'static str, // "file", "dir", "symlink", "other"
    size: u64,
}

#[derive(Serialize)]
struct LsResp {
    entries: Vec<LsEntry>,
}

#[derive(Deserialize)]
struct GrepReq {
    pattern: String,
    #[serde(default = "default_grep_path")]
    path: String,
    /// "content" (default) — return matching lines; "files_with_matches" —
    /// return only file paths; "count" — return per-file match counts.
    #[serde(default = "default_grep_mode")]
    output_mode: String,
    /// Max files to walk before bailing. Default 5000.
    #[serde(default = "default_grep_max_files")]
    max_files: usize,
}

fn default_grep_path() -> String {
    ".".into()
}
fn default_grep_mode() -> String {
    "content".into()
}
fn default_grep_max_files() -> usize {
    5000
}

#[derive(Serialize)]
struct GrepMatch {
    path: String,
    line: u64,
    text: String,
}

#[derive(Serialize)]
struct GrepResp {
    mode: String,
    matches: Vec<GrepMatch>,
    files: Vec<String>,
    counts: Vec<(String, u64)>,
    walked: usize,
    truncated: bool,
}

#[derive(Deserialize)]
struct FindReq {
    /// Glob pattern, evaluated against paths relative to session cwd.
    pattern: String,
    #[serde(default = "default_find_path")]
    path: String,
    #[serde(default = "default_find_max")]
    max_results: usize,
}

fn default_find_path() -> String {
    ".".into()
}
fn default_find_max() -> usize {
    5000
}

#[derive(Serialize)]
struct FindResp {
    paths: Vec<String>,
    walked: usize,
    truncated: bool,
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

async fn tool_ls(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<LsReq>,
) -> Result<Json<LsResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    let mut rd = tokio::fs::read_dir(&target)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("ls {}: {e}", req.path)))?;
    let mut entries = Vec::new();
    while let Some(ent) = rd
        .next_entry()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        let md = match ent.metadata().await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let kind = if md.is_dir() {
            "dir"
        } else if md.is_symlink() {
            "symlink"
        } else if md.is_file() {
            "file"
        } else {
            "other"
        };
        entries.push(LsEntry {
            name: ent.file_name().to_string_lossy().into_owned(),
            kind,
            size: md.len(),
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(LsResp { entries }))
}

async fn tool_edit(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<EditReq>,
) -> Result<Json<EditResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    let src = tokio::fs::read_to_string(&target)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read {}: {e}", req.path)))?;
    let n = src.matches(&req.old_string).count();
    if n == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("old_string not found in {}", req.path),
        ));
    }
    if n > 1 && !req.replace_all {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("old_string matches {n} times; set replace_all=true to confirm bulk replace"),
        ));
    }
    let replaced = if req.replace_all {
        src.replace(&req.old_string, &req.new_string)
    } else {
        src.replacen(&req.old_string, &req.new_string, 1)
    };
    tokio::fs::write(&target, replaced)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;
    Ok(Json(EditResp { replacements: n }))
}

async fn tool_grep(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<GrepReq>,
) -> Result<Json<GrepResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    let re = regex::Regex::new(&req.pattern)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad regex: {e}")))?;

    let want_content = req.output_mode == "content";
    let want_files = req.output_mode == "files_with_matches";
    let want_count = req.output_mode == "count";
    if !(want_content || want_files || want_count) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unknown output_mode '{}': use content|files_with_matches|count",
                req.output_mode
            ),
        ));
    }

    let mut walked = 0usize;
    let mut truncated = false;
    let mut matches = Vec::<GrepMatch>::new();
    let mut files = Vec::<String>::new();
    let mut counts = Vec::<(String, u64)>::new();

    let walker = walkdir::WalkDir::new(&target)
        .follow_links(false)
        .into_iter()
        .filter_map(|r| r.ok())
        .filter(|e| e.file_type().is_file());

    for ent in walker {
        walked += 1;
        if walked > req.max_files {
            truncated = true;
            break;
        }
        let path = ent.path();
        let rel = match path.strip_prefix(&cwd) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => path.to_string_lossy().into_owned(),
        };
        let body = match tokio::fs::read_to_string(path).await {
            Ok(b) => b,
            Err(_) => continue, // binary/permission/etc — skip
        };
        let mut per_file = 0u64;
        for (i, line) in body.lines().enumerate() {
            if re.is_match(line) {
                per_file += 1;
                if want_content {
                    matches.push(GrepMatch {
                        path: rel.clone(),
                        line: (i + 1) as u64,
                        text: line.to_string(),
                    });
                }
            }
        }
        if per_file > 0 {
            if want_files {
                files.push(rel.clone());
            } else if want_count {
                counts.push((rel.clone(), per_file));
            }
        }
    }

    Ok(Json(GrepResp {
        mode: req.output_mode,
        matches,
        files,
        counts,
        walked,
        truncated,
    }))
}

async fn tool_find(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<FindReq>,
) -> Result<Json<FindResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;
    let glob = globset::Glob::new(&req.pattern)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad glob: {e}")))?
        .compile_matcher();

    let mut paths = Vec::new();
    let mut walked = 0usize;
    let mut truncated = false;
    for ent in walkdir::WalkDir::new(&target)
        .follow_links(false)
        .into_iter()
        .filter_map(|r| r.ok())
    {
        walked += 1;
        if walked > req.max_results.saturating_mul(4).max(20_000) {
            truncated = true;
            break;
        }
        let rel = match ent.path().strip_prefix(&cwd) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => continue,
        };
        if glob.is_match(&rel) {
            paths.push(rel);
            if paths.len() >= req.max_results {
                truncated = true;
                break;
            }
        }
    }
    Ok(Json(FindResp {
        paths,
        walked,
        truncated,
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
        .route("/sessions/:id/tools/edit", post(tool_edit))
        .route("/sessions/:id/tools/ls", post(tool_ls))
        .route("/sessions/:id/tools/grep", post(tool_grep))
        .route("/sessions/:id/tools/find", post(tool_find))
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

    async fn make_sid(state: &Arc<AppState>, name: &str) -> String {
        let cwd = state.sandbox_root.join(name);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        state.sessions.insert(name.to_string(), cwd);
        name.to_string()
    }

    #[tokio::test]
    async fn ls_lists_files_and_dirs() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "ls-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("a.txt"), "x").await.unwrap();
        tokio::fs::create_dir_all(cwd.join("sub")).await.unwrap();
        let resp = tool_ls(State(state), Path(sid), Json(LsReq { path: ".".into() }))
            .await
            .unwrap();
        let names: Vec<_> = resp.0.entries.iter().map(|e| e.name.clone()).collect();
        assert!(names.contains(&"a.txt".to_string()));
        assert!(names.contains(&"sub".to_string()));
        let a = resp.0.entries.iter().find(|e| e.name == "a.txt").unwrap();
        assert_eq!(a.kind, "file");
        assert_eq!(a.size, 1);
    }

    #[tokio::test]
    async fn edit_single_occurrence() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "edit-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("f.txt"), "alpha beta gamma")
            .await
            .unwrap();
        let resp = tool_edit(
            State(state.clone()),
            Path(sid),
            Json(EditReq {
                path: "f.txt".into(),
                old_string: "beta".into(),
                new_string: "BETA".into(),
                replace_all: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.replacements, 1);
        let body = tokio::fs::read_to_string(cwd.join("f.txt")).await.unwrap();
        assert_eq!(body, "alpha BETA gamma");
    }

    #[tokio::test]
    async fn edit_refuses_ambiguous_without_replace_all() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "edit-amb-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("f.txt"), "ab ab ab")
            .await
            .unwrap();
        let err = tool_edit(
            State(state),
            Path(sid),
            Json(EditReq {
                path: "f.txt".into(),
                old_string: "ab".into(),
                new_string: "Z".into(),
                replace_all: false,
            }),
        )
        .await;
        assert!(matches!(err, Err((StatusCode::BAD_REQUEST, _))));
    }

    #[tokio::test]
    async fn edit_replace_all() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "edit-all-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("f.txt"), "ab ab ab")
            .await
            .unwrap();
        let resp = tool_edit(
            State(state.clone()),
            Path(sid),
            Json(EditReq {
                path: "f.txt".into(),
                old_string: "ab".into(),
                new_string: "Z".into(),
                replace_all: true,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.replacements, 3);
        let body = tokio::fs::read_to_string(cwd.join("f.txt")).await.unwrap();
        assert_eq!(body, "Z Z Z");
    }

    #[tokio::test]
    async fn grep_content_mode_returns_lines() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "grep-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("a.txt"), "hello world\nfoo bar\nhello again\n")
            .await
            .unwrap();
        let resp = tool_grep(
            State(state),
            Path(sid),
            Json(GrepReq {
                pattern: "hello".into(),
                path: ".".into(),
                output_mode: "content".into(),
                max_files: 100,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.matches.len(), 2);
        assert_eq!(resp.0.matches[0].line, 1);
        assert_eq!(resp.0.matches[1].line, 3);
    }

    #[tokio::test]
    async fn grep_files_mode_lists_paths() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "grep-f-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("a.txt"), "needle").await.unwrap();
        tokio::fs::write(cwd.join("b.txt"), "haystack")
            .await
            .unwrap();
        let resp = tool_grep(
            State(state),
            Path(sid),
            Json(GrepReq {
                pattern: "needle".into(),
                path: ".".into(),
                output_mode: "files_with_matches".into(),
                max_files: 100,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.files, vec!["a.txt".to_string()]);
        assert!(resp.0.matches.is_empty());
    }

    #[tokio::test]
    async fn find_glob_matches() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "find-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::create_dir_all(cwd.join("sub")).await.unwrap();
        tokio::fs::write(cwd.join("a.rs"), "").await.unwrap();
        tokio::fs::write(cwd.join("sub/b.rs"), "").await.unwrap();
        tokio::fs::write(cwd.join("c.txt"), "").await.unwrap();
        let resp = tool_find(
            State(state),
            Path(sid),
            Json(FindReq {
                pattern: "**/*.rs".into(),
                path: ".".into(),
                max_results: 100,
            }),
        )
        .await
        .unwrap();
        let mut paths = resp.0.paths;
        paths.sort();
        assert_eq!(paths, vec!["a.rs".to_string(), "sub/b.rs".to_string()]);
    }

    #[tokio::test]
    async fn find_respects_max_results() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "find-max-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        for i in 0..10 {
            tokio::fs::write(cwd.join(format!("f{i}.txt")), "")
                .await
                .unwrap();
        }
        let resp = tool_find(
            State(state),
            Path(sid),
            Json(FindReq {
                pattern: "*.txt".into(),
                path: ".".into(),
                max_results: 3,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.paths.len(), 3);
        assert!(resp.0.truncated);
    }
}
