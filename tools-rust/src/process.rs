//! Eighth tool: `process` — session-scoped persistent child processes.
//!
//! Per EFP RFC 0001 section "process abstraction". A process lives strictly
//! inside its owning session: when the session is gone (explicit delete,
//! idle reap, daemon shutdown, container loss) every `process_id` it issued
//! is invalid. No checkpoint/restore, no cross-session attach.
//!
//! This module exposes:
//!   - request/response types (`ProcessReq`, `ProcessResult`, ...)
//!   - the in-session registry types (`ProcessHandle`, `ProcessState`)
//!   - the route handler `tool_process`
//!   - a `ProcessCfg` env-driven config bundle
//!
//! The tool is double-gated: the daemon-side `TOOLS_PROCESS_ENABLED` kill
//! switch and the `api-rust` forward gate `TOOLS_EXPOSE_PROCESS`. Either
//! being unset hides the tool from callers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use base64::Engine;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::{build_sandboxed_command, cgroup_attach_pid, resolve_session, AppState};

// ---------------------------------------------------------------------------
// Config

#[derive(Debug, Clone)]
pub struct ProcessCfg {
    pub enabled: bool,
    pub max_per_session: usize,
    pub buffer_bytes: usize,
    pub idle_reap_sec: u64,
    pub kill_group: bool,
    pub force_unsupported: bool,
}

impl ProcessCfg {
    pub fn from_env() -> Self {
        fn flag(name: &str, default: bool) -> bool {
            match std::env::var(name).ok().as_deref() {
                Some("1") | Some("true") | Some("yes") => true,
                Some("0") | Some("false") | Some("no") | Some("") => false,
                None => default,
                _ => default,
            }
        }
        fn num(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default)
        }
        Self {
            enabled: flag("TOOLS_PROCESS_ENABLED", false),
            max_per_session: num("TOOLS_MAX_PROCESSES_PER_SESSION", 32),
            buffer_bytes: num("TOOLS_PROCESS_BUFFER_BYTES", 262_144),
            idle_reap_sec: std::env::var("TOOLS_SESSION_IDLE_REAP_SEC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3600),
            kill_group: flag("TOOLS_PROCESS_KILL_GROUP", true),
            force_unsupported: flag("TOOLS_PROCESS_FORCE_UNSUPPORTED", false),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire types

/// Discriminator for the action-tagged request body.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessAction {
    Start,
    Write,
    Read,
    Signal,
    Wait,
    Stop,
    List,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessEncoding {
    #[serde(rename = "utf-8")]
    #[default]
    Utf8,
    Base64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // input/eof/signal land in Batch 3 (write/stop/signal handlers)
pub struct ProcessReq {
    pub action: ProcessAction,
    #[serde(default)]
    pub process_id: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub encoding: ProcessEncoding,
    #[serde(default)]
    pub eof: bool,
    #[serde(default)]
    pub signal: Option<String>,
    #[serde(default)]
    pub timeout_sec: Option<f64>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Default, Serialize)]
pub struct ProcessResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<ProcessEncoding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eof_stdout: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eof_stderr: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<Vec<ProcessSummary>>,
}

#[derive(Debug, Serialize)]
pub struct ProcessSummary {
    pub process_id: String,
    pub command: String,
    pub running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub started_at_secs_ago: u64,
}

// ---------------------------------------------------------------------------
// Per-process state

#[derive(Debug, Clone, Copy)]
pub enum ProcessState {
    Running,
    Exited { code: i32 },
    Terminated { signal: i32 },
}

impl ProcessState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Bounded ring buffer for one stdio stream. Stores raw bytes (so base64
/// is only applied at the response edge). Tracks an internal read cursor
/// that the next `read` call drains from; on overflow the oldest bytes
/// are dropped and `truncated_total_bytes` increases.
pub struct StreamBuffer {
    inner: Mutex<StreamBufferInner>,
    notify: Notify,
    cap: usize,
}

struct StreamBufferInner {
    buf: VecDeque<u8>,
    truncated_total_bytes: u64,
    /// True once the writer side (drain task) has closed.
    closed: bool,
}

impl StreamBuffer {
    fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(StreamBufferInner {
                buf: VecDeque::new(),
                truncated_total_bytes: 0,
                closed: false,
            }),
            notify: Notify::new(),
            cap,
        }
    }

    async fn push(&self, bytes: &[u8]) {
        let mut g = self.inner.lock().await;
        for b in bytes {
            if g.buf.len() == self.cap {
                g.buf.pop_front();
                g.truncated_total_bytes = g.truncated_total_bytes.saturating_add(1);
            }
            g.buf.push_back(*b);
        }
        drop(g);
        self.notify.notify_waiters();
    }

    async fn close(&self) {
        let mut g = self.inner.lock().await;
        g.closed = true;
        drop(g);
        self.notify.notify_waiters();
    }

    /// Drain up to `max` bytes (None = all available) from the buffer.
    /// Returns (bytes, truncated_since_last_call, eof).
    async fn drain(&self, max: Option<usize>) -> (Vec<u8>, bool, bool) {
        let mut g = self.inner.lock().await;
        let take = match max {
            Some(m) => m.min(g.buf.len()),
            None => g.buf.len(),
        };
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(b) = g.buf.pop_front() {
                out.push(b);
            }
        }
        let truncated = g.truncated_total_bytes > 0;
        if truncated {
            g.truncated_total_bytes = 0;
        }
        let eof = g.closed && g.buf.is_empty();
        (out, truncated, eof)
    }
}

pub struct ProcessHandle {
    pub process_id: String,
    pub pid: u32,
    pub command_line: String,
    pub started_at: Instant,
    pub stdout: Arc<StreamBuffer>,
    pub stderr: Arc<StreamBuffer>,
    pub stdin: Mutex<Option<tokio::process::ChildStdin>>,
    pub state: Mutex<ProcessState>,
    pub wait_notify: Arc<Notify>,
}

impl ProcessHandle {
    pub fn snapshot_running(&self) -> bool {
        // Lock-free best-effort would need an atomic; the mutex is cheap
        // enough for the cardinality we expect.
        match self.state.try_lock() {
            Ok(g) => g.is_running(),
            Err(_) => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Handler

const WAIT_DEFAULT_CEILING_SEC: f64 = 300.0;
const READ_TIMEOUT_CEILING_SEC: f64 = 60.0;

pub async fn tool_process(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<ProcessReq>,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let cfg = &state.process_cfg;
    if !cfg.enabled {
        // Kill switch — pretend the route doesn't exist.
        return Err((StatusCode::NOT_FOUND, "process tool disabled".into()));
    }
    if cfg.force_unsupported {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            r#"{"code":"process_unsupported","message":"process backend unavailable"}"#.into(),
        ));
    }

    let session = resolve_session(&state, &sid)?.clone();
    session.touch();

    match req.action {
        ProcessAction::Start => action_start(&state, &sid, session, req).await,
        ProcessAction::Read => action_read(session, req).await,
        ProcessAction::Wait => action_wait(session, req).await,
        ProcessAction::List => Ok(Json(action_list(session))),
        // Other actions (write/signal/stop) land in batch 3.
        ProcessAction::Write | ProcessAction::Signal | ProcessAction::Stop => Err((
            StatusCode::NOT_IMPLEMENTED,
            r#"{"code":"action_not_yet_implemented"}"#.into(),
        )),
    }
}

// --- start ---

async fn action_start(
    state: &Arc<AppState>,
    sid: &str,
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    if req.process_id.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "start MUST NOT supply process_id".into(),
        ));
    }
    let command = req.command.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "start requires command".to_string(),
    ))?;

    if session.processes.len() >= state.process_cfg.max_per_session {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "session process cap reached".into(),
        ));
    }

    // Resolve optional cwd relative to session cwd (reject absolute / ..-escape).
    let session_cwd = if let Some(rel) = req.cwd.as_deref() {
        crate::resolve_in(&session.cwd, rel)?
    } else {
        session.cwd.clone()
    };

    // Build args slice for the helper.
    let arg_strings: Vec<String> = req.args.clone().unwrap_or_default();
    let arg_refs: Vec<&str> = arg_strings.iter().map(|s| s.as_str()).collect();

    let mut cmd = build_sandboxed_command(state, sid, &session_cwd, command, &arg_refs)?;
    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(env) = req.env.as_ref() {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }
    if state.process_cfg.kill_group {
        // SAFETY: setsid is async-signal-safe; we add to the existing
        // pre_exec chain installed by build_sandboxed_command.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;

    let pid = child
        .id()
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "no child pid".into()))?;
    // Attach to per-session cgroup if cgroup isolation is on.
    if let Some(root) = state.isolation.cgroup_root.as_ref() {
        let dir = root.join(format!("mindbox-{}", sid));
        cgroup_attach_pid(&dir, pid);
    }

    let stdin = child.stdin.take();
    let child_stdout = child.stdout.take();
    let child_stderr = child.stderr.take();

    let cap = state.process_cfg.buffer_bytes;
    let stdout_buf = Arc::new(StreamBuffer::new(cap));
    let stderr_buf = Arc::new(StreamBuffer::new(cap));
    let wait_notify = Arc::new(Notify::new());

    let process_id = Uuid::new_v4().simple().to_string();
    let cmdline = if arg_strings.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, arg_strings.join(" "))
    };
    let handle = Arc::new(ProcessHandle {
        process_id: process_id.clone(),
        pid,
        command_line: cmdline,
        started_at: Instant::now(),
        stdout: stdout_buf.clone(),
        stderr: stderr_buf.clone(),
        stdin: Mutex::new(stdin),
        state: Mutex::new(ProcessState::Running),
        wait_notify: wait_notify.clone(),
    });

    // Drain stdout
    if let Some(out) = child_stdout {
        let buf = stdout_buf.clone();
        tokio::spawn(drain_into_buffer(out, buf));
    }
    // Drain stderr
    if let Some(err) = child_stderr {
        let buf = stderr_buf.clone();
        tokio::spawn(drain_into_buffer(err, buf));
    }

    // Wait task
    let handle_for_wait = handle.clone();
    let notify_for_wait = wait_notify.clone();
    tokio::spawn(async move {
        let result = child.wait().await;
        let new_state = match result {
            Ok(status) => {
                if let Some(code) = status.code() {
                    ProcessState::Exited { code }
                } else if let Some(sig) = signal_from_status(&status) {
                    ProcessState::Terminated { signal: sig }
                } else {
                    ProcessState::Exited { code: -1 }
                }
            }
            Err(_) => ProcessState::Exited { code: -1 },
        };
        {
            let mut g = handle_for_wait.state.lock().await;
            *g = new_state;
        }
        // Close stdout/stderr buffers so readers can observe EOF.
        handle_for_wait.stdout.close().await;
        handle_for_wait.stderr.close().await;
        notify_for_wait.notify_waiters();
    });

    session.processes.insert(process_id.clone(), handle);

    Ok(Json(ProcessResult {
        process_id: Some(process_id),
        running: Some(true),
        ..Default::default()
    }))
}

#[cfg(unix)]
fn signal_from_status(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn signal_from_status(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

async fn drain_into_buffer<R: AsyncReadExt + Unpin + Send + 'static>(
    mut r: R,
    buf: Arc<StreamBuffer>,
) {
    let mut tmp = [0u8; 4096];
    loop {
        match r.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => buf.push(&tmp[..n]).await,
            Err(_) => break,
        }
    }
}

// --- read ---

async fn action_read(
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let process_id = req.process_id.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "read requires process_id".to_string(),
    ))?;
    let handle = session
        .processes
        .get(process_id)
        .ok_or((StatusCode::NOT_FOUND, "process not found".to_string()))?
        .clone();

    let max = req.max_bytes;
    let timeout = req
        .timeout_sec
        .map(|t| t.clamp(0.0, READ_TIMEOUT_CEILING_SEC));

    // First non-blocking drain
    let (mut out_bytes, mut tr_out, mut eof_out) = handle.stdout.drain(max).await;
    let (mut err_bytes, mut tr_err, mut eof_err) = handle.stderr.drain(max).await;

    // If nothing yet and a timeout is requested, wait a little for new data
    // on either stream OR for the child to exit (which closes the buffers).
    if out_bytes.is_empty() && err_bytes.is_empty() && !eof_out && !eof_err {
        if let Some(t) = timeout {
            if t > 0.0 {
                let dur = Duration::from_millis((t * 1000.0) as u64);
                let _ = tokio::time::timeout(dur, async {
                    tokio::select! {
                        _ = handle.stdout.notify.notified() => {},
                        _ = handle.stderr.notify.notified() => {},
                        _ = handle.wait_notify.notified() => {},
                    }
                })
                .await;
                let (o, t_o, e_o) = handle.stdout.drain(max).await;
                let (e, t_e, e_e) = handle.stderr.drain(max).await;
                out_bytes = o;
                err_bytes = e;
                tr_out = tr_out || t_o;
                tr_err = tr_err || t_e;
                eof_out = e_o;
                eof_err = e_e;
            }
        }
    }

    let (state_running, exit_code) = process_running_and_code(&handle).await;

    let (stdout_enc, stderr_enc) = encode_streams(req.encoding, &out_bytes, &err_bytes);

    Ok(Json(ProcessResult {
        process_id: Some(process_id.to_string()),
        running: Some(state_running),
        exit_code,
        stdout: Some(stdout_enc),
        stderr: Some(stderr_enc),
        encoding: Some(req.encoding),
        eof_stdout: Some(eof_out),
        eof_stderr: Some(eof_err),
        truncated: Some(tr_out || tr_err),
        ..Default::default()
    }))
}

fn encode_streams(enc: ProcessEncoding, stdout: &[u8], stderr: &[u8]) -> (String, String) {
    match enc {
        ProcessEncoding::Utf8 => (
            String::from_utf8_lossy(stdout).into_owned(),
            String::from_utf8_lossy(stderr).into_owned(),
        ),
        ProcessEncoding::Base64 => {
            let b = base64::engine::general_purpose::STANDARD;
            (b.encode(stdout), b.encode(stderr))
        }
    }
}

async fn process_running_and_code(handle: &ProcessHandle) -> (bool, Option<i32>) {
    let g = handle.state.lock().await;
    match *g {
        ProcessState::Running => (true, None),
        ProcessState::Exited { code } => (false, Some(code)),
        ProcessState::Terminated { signal: _ } => (false, None),
    }
}

// --- wait ---

async fn action_wait(
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let process_id = req.process_id.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "wait requires process_id".to_string(),
    ))?;
    let handle = session
        .processes
        .get(process_id)
        .ok_or((StatusCode::NOT_FOUND, "process not found".to_string()))?
        .clone();

    let ceiling = req
        .timeout_sec
        .unwrap_or(WAIT_DEFAULT_CEILING_SEC)
        .clamp(0.0, WAIT_DEFAULT_CEILING_SEC);

    if ceiling > 0.0 && handle.snapshot_running() {
        let dur = Duration::from_millis((ceiling * 1000.0) as u64);
        let _ = tokio::time::timeout(dur, handle.wait_notify.notified()).await;
    }

    let (state_running, exit_code) = process_running_and_code(&handle).await;
    let signal_num = if !state_running && exit_code.is_none() {
        // We reached the Terminated branch.
        let g = handle.state.lock().await;
        match *g {
            ProcessState::Terminated { signal } => Some(signal),
            _ => None,
        }
    } else {
        None
    };

    Ok(Json(ProcessResult {
        process_id: Some(process_id.to_string()),
        running: Some(state_running),
        exit_code,
        signal: signal_num,
        ..Default::default()
    }))
}

// --- list ---

fn action_list(session: Arc<crate::SessionState>) -> ProcessResult {
    let now = Instant::now();
    let mut summaries = Vec::new();
    for entry in session.processes.iter() {
        let h = entry.value();
        let (running, exit_code) = match h.state.try_lock() {
            Ok(g) => match *g {
                ProcessState::Running => (true, None),
                ProcessState::Exited { code } => (false, Some(code)),
                ProcessState::Terminated { .. } => (false, None),
            },
            Err(_) => (true, None),
        };
        summaries.push(ProcessSummary {
            process_id: h.process_id.clone(),
            command: h.command_line.clone(),
            running,
            exit_code,
            started_at_secs_ago: now.saturating_duration_since(h.started_at).as_secs(),
        });
    }
    summaries.sort_by(|a, b| b.started_at_secs_ago.cmp(&a.started_at_secs_ago));
    ProcessResult {
        processes: Some(summaries),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Public registry types reused by SessionState

pub type SessionProcessMap = DashMap<String, Arc<ProcessHandle>>;

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IsolationCfg, SessionState};

    fn tmp_state(enabled: bool, force_unsupported: bool) -> (tempfile::TempDir, Arc<AppState>) {
        let td = tempfile::tempdir().unwrap();
        let state = Arc::new(AppState {
            sandbox_root: td.path().to_path_buf(),
            session_root: td.path().to_path_buf(),
            chroot_root: None,
            sessions: DashMap::new(),
            isolation: IsolationCfg::default(),
            seccomp_filter: None,
            process_cfg: ProcessCfg {
                enabled,
                max_per_session: 4,
                buffer_bytes: 8 * 1024,
                idle_reap_sec: 3600,
                kill_group: false,
                force_unsupported,
            },
        });
        (td, state)
    }

    async fn make_session(state: &Arc<AppState>, name: &str) -> Arc<SessionState> {
        let cwd = state.sandbox_root.join(name);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        let sess = Arc::new(SessionState::new(cwd));
        state.sessions.insert(name.to_string(), sess.clone());
        sess
    }

    fn req(action: ProcessAction) -> ProcessReq {
        ProcessReq {
            action,
            process_id: None,
            command: None,
            args: None,
            cwd: None,
            env: None,
            input: None,
            encoding: ProcessEncoding::Utf8,
            eof: false,
            signal: None,
            timeout_sec: None,
            max_bytes: None,
        }
    }

    // 7.18 daemon kill switch
    #[tokio::test]
    async fn disabled_kill_switch_returns_404() {
        let (_td, state) = tmp_state(false, false);
        make_session(&state, "s").await;
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(req(ProcessAction::List)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    // 7.19 force_unsupported probe
    #[tokio::test]
    async fn force_unsupported_returns_501() {
        let (_td, state) = tmp_state(true, true);
        make_session(&state, "s").await;
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(req(ProcessAction::List)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::NOT_IMPLEMENTED);
        assert!(err.1.contains("process_unsupported"));
    }

    // 7.2 schema validation: start without command → 400
    #[tokio::test]
    async fn start_without_command_400() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut r = req(ProcessAction::Start);
        // command missing
        r.command = None;
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(r),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    // 7.2 read without process_id → 400
    #[tokio::test]
    async fn read_without_pid_400() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(req(ProcessAction::Read)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    // 7.1 start returns stable process_id; list shows it
    #[tokio::test]
    async fn start_then_list_returns_process() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut r = req(ProcessAction::Start);
        r.command = Some("/bin/sh".into());
        r.args = Some(vec!["-c".into(), "echo hello".into()]);
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(r),
        )
        .await
        .unwrap();
        let pid = resp.0.process_id.clone().expect("process_id");
        assert!(resp.0.running.unwrap());

        let listed = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(req(ProcessAction::List)),
        )
        .await
        .unwrap();
        let procs = listed.0.processes.expect("processes list");
        assert!(procs.iter().any(|p| p.process_id == pid));
    }

    // 7.12 wait with timeout returns running:true; subsequent wait returns exit
    #[tokio::test]
    async fn wait_timeout_then_exit() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut start_r = req(ProcessAction::Start);
        start_r.command = Some("/bin/sh".into());
        start_r.args = Some(vec!["-c".into(), "sleep 0.3; exit 7".into()]);
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(start_r),
        )
        .await
        .unwrap();
        let pid = resp.0.process_id.unwrap();

        let mut wait_r = req(ProcessAction::Wait);
        wait_r.process_id = Some(pid.clone());
        wait_r.timeout_sec = Some(0.05);
        let timed = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(wait_r),
        )
        .await
        .unwrap();
        assert_eq!(timed.0.running, Some(true));
        assert!(timed.0.exit_code.is_none());

        let mut wait_r = req(ProcessAction::Wait);
        wait_r.process_id = Some(pid);
        wait_r.timeout_sec = Some(3.0);
        let done = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(wait_r),
        )
        .await
        .unwrap();
        assert_eq!(done.0.running, Some(false));
        assert_eq!(done.0.exit_code, Some(7));
    }

    // 7.13 non-zero exit is not an HTTP error
    #[tokio::test]
    async fn nonzero_exit_returns_200() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut start_r = req(ProcessAction::Start);
        start_r.command = Some("/bin/sh".into());
        start_r.args = Some(vec!["-c".into(), "exit 42".into()]);
        let pid = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(start_r),
        )
        .await
        .unwrap()
        .0
        .process_id
        .unwrap();
        let mut wait_r = req(ProcessAction::Wait);
        wait_r.process_id = Some(pid);
        wait_r.timeout_sec = Some(3.0);
        let resp = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(wait_r),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.exit_code, Some(42));
    }

    // 7.4 incremental read across two passes
    #[tokio::test]
    async fn incremental_read_returns_disjoint_slices() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut start_r = req(ProcessAction::Start);
        start_r.command = Some("/bin/sh".into());
        start_r.args = Some(vec![
            "-c".into(),
            "printf 'A'; sleep 0.1; printf 'B'; sleep 0.1; printf 'C'".into(),
        ]);
        let pid = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(start_r),
        )
        .await
        .unwrap()
        .0
        .process_id
        .unwrap();

        let mut concat = String::new();
        // Loop reading until EOF
        for _ in 0..20 {
            let mut read_r = req(ProcessAction::Read);
            read_r.process_id = Some(pid.clone());
            read_r.timeout_sec = Some(0.2);
            let resp = tool_process(
                axum::extract::State(state.clone()),
                axum::extract::Path("s".into()),
                axum::Json(read_r),
            )
            .await
            .unwrap();
            if let Some(s) = resp.0.stdout.as_deref() {
                concat.push_str(s);
            }
            if resp.0.eof_stdout == Some(true) {
                break;
            }
        }
        assert_eq!(concat, "ABC");
    }

    // 7.5 read after exit returns buffered data with eof_stdout
    #[tokio::test]
    async fn read_after_exit_still_returns_buffer() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut start_r = req(ProcessAction::Start);
        start_r.command = Some("/bin/sh".into());
        start_r.args = Some(vec!["-c".into(), "printf 'X'".into()]);
        let pid = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(start_r),
        )
        .await
        .unwrap()
        .0
        .process_id
        .unwrap();
        // Wait for exit
        let mut wait_r = req(ProcessAction::Wait);
        wait_r.process_id = Some(pid.clone());
        wait_r.timeout_sec = Some(3.0);
        let _ = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(wait_r),
        )
        .await
        .unwrap();

        // Allow drain task to complete by giving the runtime a tick.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut read_r = req(ProcessAction::Read);
        read_r.process_id = Some(pid);
        let resp = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(read_r),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.stdout.as_deref(), Some("X"));
        assert_eq!(resp.0.eof_stdout, Some(true));
        assert_eq!(resp.0.running, Some(false));
    }

    // 7.16 per-session cap enforcement: cap+1 start returns 429
    #[tokio::test]
    async fn per_session_cap_enforced() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        for _ in 0..4 {
            let mut start_r = req(ProcessAction::Start);
            start_r.command = Some("/bin/sh".into());
            start_r.args = Some(vec!["-c".into(), "sleep 5".into()]);
            let _ = tool_process(
                axum::extract::State(state.clone()),
                axum::extract::Path("s".into()),
                axum::Json(start_r),
            )
            .await
            .unwrap();
        }
        let mut over = req(ProcessAction::Start);
        over.command = Some("/bin/sh".into());
        over.args = Some(vec!["-c".into(), "true".into()]);
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(over),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::TOO_MANY_REQUESTS);
    }

    // 7.3 cwd traversal rejected on start
    #[tokio::test]
    async fn start_cwd_traversal_400() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let mut r = req(ProcessAction::Start);
        r.command = Some("/bin/true".into());
        r.cwd = Some("../escape".into());
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(r),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    // Schema deserialization sanity: snake_case action, kebab encoding
    #[test]
    fn schema_deserializes_actions() {
        let r: ProcessReq = serde_json::from_value(serde_json::json!({
            "action": "start", "command": "/bin/true"
        }))
        .unwrap();
        assert_eq!(r.action, ProcessAction::Start);
        assert_eq!(r.encoding, ProcessEncoding::Utf8);

        let r: ProcessReq = serde_json::from_value(serde_json::json!({
            "action": "read",
            "process_id": "abc",
            "encoding": "base64",
        }))
        .unwrap();
        assert_eq!(r.action, ProcessAction::Read);
        assert_eq!(r.encoding, ProcessEncoding::Base64);
    }
}
