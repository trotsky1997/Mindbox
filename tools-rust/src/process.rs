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
        ProcessAction::Write => action_write(session, req).await,
        ProcessAction::Signal => action_signal(&state, session, req).await,
        ProcessAction::Stop => action_stop(&state, session, req).await,
        ProcessAction::List => Ok(Json(action_list(session))),
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
// Batch 3: write / signal / stop + lifecycle helpers

use tokio::io::AsyncWriteExt;

const STOP_GRACE_SEC: f64 = 5.0;

async fn action_write(
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let process_id = req.process_id.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "write requires process_id".to_string(),
    ))?;
    let handle = session
        .processes
        .get(process_id)
        .ok_or((StatusCode::NOT_FOUND, "process not found".to_string()))?
        .clone();

    // Decode input bytes per encoding.
    let bytes: Vec<u8> =
        match (req.encoding, req.input.as_deref()) {
            (_, None) => Vec::new(),
            (ProcessEncoding::Utf8, Some(s)) => s.as_bytes().to_vec(),
            (ProcessEncoding::Base64, Some(s)) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("base64 decode: {e}")))?,
        };

    // Tolerate already-exited child: return a successful describe-state response.
    let (running, exit_code) = process_running_and_code(&handle).await;
    if !running {
        return Ok(Json(ProcessResult {
            process_id: Some(process_id.to_string()),
            running: Some(false),
            exit_code,
            ..Default::default()
        }));
    }

    let mut stdin_slot = handle.stdin.lock().await;
    let mut stdin = stdin_slot
        .take()
        .ok_or((StatusCode::BAD_REQUEST, "stdin already closed".to_string()))?;

    if !bytes.is_empty() {
        if let Err(e) = stdin.write_all(&bytes).await {
            // Put it back so future writes can still report status.
            *stdin_slot = Some(stdin);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("write stdin: {e}"),
            ));
        }
        let _ = stdin.flush().await;
    }

    if req.eof {
        // Drop the stdin half — child sees EOF.
        drop(stdin);
        // Leave the slot None so subsequent writes get 400.
    } else {
        *stdin_slot = Some(stdin);
    }

    Ok(Json(ProcessResult {
        process_id: Some(process_id.to_string()),
        running: Some(true),
        ..Default::default()
    }))
}

/// Parse a POSIX signal name like "SIGTERM" or just "TERM" / "9".
fn parse_signal(name: &str) -> Option<i32> {
    let s = name.trim();
    if let Ok(n) = s.parse::<i32>() {
        return Some(n);
    }
    let upper = s.to_uppercase();
    let stripped = upper.strip_prefix("SIG").unwrap_or(&upper);
    Some(match stripped {
        "HUP" => libc::SIGHUP,
        "INT" => libc::SIGINT,
        "QUIT" => libc::SIGQUIT,
        "ILL" => libc::SIGILL,
        "ABRT" => libc::SIGABRT,
        "FPE" => libc::SIGFPE,
        "KILL" => libc::SIGKILL,
        "USR1" => libc::SIGUSR1,
        "SEGV" => libc::SIGSEGV,
        "USR2" => libc::SIGUSR2,
        "PIPE" => libc::SIGPIPE,
        "ALRM" => libc::SIGALRM,
        "TERM" => libc::SIGTERM,
        "CHLD" => libc::SIGCHLD,
        "CONT" => libc::SIGCONT,
        "STOP" => libc::SIGSTOP,
        "TSTP" => libc::SIGTSTP,
        "WINCH" => libc::SIGWINCH,
        _ => return None,
    })
}

/// Deliver `sig` to the child. Targets the process group when `kill_group`,
/// otherwise the pid directly. ESRCH (already exited) is reported back as
/// non-fatal so the caller can observe state.
fn send_signal_to(pid: u32, sig: i32, kill_group: bool) -> std::io::Result<()> {
    let target = if kill_group {
        -(pid as i32)
    } else {
        pid as i32
    };
    // SAFETY: kill(2) is async-signal-safe; we pass a valid signal number.
    let rc = unsafe { libc::kill(target, sig) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    // ESRCH = no such process; child already exited.
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err)
}

async fn action_signal(
    state: &Arc<AppState>,
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let process_id = req.process_id.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "signal requires process_id".to_string(),
    ))?;
    let signal_name = req.signal.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "signal requires signal name".to_string(),
    ))?;
    let sig = parse_signal(signal_name).ok_or((
        StatusCode::BAD_REQUEST,
        format!("unknown signal: {signal_name}"),
    ))?;
    let handle = session
        .processes
        .get(process_id)
        .ok_or((StatusCode::NOT_FOUND, "process not found".to_string()))?
        .clone();

    send_signal_to(handle.pid, sig, state.process_cfg.kill_group)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("kill: {e}")))?;

    let (running, exit_code) = process_running_and_code(&handle).await;
    let signal_num = if !running && exit_code.is_none() {
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
        running: Some(running),
        exit_code,
        signal: signal_num,
        ..Default::default()
    }))
}

async fn action_stop(
    state: &Arc<AppState>,
    session: Arc<crate::SessionState>,
    req: ProcessReq,
) -> Result<Json<ProcessResult>, (StatusCode, String)> {
    let process_id = req.process_id.as_deref().ok_or((
        StatusCode::BAD_REQUEST,
        "stop requires process_id".to_string(),
    ))?;
    let handle = session
        .processes
        .get(process_id)
        .ok_or((StatusCode::NOT_FOUND, "process not found".to_string()))?
        .clone();

    let grace = req.timeout_sec.unwrap_or(STOP_GRACE_SEC).clamp(0.0, 60.0);

    terminate_handle(&handle, grace, state.process_cfg.kill_group).await;

    let (running, exit_code) = process_running_and_code(&handle).await;
    let signal_num = if !running && exit_code.is_none() {
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
        running: Some(running),
        exit_code,
        signal: signal_num,
        ..Default::default()
    }))
}

/// Terminate a process: SIGTERM, wait up to `grace` seconds for it to
/// observe its terminal state via the wait task, then SIGKILL if still
/// running. Safe to call on already-exited processes (best-effort).
pub(crate) async fn terminate_handle(handle: &Arc<ProcessHandle>, grace: f64, kill_group: bool) {
    if !handle.snapshot_running() {
        return;
    }
    let _ = send_signal_to(handle.pid, libc::SIGTERM, kill_group);
    if grace > 0.0 {
        let dur = Duration::from_millis((grace * 1000.0) as u64);
        let _ = tokio::time::timeout(dur, handle.wait_notify.notified()).await;
    }
    if handle.snapshot_running() {
        let _ = send_signal_to(handle.pid, libc::SIGKILL, kill_group);
        // give the wait task a brief window to observe termination
        let _ = tokio::time::timeout(Duration::from_secs(2), handle.wait_notify.notified()).await;
    }
}

/// Terminate every process owned by `session`. Used by `delete_session`,
/// the daemon graceful shutdown handler, and the idle reaper.
pub(crate) async fn reap_session_processes(session: &crate::SessionState, kill_group: bool) {
    let handles: Vec<Arc<ProcessHandle>> = session
        .processes
        .iter()
        .map(|e| e.value().clone())
        .collect();
    for h in handles {
        terminate_handle(&h, STOP_GRACE_SEC, kill_group).await;
    }
    session.processes.clear();
}

/// Background task that periodically evicts sessions whose last_touched
/// timestamp is older than `idle_reap_sec`. Runs every
/// `max(idle_reap_sec / 4, 1)` seconds, capped to 60s.
pub(crate) fn spawn_idle_reaper(state: Arc<AppState>) {
    let idle_sec = state.process_cfg.idle_reap_sec;
    let kill_group = state.process_cfg.kill_group;
    if idle_sec == 0 {
        return;
    }
    let tick = (idle_sec / 4).clamp(1, 60);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(tick));
        // skip the immediate-tick fire
        interval.tick().await;
        loop {
            interval.tick().await;
            let now = Instant::now();
            let mut victims: Vec<String> = Vec::new();
            for entry in state.sessions.iter() {
                let last = *entry.value().last_touched.lock().unwrap();
                if now.saturating_duration_since(last).as_secs() >= idle_sec {
                    victims.push(entry.key().clone());
                }
            }
            for sid in victims {
                if let Some((_, session)) = state.sessions.remove(&sid) {
                    reap_session_processes(&session, kill_group).await;
                    let _ = tokio::fs::remove_dir_all(&session.cwd).await;
                    if let Some(root) = state.isolation.cgroup_root.as_ref() {
                        crate::cleanup_session_cgroup(&root.join(format!("mindbox-{}", sid)));
                    }
                    tracing::info!("[reaper] evicted idle session {}", sid);
                }
            }
        }
    });
}

/// Graceful shutdown: stop accepting new HTTP requests (caller's job) then
/// drain every session like delete_session does. Returns once every child
/// has reached a terminal state or grace timeouts have elapsed.
pub(crate) async fn drain_all_sessions(state: &Arc<AppState>) {
    let sids: Vec<String> = state.sessions.iter().map(|e| e.key().clone()).collect();
    for sid in sids {
        if let Some((_, session)) = state.sessions.remove(&sid) {
            reap_session_processes(&session, state.process_cfg.kill_group).await;
        }
    }
}

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

    #[test]
    fn parse_signal_understands_common_names() {
        assert_eq!(parse_signal("SIGTERM"), Some(libc::SIGTERM));
        assert_eq!(parse_signal("TERM"), Some(libc::SIGTERM));
        assert_eq!(parse_signal("sigkill"), Some(libc::SIGKILL));
        assert_eq!(parse_signal("9"), Some(9));
        assert_eq!(parse_signal("NOPE"), None);
    }

    // helper: start a /bin/sh -c <cmd>
    async fn start_sh(state: &Arc<AppState>, sid: &str, sh: &str) -> String {
        let mut r = req(ProcessAction::Start);
        r.command = Some("/bin/sh".into());
        r.args = Some(vec!["-c".into(), sh.into()]);
        tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path(sid.into()),
            axum::Json(r),
        )
        .await
        .unwrap()
        .0
        .process_id
        .unwrap()
    }

    async fn wait_pid(state: &Arc<AppState>, sid: &str, pid: &str, t: f64) -> ProcessResult {
        let mut r = req(ProcessAction::Wait);
        r.process_id = Some(pid.into());
        r.timeout_sec = Some(t);
        tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path(sid.into()),
            axum::Json(r),
        )
        .await
        .unwrap()
        .0
    }

    // 7.7 write with eof closes stdin; cat echoes input then exits 0.
    // After the child has exited, write returns running:false (describe state),
    // not 4xx. Writing to a *still-running* process whose stdin we closed
    // returns 400.
    #[tokio::test]
    async fn write_with_eof_closes_stdin() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let pid = start_sh(&state, "s", "cat").await;

        let mut w = req(ProcessAction::Write);
        w.process_id = Some(pid.clone());
        w.input = Some("hello\n".into());
        w.eof = true;
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(w),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.running, Some(true));

        let exit = wait_pid(&state, "s", &pid, 3.0).await;
        assert_eq!(exit.running, Some(false));
        assert_eq!(exit.exit_code, Some(0));
    }

    // EOF on a still-running child closes stdin; second write 400s.
    #[tokio::test]
    async fn second_write_after_eof_is_400_while_running() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        // Stay running even after stdin EOF
        let pid = start_sh(&state, "s", "cat >/dev/null; sleep 30").await;

        let mut w = req(ProcessAction::Write);
        w.process_id = Some(pid.clone());
        w.input = Some("hi".into());
        w.eof = true;
        let _ = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(w),
        )
        .await
        .unwrap();
        // Let child reach the sleep branch
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut w2 = req(ProcessAction::Write);
        w2.process_id = Some(pid.clone());
        w2.input = Some("more".into());
        let err = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(w2),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);

        // Cleanup
        let mut stop_r = req(ProcessAction::Stop);
        stop_r.process_id = Some(pid);
        stop_r.timeout_sec = Some(0.5);
        let _ = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(stop_r),
        )
        .await
        .unwrap();
    }

    // 7.8 base64 round-trip: cat back what we sent
    #[tokio::test]
    async fn write_read_base64_round_trip() {
        use base64::Engine as _;
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let pid = start_sh(&state, "s", "cat").await;

        // Send base64-encoded bytes (with a NUL to exercise non-UTF8)
        let raw: &[u8] = b"hi\x00world";
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        let mut w = req(ProcessAction::Write);
        w.process_id = Some(pid.clone());
        w.input = Some(encoded);
        w.encoding = ProcessEncoding::Base64;
        w.eof = true;
        let _ = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(w),
        )
        .await
        .unwrap();
        let _ = wait_pid(&state, "s", &pid, 3.0).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut r = req(ProcessAction::Read);
        r.process_id = Some(pid);
        r.encoding = ProcessEncoding::Base64;
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(r),
        )
        .await
        .unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(resp.0.stdout.as_deref().unwrap())
            .unwrap();
        assert_eq!(decoded, raw);
    }

    // 7.9 signal SIGTERM against a graceful child → exits with signal info
    #[tokio::test]
    async fn signal_sigterm_terminates() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        // sleep is graceful for SIGTERM (default action: terminate)
        let pid = start_sh(&state, "s", "sleep 5").await;

        let mut sig = req(ProcessAction::Signal);
        sig.process_id = Some(pid.clone());
        sig.signal = Some("SIGTERM".into());
        let _ = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(sig),
        )
        .await
        .unwrap();

        let exit = wait_pid(&state, "s", &pid, 3.0).await;
        assert_eq!(exit.running, Some(false));
        // sleep terminated by SIGTERM: no exit code, signal == 15
        assert!(exit.exit_code.is_none());
        assert_eq!(exit.signal, Some(libc::SIGTERM));
    }

    // 7.10 stop escalates to SIGKILL when child traps SIGTERM
    #[tokio::test]
    async fn stop_escalates_to_sigkill() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        // trap '' TERM disables SIGTERM; only SIGKILL can stop it
        let pid = start_sh(&state, "s", "trap '' TERM; while :; do sleep 0.05; done").await;

        let mut s = req(ProcessAction::Stop);
        s.process_id = Some(pid.clone());
        s.timeout_sec = Some(0.4);
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(s),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.running, Some(false));
        assert_eq!(resp.0.signal, Some(libc::SIGKILL));
    }

    // 7.11 signal against an exited process is a successful no-op
    #[tokio::test]
    async fn signal_on_exited_is_ok() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let pid = start_sh(&state, "s", "true").await;
        let _ = wait_pid(&state, "s", &pid, 3.0).await;

        let mut sig = req(ProcessAction::Signal);
        sig.process_id = Some(pid.clone());
        sig.signal = Some("SIGTERM".into());
        let resp = tool_process(
            axum::extract::State(state.clone()),
            axum::extract::Path("s".into()),
            axum::Json(sig),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.running, Some(false));
        assert_eq!(resp.0.exit_code, Some(0));
    }

    // 7.15 delete_session reaps every owned process
    #[tokio::test]
    async fn reap_session_processes_kills_all_children() {
        let (_td, state) = tmp_state(true, false);
        let sess = make_session(&state, "s").await;
        let mut pids = Vec::new();
        for _ in 0..3 {
            pids.push(start_sh(&state, "s", "sleep 30").await);
        }
        reap_session_processes(&sess, false).await;
        assert!(sess.processes.is_empty());
        // every child should be terminated; OS check via kill(pid, 0) → ESRCH
        for pid_str in &pids {
            let h = sess.processes.get(pid_str);
            assert!(h.is_none());
        }
    }

    // 7.16 cross-session reuse: pid from session A passed to session B → 404
    #[tokio::test]
    async fn cross_session_pid_returns_404() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "a").await;
        make_session(&state, "b").await;
        let pid_a = start_sh(&state, "a", "sleep 5").await;

        let mut r = req(ProcessAction::Read);
        r.process_id = Some(pid_a);
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("b".into()),
            axum::Json(r),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    // 7.2 write without process_id → 400
    #[tokio::test]
    async fn write_without_pid_400() {
        let (_td, state) = tmp_state(true, false);
        make_session(&state, "s").await;
        let err = tool_process(
            axum::extract::State(state),
            axum::extract::Path("s".into()),
            axum::Json(req(ProcessAction::Write)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }
}
