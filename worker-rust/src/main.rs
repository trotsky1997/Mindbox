// worker-rust v0.3: full protobuf transport.
//
// api-rust ↔ worker parent: protobuf Request / Response (frame body)
// worker parent ↔ child:    protobuf Job / ChildResponse (frame body, via socketpair)
//
// The Python helper (sandbox_helper.py) uses google.protobuf to parse Job and
// build ChildResponse; the pb2 module is shipped as /inspect_pb2.py in each
// template image and added to sys.path via ENV PYTHONPATH="/".

use anyhow::Result;
use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};
use nix::unistd::{fork, ForkResult};
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/inspect.rs"));
}

// ---- config -------------------------------------------------------------

struct Config {
    socket_path: PathBuf,
    pool_size: usize,
    max_timeout: u32,
    prewarm: Vec<String>,
    max_age: Duration,
    max_idle: Duration,
    reaper_interval: Duration,
    max_total_rss_mb: u64,
}

impl Config {
    fn from_env() -> Self {
        let env_u64 = |k: &str, d: u64| std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d);
        Self {
            socket_path: std::env::var("WORKER_SOCKET_PATH")
                .unwrap_or_else(|_| "/sockets/worker.sock".into()).into(),
            pool_size: env_u64("WORKER_POOL_SIZE", 32) as usize,
            max_timeout: env_u64("WORKER_MAX_TIMEOUT", 60) as u32,
            prewarm: std::env::var("WORKER_PREWARM_MODULES").unwrap_or_default()
                .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            max_age: Duration::from_secs(env_u64("WORKER_MAX_AGE_SECONDS", 600)),
            max_idle: Duration::from_secs(env_u64("WORKER_MAX_IDLE_SECONDS", 120)),
            reaper_interval: Duration::from_secs(env_u64("WORKER_REAPER_INTERVAL_SEC", 5)),
            max_total_rss_mb: env_u64("WORKER_MAX_TOTAL_RSS_MB", 0),
        }
    }
}

// ---- frame I/O ----------------------------------------------------------

const MAX_FRAME: usize = 64 * 1024 * 1024;

fn write_all_fd(fd: RawFd, mut buf: &[u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        let r = unsafe { libc::write(fd, buf.as_ptr() as *const _, buf.len()) };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted { continue; }
            return Err(e);
        }
        buf = &buf[r as usize..];
    }
    Ok(())
}
fn read_exact_fd(fd: RawFd, mut buf: &mut [u8]) -> std::io::Result<()> {
    while !buf.is_empty() {
        let r = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len()) };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted { continue; }
            return Err(e);
        }
        if r == 0 { return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof")); }
        buf = &mut buf[r as usize..];
    }
    Ok(())
}
fn send_frame_fd(fd: RawFd, payload: &[u8]) -> std::io::Result<()> {
    let n = (payload.len() as u32).to_be_bytes();
    write_all_fd(fd, &n)?;
    write_all_fd(fd, payload)
}
fn recv_frame_fd(fd: RawFd) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    read_exact_fd(fd, &mut hdr)?;
    let n = u32::from_be_bytes(hdr) as usize;
    if n > MAX_FRAME { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large")); }
    let mut buf = vec![0u8; n];
    read_exact_fd(fd, &mut buf)?;
    Ok(buf)
}
async fn send_frame_async(s: &mut UnixStream, payload: &[u8]) -> std::io::Result<()> {
    let n = (payload.len() as u32).to_be_bytes();
    s.write_all(&n).await?;
    s.write_all(payload).await
}
async fn recv_frame_async(s: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    s.read_exact(&mut hdr).await?;
    let n = u32::from_be_bytes(hdr) as usize;
    if n > MAX_FRAME { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large")); }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).await?;
    Ok(buf)
}

// ---- Python sandbox helper ----------------------------------------------

const SANDBOX_HELPER_PY: &str = include_str!("sandbox_helper.py");

// ---- child main: protobuf in, protobuf out ------------------------------

fn child_main(fd: RawFd) -> ! {
    unsafe { libc::signal(libc::SIGCHLD, libc::SIG_DFL); }
    loop {
        let job_bytes = match recv_frame_fd(fd) {
            Ok(b) => b,
            Err(_) => unsafe { libc::_exit(0); },
        };
        // Python returns serialized ChildResponse bytes.
        let resp_bytes: Vec<u8> = Python::with_gil(|py| {
            let main_mod = match py.import_bound("__main__") { Ok(m) => m, Err(_) => return fallback_child_err("__main__ missing") };
            let globals = main_mod.dict();
            let runner = match globals.get_item("_run_sandbox_pb") {
                Ok(Some(r)) => r,
                _ => return fallback_child_err("_run_sandbox_pb missing"),
            };
            let py_bytes = PyBytes::new_bound(py, &job_bytes);
            match runner.call1((py_bytes,)) {
                Ok(r) => match r.downcast::<PyBytes>() {
                    Ok(b) => b.as_bytes().to_vec(),
                    Err(_) => fallback_child_err("non-bytes return"),
                },
                Err(e) => fallback_child_err(&format!("py err: {}", e)),
            }
        });
        // Quick peek: did helper set expire=true?
        let expire = match pb::ChildResponse::decode(&*resp_bytes) {
            Ok(r) => r.expire,
            Err(_) => true,  // bad pb → defensively respawn
        };
        let _ = send_frame_fd(fd, &resp_bytes);
        if expire { unsafe { libc::_exit(0); } }
    }
}

fn fallback_child_err(msg: &str) -> Vec<u8> {
    let resp = pb::ChildResponse {
        stdout: String::new(),
        stderr: msg.to_string(),
        exit_code: 1,
        expire: true,
        lifecycle: Some(pb::Lifecycle {
            expire_reason: "fallback".to_string(),
            ..Default::default()
        }),
        output_files: ::std::collections::HashMap::new(),
        deleted_files: vec![],
        output_files_b64: Default::default(),
    };
    resp.encode_to_vec()
}

// ---- pool entry + stats --------------------------------------------------

struct IdleEntry {
    pid: u32, fd: OwnedFd,
    born_at: Instant, last_used: Instant,
    requests_served: u64, last_rss_mb: u64,
}

struct Stats {
    started_at: Instant,
    requests_total: AtomicU64,
    forks_total: AtomicU64,
    respawns_by_reason: Mutex<HashMap<String, u64>>,
    kills_hard_timeout: AtomicU64,
    kills_reaper_age: AtomicU64,
    kills_reaper_idle: AtomicU64,
}
impl Stats {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            requests_total: AtomicU64::new(0),
            forks_total: AtomicU64::new(0),
            respawns_by_reason: Mutex::new(HashMap::new()),
            kills_hard_timeout: AtomicU64::new(0),
            kills_reaper_age: AtomicU64::new(0),
            kills_reaper_idle: AtomicU64::new(0),
        }
    }
    fn bump_respawn(&self, reason: &str) {
        *self.respawns_by_reason.lock().unwrap().entry(reason.to_string()).or_insert(0) += 1;
    }
}

type IdleQueue = Arc<Mutex<VecDeque<IdleEntry>>>;

fn forker_thread(rx: mpsc::Receiver<()>, idle: IdleQueue, stats: Arc<Stats>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        let Ok(()) = rx.recv_timeout(Duration::from_secs(1)) else { continue; };
        let (parent_sock, child_sock) = match socketpair(AddressFamily::Unix, SockType::Stream, None, SockFlag::empty()) {
            Ok(p) => p,
            Err(e) => { eprintln!("[forker] socketpair: {}", e); continue; }
        };
        let child_fd_raw: RawFd = child_sock.as_raw_fd();
        let enqueue: Option<IdleEntry> = Python::with_gil(|_py| {
            unsafe { pyo3::ffi::PyOS_BeforeFork(); }
            match unsafe { fork() } {
                Ok(ForkResult::Child) => {
                    unsafe { pyo3::ffi::PyOS_AfterFork_Child(); }
                    drop(parent_sock);
                    child_main(child_fd_raw);
                }
                Ok(ForkResult::Parent { child }) => {
                    unsafe { pyo3::ffi::PyOS_AfterFork_Parent(); }
                    drop(child_sock);
                    let now = Instant::now();
                    Some(IdleEntry {
                        pid: child.as_raw() as u32, fd: parent_sock,
                        born_at: now, last_used: now,
                        requests_served: 0, last_rss_mb: 0,
                    })
                }
                Err(e) => {
                    unsafe { pyo3::ffi::PyOS_AfterFork_Parent(); }
                    eprintln!("[forker] fork err: {}", e);
                    None
                }
            }
        });
        if let Some(entry) = enqueue {
            stats.forks_total.fetch_add(1, Ordering::Relaxed);
            idle.lock().unwrap().push_back(entry);
        }
    }
}

fn reaper_thread(state: Arc<AppState>) {
    loop {
        std::thread::sleep(state.reaper_interval);
        if state.shutdown.load(Ordering::Relaxed) { return; }
        let now = Instant::now();
        let mut to_drain: Vec<(&'static str, IdleEntry)> = Vec::new();
        {
            let mut idle = state.idle.lock().unwrap();
            let mut keep = VecDeque::with_capacity(idle.len());
            while let Some(entry) = idle.pop_front() {
                let age = now.duration_since(entry.born_at);
                let idle_for = now.duration_since(entry.last_used);
                if age > state.max_age {
                    to_drain.push(("age", entry));
                } else if idle_for > state.max_idle {
                    to_drain.push(("idle_age", entry));
                } else {
                    keep.push_back(entry);
                }
            }
            *idle = keep;
        }
        if state.max_total_rss_mb > 0 {
            let victim: Option<IdleEntry> = {
                let mut idle = state.idle.lock().unwrap();
                let total: u64 = idle.iter().map(|e| e.last_rss_mb).sum();
                let half_idle = idle.len() >= state.pool_size / 2;
                if total > state.max_total_rss_mb && half_idle && !idle.is_empty() {
                    let oldest_idx = idle.iter().enumerate()
                        .min_by_key(|(_, e)| e.born_at).map(|(i, _)| i).unwrap_or(0);
                    idle.remove(oldest_idx)
                } else { None }
            };
            if let Some(entry) = victim { to_drain.push(("memory_pressure", entry)); }
        }
        for (reason, entry) in to_drain {
            match reason {
                "age" => { state.stats.kills_reaper_age.fetch_add(1, Ordering::Relaxed); }
                "idle_age" => { state.stats.kills_reaper_idle.fetch_add(1, Ordering::Relaxed); }
                _ => {}
            }
            state.stats.bump_respawn(reason);
            drop(entry.fd);
            let _ = state.fork_tx.lock().unwrap().send(());
        }
    }
}

// ---- AppState ------------------------------------------------------------

struct AppState {
    idle: IdleQueue,
    fork_tx: Mutex<mpsc::Sender<()>>,
    pool_size: usize,
    max_timeout: u32,
    prewarm: Vec<String>,
    stats: Arc<Stats>,
    max_age: Duration,
    max_idle: Duration,
    reaper_interval: Duration,
    max_total_rss_mb: u64,
    shutdown: Arc<AtomicBool>,
}
impl AppState {
    fn trigger_refill(&self) { let _ = self.fork_tx.lock().unwrap().send(()); }
    fn pop_idle(&self) -> Option<IdleEntry> { self.idle.lock().unwrap().pop_front() }
    fn push_idle(&self, e: IdleEntry) { self.idle.lock().unwrap().push_back(e); }
    fn idle_count(&self) -> usize { self.idle.lock().unwrap().len() }
}

async fn acquire_child(state: &AppState, deadline: Instant) -> Option<IdleEntry> {
    loop {
        if let Some(e) = state.pop_idle() { return Some(e); }
        if Instant::now() >= deadline { return None; }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ---- dispatch helpers ---------------------------------------------------

fn health_response_pb(state: &AppState) -> pb::HealthResp {
    pb::HealthResp {
        ok: true,
        pid: std::process::id(),
        pool_size: state.pool_size as u32,
        idle: state.idle_count() as u32,
        prewarm: state.prewarm.clone(),
    }
}

fn stats_response_json(state: &AppState) -> String {
    let idle = state.idle.lock().unwrap();
    let children: Vec<Value> = idle.iter().map(|e| json!({
        "pid": e.pid,
        "age_sec": e.born_at.elapsed().as_secs(),
        "idle_sec": e.last_used.elapsed().as_secs(),
        "requests_served": e.requests_served,
        "last_rss_mb": e.last_rss_mb,
        "state": "idle",
    })).collect();
    let idle_count = idle.len();
    drop(idle);
    let respawns = state.stats.respawns_by_reason.lock().unwrap().clone();
    let v = json!({
        "uptime_sec": state.stats.started_at.elapsed().as_secs(),
        "pool_size": state.pool_size,
        "idle": idle_count,
        "active_approx": state.pool_size.saturating_sub(idle_count),
        "prewarm": state.prewarm.clone(),
        "config": {
            "max_age_sec": state.max_age.as_secs(),
            "max_idle_sec": state.max_idle.as_secs(),
            "reaper_interval_sec": state.reaper_interval.as_secs(),
            "max_total_rss_mb": state.max_total_rss_mb,
        },
        "lifetime": {
            "requests_total": state.stats.requests_total.load(Ordering::Relaxed),
            "forks_total": state.stats.forks_total.load(Ordering::Relaxed),
            "respawns_by_reason": respawns,
            "kills_hard_timeout": state.stats.kills_hard_timeout.load(Ordering::Relaxed),
            "kills_reaper_age": state.stats.kills_reaper_age.load(Ordering::Relaxed),
            "kills_reaper_idle": state.stats.kills_reaper_idle.load(Ordering::Relaxed),
        },
        "children": children,
    });
    v.to_string()
}

fn drain_response_json(state: &AppState, pid: Option<u32>, reason: Option<String>) -> String {
    let reason = reason.unwrap_or_else(|| "manual".to_string());
    let v = match pid {
        Some(target_pid) => {
            let entry = {
                let mut idle = state.idle.lock().unwrap();
                let pos = idle.iter().position(|e| e.pid == target_pid);
                pos.and_then(|i| idle.remove(i))
            };
            if let Some(entry) = entry {
                state.stats.bump_respawn(&format!("drain_{}", reason));
                drop(entry.fd);
                state.trigger_refill();
                json!({"drained": target_pid, "method": "fd_close", "found": "idle"})
            } else {
                unsafe { libc::kill(target_pid as i32, libc::SIGTERM); }
                state.stats.bump_respawn(&format!("drain_active_{}", reason));
                json!({"drained": target_pid, "method": "sigterm", "found": "active_or_dead"})
            }
        }
        None => {
            let entries: Vec<IdleEntry> = state.idle.lock().unwrap().drain(..).collect();
            let n = entries.len();
            for e in entries {
                state.stats.bump_respawn(&format!("drain_{}", reason));
                drop(e.fd);
                state.trigger_refill();
            }
            json!({"drained_idle": n, "method": "fd_close"})
        }
    };
    v.to_string()
}

async fn exec_with_child(state: Arc<AppState>, job: pb::Job) -> pb::ExecResult {
    let start = Instant::now();
    let timeout_s = (job.timeout.max(1).min(state.max_timeout)) as u64;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut entry = match acquire_child(&state, deadline).await {
        Some(c) => c,
        None => return pb::ExecResult {
            stdout: String::new(), stderr: "no idle child".into(),
            exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![], output_files_b64: Default::default() },
    };
    let raw_fd = entry.fd.into_raw_fd();
    let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(raw_fd) };
    if std_stream.set_nonblocking(true).is_err() {
        state.trigger_refill();
        return pb::ExecResult { stdout: String::new(), stderr: "nonblock fail".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()};
    }
    let mut stream = match UnixStream::from_std(std_stream) {
        Ok(s) => s,
        Err(_) => { state.trigger_refill(); return pb::ExecResult { stdout: String::new(), stderr: "from_std fail".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()}; }
    };
    let job_bytes = job.encode_to_vec();
    let n = (job_bytes.len() as u32).to_be_bytes();
    if stream.write_all(&n).await.is_err() || stream.write_all(&job_bytes).await.is_err() {
        unsafe { libc::kill(entry.pid as i32, libc::SIGKILL); }
        state.stats.kills_hard_timeout.fetch_add(1, Ordering::Relaxed);
        state.trigger_refill();
        return pb::ExecResult { stdout: String::new(), stderr: "send fail".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()};
    }

    let recv_fut = async {
        let mut hdr = [0u8; 4];
        stream.read_exact(&mut hdr).await?;
        let n = u32::from_be_bytes(hdr) as usize;
        if n > MAX_FRAME {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
        }
        let mut body = vec![0u8; n];
        stream.read_exact(&mut body).await?;
        Ok::<_, std::io::Error>(body)
    };
    let body = match tokio::time::timeout(Duration::from_secs(timeout_s + 5), recv_fut).await {
        Ok(Ok(b)) => b,
        Ok(Err(_)) => {
            unsafe { libc::kill(entry.pid as i32, libc::SIGKILL); }
            state.stats.kills_hard_timeout.fetch_add(1, Ordering::Relaxed);
            state.stats.bump_respawn("child_died");
            state.trigger_refill();
            return pb::ExecResult { stdout: String::new(), stderr: "child socket error".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()};
        }
        Err(_) => {
            unsafe { libc::kill(entry.pid as i32, libc::SIGKILL); }
            state.stats.kills_hard_timeout.fetch_add(1, Ordering::Relaxed);
            state.stats.bump_respawn("hard_timeout");
            state.trigger_refill();
            return pb::ExecResult { stdout: String::new(), stderr: "child hard timeout".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()};
        }
    };
    let std_stream = match stream.into_std() {
        Ok(s) => s,
        Err(_) => { unsafe { libc::kill(entry.pid as i32, libc::SIGKILL); } state.trigger_refill();
            return pb::ExecResult { stdout: String::new(), stderr: "into_std fail".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()}; }
    };
    let _ = std_stream.set_nonblocking(false);
    entry.fd = unsafe { OwnedFd::from_raw_fd(std_stream.into_raw_fd()) };

    let child_resp = match pb::ChildResponse::decode(&*body) {
        Ok(r) => r,
        Err(_) => {
            drop(entry.fd);
            unsafe { libc::kill(entry.pid as i32, libc::SIGKILL); }
            state.stats.bump_respawn("bad_pb");
            state.trigger_refill();
            return pb::ExecResult { stdout: String::new(), stderr: "child returned invalid pb".into(), exit_code: 1, elapsed_ms: 0, output_files: ::std::collections::HashMap::new(), deleted_files: vec![] , output_files_b64: Default::default()};
        }
    };

    state.stats.requests_total.fetch_add(1, Ordering::Relaxed);
    let lc = child_resp.lifecycle.unwrap_or_default();
    if child_resp.expire {
        let reason = if lc.expire_reason.is_empty() { "unknown".to_string() } else { lc.expire_reason };
        state.stats.bump_respawn(&reason);
        drop(entry.fd);
        state.trigger_refill();
    } else {
        entry.last_used = Instant::now();
        entry.requests_served = lc.requests_served;
        entry.last_rss_mb = lc.rss_mb;
        state.push_idle(entry);
    }
    pb::ExecResult {
        stdout: child_resp.stdout,
        stderr: child_resp.stderr,
        exit_code: child_resp.exit_code,
        elapsed_ms: start.elapsed().as_millis() as u64,
        output_files: child_resp.output_files,
        deleted_files: child_resp.deleted_files,
output_files_b64: child_resp.output_files_b64,
    }
}


fn send_with_fds(stream_fd: RawFd, payload: &[u8], fds: &[RawFd]) -> std::io::Result<()> {
    use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
    use std::io::IoSlice;
    let header = (payload.len() as u32).to_be_bytes();
    let framed: Vec<u8> = header.iter().chain(payload.iter()).copied().collect();
    let iov = [IoSlice::new(&framed)];
    let cmsgs = if fds.is_empty() {
        vec![]
    } else {
        vec![ControlMessage::ScmRights(fds)]
    };
    sendmsg::<()>(stream_fd, &iov, &cmsgs, MsgFlags::empty(), None)
        .map(|_| ())
        .map_err(|e| std::io::Error::other(format!("sendmsg: {}", e)))
}

// ---- connection handler -------------------------------------------------

async fn handle_connection(state: Arc<AppState>, mut stream: UnixStream) {
    loop {
        let frame = match recv_frame_async(&mut stream).await {
            Ok(b) => b,
            Err(_) => return,
        };
        let request: pb::Request = match pb::Request::decode(&*frame) {
            Ok(r) => r,
            Err(e) => {
                let resp = pb::Response { kind: "error".into(), error: format!("bad pb: {}", e), ..Default::default() };
                let _ = send_frame_async(&mut stream, &resp.encode_to_vec()).await;
                continue;
            }
        };
        // Hybrid path: "lease" and "refill" use SCM_RIGHTS to hand out child fds.
        if request.cmd == "lease" || request.cmd == "refill" {
            let count = if request.cmd == "refill" {
                1
            } else {
                std::cmp::max(1, request.lease_count as usize)
            };
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut acquired: Vec<IdleEntry> = Vec::with_capacity(count);
            for _ in 0..count {
                match acquire_child(&state, deadline).await {
                    Some(e) => acquired.push(e),
                    None => break,
                }
            }
            // Trigger refill BEFORE we hand fds out so forker has head start.
            for _ in 0..acquired.len() { state.trigger_refill(); }

            let fds: Vec<RawFd> = acquired.iter().map(|e| e.fd.as_raw_fd()).collect();
            let resp = pb::Response { kind: request.cmd.clone(), ..Default::default() };
            let resp_bytes = resp.encode_to_vec();
            let raw_fd = stream.as_raw_fd();
            if let Err(e) = send_with_fds(raw_fd, &resp_bytes, &fds) {
                eprintln!("[parent] send_with_fds failed: {}", e);
                return;
            }
            // After sendmsg, the kernel has dup'd fds into the receiver. Dropping the
            // OwnedFds here closes our side; the receiver still has live copies.
            drop(acquired);
            continue;
        }

        let resp: pb::Response = match request.cmd.as_str() {
            "health" => pb::Response { kind: "health".into(), health: Some(health_response_pb(&state)), ..Default::default() },
            "stats" => pb::Response { kind: "stats".into(), json: stats_response_json(&state), ..Default::default() },
            "drain" => {
                let d = request.drain.unwrap_or_default();
                pb::Response { kind: "drain".into(), json: drain_response_json(&state, d.pid, Some(d.reason)), ..Default::default() }
            }
            _ => {
                let job = request.job.unwrap_or_default();
                let exec = exec_with_child(state.clone(), job).await;
                pb::Response { kind: "exec".into(), exec: Some(exec), ..Default::default() }
            }
        };
        if send_frame_async(&mut stream, &resp.encode_to_vec()).await.is_err() {
            return;
        }
    }
}

// ---- main ---------------------------------------------------------------

fn main() -> Result<()> {
    let cfg = Config::from_env();
    eprintln!("[parent] booting socket={} pool_size={} prewarm={:?}",
        cfg.socket_path.display(), cfg.pool_size, cfg.prewarm);
    unsafe { libc::signal(libc::SIGCHLD, libc::SIG_IGN); }

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| -> PyResult<()> {
        let main_mod = py.import_bound("__main__")?;
        let globals: Bound<PyDict> = main_mod.dict();
        py.run_bound(SANDBOX_HELPER_PY, Some(&globals), None)?;
        for name in &cfg.prewarm {
            let t = Instant::now();
            match py.import_bound(name.as_str()) {
                Ok(_) => eprintln!("[parent] prewarm ok: {} ({:.0}ms)", name, t.elapsed().as_secs_f64()*1000.0),
                Err(e) => eprintln!("[parent] prewarm FAIL {}: {}", name, e),
            }
        }
        let gc = py.import_bound("gc")?;
        gc.call_method0("collect")?;
        gc.call_method0("freeze")?;
        eprintln!("[parent] gc.freeze() done");
        Ok(())
    })?;

    let idle: IdleQueue = Arc::new(Mutex::new(VecDeque::new()));
    let (fork_tx, fork_rx) = mpsc::channel::<()>();
    let stats = Arc::new(Stats::new());
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let idle_c = idle.clone();
        let stats_c = stats.clone();
        let shutdown_c = shutdown.clone();
        std::thread::Builder::new().name("forker".into()).spawn(move || {
            forker_thread(fork_rx, idle_c, stats_c, shutdown_c);
        })?;
    }
    for _ in 0..cfg.pool_size { let _ = fork_tx.send(()); }
    let want_initial = cfg.pool_size.min(4);
    let deadline = Instant::now() + Duration::from_secs(30);
    while idle.lock().unwrap().len() < want_initial && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    eprintln!("[parent] initial idle={}", idle.lock().unwrap().len());

    let state = Arc::new(AppState {
        idle, fork_tx: Mutex::new(fork_tx),
        pool_size: cfg.pool_size, max_timeout: cfg.max_timeout, prewarm: cfg.prewarm,
        stats, max_age: cfg.max_age, max_idle: cfg.max_idle,
        reaper_interval: cfg.reaper_interval, max_total_rss_mb: cfg.max_total_rss_mb,
        shutdown: shutdown.clone(),
    });
    {
        let state_c = state.clone();
        std::thread::Builder::new().name("reaper".into()).spawn(move || reaper_thread(state_c))?;
    }

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        if let Some(parent) = cfg.socket_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::remove_file(&cfg.socket_path);
        let listener = UnixListener::bind(&cfg.socket_path)?;
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&cfg.socket_path, std::fs::Permissions::from_mode(0o666));
        eprintln!("[parent] listening on {}", cfg.socket_path.display());
        loop {
            let (stream, _) = listener.accept().await?;
            let state_c = state.clone();
            tokio::spawn(handle_connection(state_c, stream));
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    })?;
    shutdown.store(true, Ordering::Relaxed);
    Ok(())
}
