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
//!   POST   /sessions/:id/tools/read    {path, offset?, limit?} → {content, bytes}
//!   POST   /sessions/:id/tools/write   {path, content}         → {bytes}
//!   POST   /sessions/:id/tools/bash    {command, timeout?}     → {stdout, stderr, exit_code}
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
    /// Top-level sandbox storage (TOOLS_SANDBOX_ROOT env). When chroot
    /// isolation is off, this is also where sessions live.
    sandbox_root: PathBuf,
    /// Parent dir of each session's cwd. Equals sandbox_root when chroot
    /// is off; equals sandbox_root/.rootfs/sessions when chroot is on.
    session_root: PathBuf,
    /// When Some, every tool_bash subprocess chroots into this dir
    /// and chdirs to /sessions/<sid>. session_root is then a child of
    /// chroot_root so the daemon-side filesystem operations and the
    /// chroot-side bash view see the same files (via the same inodes).
    chroot_root: Option<PathBuf>,
    sessions: DashMap<String, PathBuf>,
    isolation: IsolationCfg,
    seccomp_filter: Option<Arc<seccompiler::BpfProgram>>,
}

/// Which optional isolation layers to apply on bash subprocess spawn.
/// Default: all off (backwards-compat). Enable via TOOLS_ISOLATION env,
/// comma-separated. Recognised values: "chroot", "seccomp", "cgroup".
/// Layers that aren't supported by the host environment silently no-op
/// (e.g. cgroup v2 not mounted → cgroup flag still parsed but effective_cgroup
/// returns None at first use).
#[derive(Debug, Clone, Default)]
pub struct IsolationCfg {
    pub chroot: bool,
    pub seccomp: bool,
    pub cgroup: bool,
    /// Path to the cgroup v2 root, if the host exposes one we can write to.
    /// Cached at startup; if `cgroup` is enabled but this is None, every
    /// per-session attempt to create a sub-cgroup is a no-op.
    pub cgroup_root: Option<PathBuf>,
}

impl IsolationCfg {
    pub fn from_env() -> Self {
        let raw = std::env::var("TOOLS_ISOLATION").unwrap_or_default();
        let set: std::collections::HashSet<&str> = raw
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let cgroup = set.contains("cgroup");
        let cgroup_root = if cgroup { detect_cgroup_v2() } else { None };
        Self {
            chroot: set.contains("chroot"),
            seccomp: set.contains("seccomp"),
            cgroup,
            cgroup_root,
        }
    }
    /// True if any layer is actually active (declared AND supported).
    #[allow(dead_code)]
    pub fn any_active(&self) -> bool {
        self.chroot || self.seccomp || self.cgroup_root.is_some()
    }
}

/// Detect a writable cgroup v2 hierarchy. Returns Some(root) iff:
/// 1. `/sys/fs/cgroup/cgroup.controllers` exists (so we are on cgroup v2)
/// 2. We can mkdir a probe directory under it (so delegation gives us write)
///
/// Otherwise None — caller skips cgroup ops.
fn detect_cgroup_v2() -> Option<PathBuf> {
    let root = PathBuf::from("/sys/fs/cgroup");
    if !root.join("cgroup.controllers").exists() {
        return None;
    }
    let probe = root.join(format!(".mindbox-probe-{}", std::process::id()));
    match std::fs::create_dir(&probe) {
        Ok(_) => {
            let _ = std::fs::remove_dir(&probe);
            Some(root)
        }
        Err(_) => None,
    }
}

/// Create a sub-cgroup for the session and apply default limits. Returns
/// the cgroup directory path if successful; None when cgroup layer is off
/// or write fails (so the caller can skip the join step gracefully).
fn create_session_cgroup(isolation: &IsolationCfg, sid: &str) -> Option<PathBuf> {
    let root = isolation.cgroup_root.as_ref()?;
    let dir = root.join(format!("mindbox-{}", sid));
    if std::fs::create_dir(&dir).is_err() {
        return None;
    }
    // Conservative defaults — env overrides could be added later.
    let _ = std::fs::write(
        dir.join("memory.max"),
        b"512M
",
    );
    let _ = std::fs::write(
        dir.join("cpu.max"),
        b"50000 100000
",
    ); // 50% of 1 core
    let _ = std::fs::write(
        dir.join("pids.max"),
        b"256
",
    );
    Some(dir)
}

/// Add a process to its session's cgroup. Best-effort: silently ignores
/// failure (the session cwd still exists; bash will just run uncgrouped).
fn cgroup_attach_pid(cgroup_dir: &std::path::Path, pid: u32) {
    let _ = std::fs::write(
        cgroup_dir.join("cgroup.procs"),
        format!(
            "{}
",
            pid
        ),
    );
}

/// Best-effort cgroup teardown. Kills any remaining processes (kill_pids)
/// then rmdirs. Failure is silent (process might already be gone, kernel
/// keeps the empty cgroup until last ref drops).
fn cleanup_session_cgroup(cgroup_dir: &std::path::Path) {
    let _ = std::fs::write(
        cgroup_dir.join("cgroup.kill"),
        b"1
",
    );
    let _ = std::fs::remove_dir(cgroup_dir);
}

/// Build a seccomp filter for bash subprocesses: default-allow with an
/// explicit denylist of privilege-escalation / namespace / out-of-band-IO
/// syscalls. A default-deny allowlist is safer but too brittle for general
/// bash workloads — any uncommon syscall would break random user code.
fn build_seccomp_filter() -> anyhow::Result<seccompiler::BpfProgram> {
    use seccompiler::{SeccompAction, SeccompFilter, TargetArch};
    let denied: &[i64] = &[
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_setuid,
        libc::SYS_setgid,
        libc::SYS_setresuid,
        libc::SYS_setresgid,
        libc::SYS_setreuid,
        libc::SYS_setregid,
        libc::SYS_capset,
        libc::SYS_ptrace,
        libc::SYS_personality,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_reboot,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_kexec_load,
        libc::SYS_chroot,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_unshare,
    ];
    let mut rules: std::collections::BTreeMap<i64, Vec<seccompiler::SeccompRule>> =
        std::collections::BTreeMap::new();
    for syscall in denied {
        rules.insert(*syscall, vec![]); // no arg-match → all calls of this syscall
    }
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::Allow,                     // default = allow
        SeccompAction::Errno(libc::EPERM as u32), // denylist returns -EPERM
        TargetArch::x86_64,
    )?;
    Ok(filter.try_into()?)
}

/// Apply PR_SET_NO_NEW_PRIVS and the prebuilt seccomp filter to the
/// current process. Called from inside a tokio::process::Command pre_exec
/// hook (post-fork, pre-exec).
///
/// # Safety
/// Caller must invoke only between fork and exec. seccompiler's apply_filter
/// and prctl on the post-fork path are async-signal-safe.
unsafe fn apply_bash_isolation(filter: &seccompiler::BpfProgram) -> std::io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    seccompiler::apply_filter(filter)
        .map_err(|e| std::io::Error::other(format!("apply_filter: {e}")))?;
    Ok(())
}

/// Set of binaries copied into the shared chroot rootfs. The full ldd
/// transitive closure of each binary gets pulled in too, so bash + the
/// listed coreutils have their shared libraries available post-chroot.
const ROOTFS_BINARIES: &[&str] = &[
    "/bin/bash",
    "/bin/sh",
    "/bin/ls",
    "/bin/cat",
    "/bin/cp",
    "/bin/mv",
    "/bin/rm",
    "/bin/mkdir",
    "/bin/echo",
    "/bin/grep",
    "/bin/sed",
    "/bin/awk",
    "/bin/sort",
    "/bin/head",
    "/bin/tail",
    "/bin/wc",
    "/usr/bin/python3",
    "/usr/bin/node",
    "/usr/bin/git",
    "/usr/bin/rg",
    "/usr/bin/fd",
    "/usr/bin/fdfind",
    "/usr/bin/uname",
    "/usr/bin/which",
    "/usr/bin/env",
    "/usr/bin/find",
    "/usr/bin/diff",
];

/// Prepare a shared sub-rootfs at `rootfs` so bash subprocesses can chroot
/// into it. Idempotent: skips if `rootfs/bin/bash` already exists. Pulls
/// in each binary in ROOTFS_BINARIES (best-effort: missing ones are fine)
/// plus their ldd-resolved shared libraries plus a handful of /etc files
/// bash + glibc commonly read.
///
/// Cost: typically 30-100 MB of file copies + ldd subprocess invocations.
/// Done once at daemon startup, not per session.
fn prepare_shared_rootfs(rootfs: &std::path::Path) -> std::io::Result<()> {
    use std::process::Command as StdCommand;
    if rootfs.join("bin/bash").exists() {
        return Ok(());
    }
    std::fs::create_dir_all(rootfs)?;
    for dir in [
        "bin",
        "usr/bin",
        "lib",
        "lib/x86_64-linux-gnu",
        "lib64",
        "etc",
        "sessions",
        "tmp",
    ] {
        std::fs::create_dir_all(rootfs.join(dir))?;
    }
    // /tmp is writable for bash; everyone read+write+sticky.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(rootfs.join("tmp"), std::fs::Permissions::from_mode(0o1777))?;

    let mut copied_libs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let copy_one = |src: &std::path::Path, dst: &std::path::Path| -> std::io::Result<()> {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src, dst)?;
        Ok(())
    };

    for bin in ROOTFS_BINARIES {
        let src = std::path::Path::new(bin);
        if !src.exists() {
            continue; // template-specific binaries may not be on every host
        }
        let dst = rootfs.join(bin.trim_start_matches('/'));
        if let Err(e) = copy_one(src, &dst) {
            tracing::warn!("chroot rootfs: copy {} failed: {}", bin, e);
            continue;
        }
        // ldd to find transitive shared lib deps. Best-effort: if ldd
        // fails or the binary is statically linked we just skip libs for it.
        if let Ok(out) = StdCommand::new("ldd").arg(bin).output() {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                for tok in line.split_whitespace() {
                    if !tok.starts_with('/') {
                        continue;
                    }
                    if !copied_libs.insert(tok.to_string()) {
                        continue;
                    }
                    let lib_src = std::path::Path::new(tok);
                    if !lib_src.is_file() {
                        continue;
                    }
                    let lib_dst = rootfs.join(tok.trim_start_matches('/'));
                    let _ = copy_one(lib_src, &lib_dst);
                }
            }
        }
    }

    // glibc resolver helpers + nsswitch + minimal /etc files bash touches.
    for f in [
        "/etc/passwd",
        "/etc/group",
        "/etc/nsswitch.conf",
        "/etc/resolv.conf",
        "/etc/hosts",
        "/etc/ld.so.cache",
    ] {
        let src = std::path::Path::new(f);
        if src.exists() {
            let _ = copy_one(src, &rootfs.join(f.trim_start_matches('/')));
        }
    }
    tracing::info!(
        "prepared chroot rootfs at {} ({} unique libs)",
        rootfs.display(),
        copied_libs.len()
    );
    Ok(())
}

/// Apply chroot then chdir into the in-chroot session path. Called from
/// the bash subprocess pre_exec hook in addition to (or in place of) the
/// seccomp variant. Uses raw libc::chroot + libc::chdir for async-signal-
/// safety (no allocations after fork).
///
/// # Safety
/// Must run between fork and exec. cstr must be a null-terminated C string.
unsafe fn apply_chroot(
    rootfs: *const libc::c_char,
    cwd_in_chroot: *const libc::c_char,
) -> std::io::Result<()> {
    if unsafe { libc::chroot(rootfs) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::chdir(cwd_in_chroot) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[derive(Serialize)]
struct CreateSessionResp {
    session_id: String,
    cwd: String,
}

#[derive(Deserialize)]
struct ReadReq {
    path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
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
    #[serde(alias = "cmd")]
    command: String,
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

#[derive(Clone, Deserialize)]
struct EditReplacement {
    #[serde(rename = "oldText")]
    old_text: String,
    #[serde(rename = "newText")]
    new_text: String,
}

#[derive(Deserialize)]
struct EditReq {
    path: String,
    #[serde(default)]
    edits: Vec<EditReplacement>,
    #[serde(default)]
    old_string: Option<String>,
    #[serde(default)]
    new_string: Option<String>,
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
    #[serde(default)]
    limit: Option<usize>,
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
    #[serde(default)]
    glob: Option<String>,
    #[serde(default, rename = "ignoreCase")]
    ignore_case: bool,
    #[serde(default)]
    literal: bool,
    #[serde(default)]
    context: usize,
    #[serde(default = "default_grep_limit")]
    limit: usize,
    /// Legacy: "content" (default), "files_with_matches", or "count".
    #[serde(default = "default_grep_mode")]
    output_mode: String,
    /// Legacy safety cap for walked files.
    #[serde(default = "default_grep_max_files")]
    max_files: usize,
}

fn default_grep_path() -> String {
    ".".into()
}
fn default_grep_mode() -> String {
    "content".into()
}
fn default_grep_limit() -> usize {
    100
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
    #[serde(default = "default_find_limit", alias = "max_results")]
    limit: usize,
}

fn default_find_path() -> String {
    ".".into()
}
fn default_find_limit() -> usize {
    1000
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

fn slice_lines(
    content: &str,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<String, (StatusCode, String)> {
    let start = offset.unwrap_or(1);
    if start == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "offset is 1-indexed and must be greater than 0".to_string(),
        ));
    }
    let Some(limit) = limit else {
        return Ok(content
            .split_inclusive('\n')
            .skip(start - 1)
            .collect::<String>());
    };
    if limit == 0 {
        return Ok(String::new());
    }
    Ok(content
        .split_inclusive('\n')
        .skip(start - 1)
        .take(limit)
        .collect::<String>())
}

fn apply_edit_req(
    src: &str,
    path: &str,
    req: &EditReq,
) -> Result<(String, usize), (StatusCode, String)> {
    let has_legacy = req.old_string.is_some() || req.new_string.is_some();
    if has_legacy && !req.edits.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "use either edits[] or legacy old_string/new_string, not both".to_string(),
        ));
    }
    if has_legacy {
        let old = req.old_string.as_ref().ok_or((
            StatusCode::BAD_REQUEST,
            "legacy edit requires old_string".to_string(),
        ))?;
        let new = req.new_string.as_ref().ok_or((
            StatusCode::BAD_REQUEST,
            "legacy edit requires new_string".to_string(),
        ))?;
        if old.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "old_string must not be empty".to_string(),
            ));
        }
        let n = src.matches(old).count();
        if n == 0 {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("old_string not found in {path}"),
            ));
        }
        if n > 1 && !req.replace_all {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "old_string matches {n} times; set replace_all=true to confirm bulk replace"
                ),
            ));
        }
        let replaced = if req.replace_all {
            src.replace(old, new)
        } else {
            src.replacen(old, new, 1)
        };
        return Ok((replaced, n));
    }

    if req.edits.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "edit requires at least one edits[] replacement".to_string(),
        ));
    }

    let mut ranges = Vec::with_capacity(req.edits.len());
    for edit in &req.edits {
        if edit.old_text.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "oldText must not be empty".to_string(),
            ));
        }
        let found: Vec<_> = src.match_indices(&edit.old_text).collect();
        if found.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("oldText not found in {path}"),
            ));
        }
        if found.len() > 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("oldText matches {} times in {path}", found.len()),
            ));
        }
        let start = found[0].0;
        ranges.push((start, start + edit.old_text.len(), edit.new_text.clone()));
    }

    ranges.sort_by_key(|(start, _, _)| *start);
    for pair in ranges.windows(2) {
        if pair[1].0 < pair[0].1 {
            return Err((
                StatusCode::BAD_REQUEST,
                "edits[] replacements must not overlap".to_string(),
            ));
        }
    }

    let mut out = String::with_capacity(src.len());
    let mut cursor = 0usize;
    for (start, end, new_text) in ranges {
        out.push_str(&src[cursor..start]);
        out.push_str(&new_text);
        cursor = end;
    }
    out.push_str(&src[cursor..]);
    Ok((out, req.edits.len()))
}

// ---------------------------------------------------------------------------

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CreateSessionResp>, (StatusCode, String)> {
    let id = Uuid::new_v4().simple().to_string();
    let cwd = state.session_root.join(&id);
    tokio::fs::create_dir_all(&cwd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")))?;
    let _ = create_session_cgroup(&state.isolation, &id);
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
    if let Some(root) = state.isolation.cgroup_root.as_ref() {
        cleanup_session_cgroup(&root.join(format!("mindbox-{}", id)));
    }
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
    let content = String::from_utf8_lossy(&bytes).into_owned();
    if req.offset.is_none() && req.limit.is_none() {
        return Ok(Json(ReadResp {
            content,
            bytes: bytes.len(),
        }));
    }
    let content = slice_lines(&content, req.offset, req.limit)?;
    Ok(Json(ReadResp {
        bytes: content.len(),
        content,
    }))
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
    if let Some(limit) = req.limit {
        entries.truncate(limit);
    }
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
    let (replaced, replacements) = apply_edit_req(&src, &req.path, &req)?;
    tokio::fs::write(&target, replaced)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;
    Ok(Json(EditResp { replacements }))
}

async fn tool_grep(
    State(state): State<Arc<AppState>>,
    Path(sid): Path<String>,
    Json(req): Json<GrepReq>,
) -> Result<Json<GrepResp>, (StatusCode, String)> {
    let cwd = resolve_session(&state, &sid)?.clone();
    let target = resolve_in(&cwd, &req.path)?;

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

    let grep = GrepSearchReq {
        pattern: req.pattern.clone(),
        glob: req.glob.clone(),
        ignore_case: req.ignore_case,
        literal: req.literal,
        context: req.context,
        limit: req.limit,
        max_files: req.max_files,
        want_content,
        want_files,
        want_count,
    };

    let cwd_clone = cwd.clone();
    let result = tokio::task::spawn_blocking(move || run_ripgrep_search(&target, &cwd_clone, grep))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")))??;

    Ok(Json(GrepResp {
        mode: req.output_mode,
        matches: result.matches,
        files: result.files,
        counts: result.counts,
        walked: result.walked,
        truncated: result.truncated,
    }))
}

struct GrepSearchReq {
    pattern: String,
    glob: Option<String>,
    ignore_case: bool,
    literal: bool,
    context: usize,
    limit: usize,
    max_files: usize,
    want_content: bool,
    want_files: bool,
    want_count: bool,
}

struct RipgrepResult {
    matches: Vec<GrepMatch>,
    files: Vec<String>,
    counts: Vec<(String, u64)>,
    walked: usize,
    truncated: bool,
}

fn run_ripgrep_search(
    target: &std::path::Path,
    cwd: &std::path::Path,
    req: GrepSearchReq,
) -> Result<RipgrepResult, (StatusCode, String)> {
    use grep::matcher::Matcher;
    use grep::regex::RegexMatcherBuilder;
    use std::collections::BTreeSet;

    let mut builder = RegexMatcherBuilder::new();
    builder
        .case_insensitive(req.ignore_case)
        .fixed_strings(req.literal);
    let matcher = builder
        .build(&req.pattern)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad regex: {e}")))?;
    let glob = match req.glob.as_deref() {
        Some(pattern) => Some(
            globset::Glob::new(pattern)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad glob: {e}")))?
                .compile_matcher(),
        ),
        None => None,
    };

    let mut walked = 0usize;
    let mut truncated = false;
    let mut matches = Vec::<GrepMatch>::new();
    let mut files = Vec::<String>::new();
    let mut counts = Vec::<(String, u64)>::new();
    let mut emitted_matches = 0usize;

    let walker = ignore::WalkBuilder::new(target)
        .follow_links(false)
        .standard_filters(false)
        .build();

    for ent in walker.flatten() {
        if !ent.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        walked += 1;
        if walked > req.max_files {
            truncated = true;
            break;
        }
        let path = ent.path();
        let rel = match path.strip_prefix(cwd) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => path.to_string_lossy().into_owned(),
        };
        if glob.as_ref().is_some_and(|g| !g.is_match(&rel)) {
            continue;
        }

        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if bytes.contains(&b'\0') {
            continue;
        }
        let text = String::from_utf8_lossy(&bytes);
        let lines: Vec<&str> = text.split_inclusive('\n').collect();
        let mut matched_lines = Vec::new();
        let mut per_file = 0u64;

        for (idx, line) in lines.iter().enumerate() {
            if !matcher.is_match(line.as_bytes()).unwrap_or(false) {
                continue;
            }
            per_file += 1;
            if req.want_content {
                if emitted_matches >= req.limit {
                    truncated = true;
                    break;
                }
                matched_lines.push(idx);
                emitted_matches += 1;
            }
        }

        if per_file > 0 {
            if req.want_content {
                let mut selected = BTreeSet::new();
                for idx in matched_lines {
                    let start = idx.saturating_sub(req.context);
                    let end = idx
                        .saturating_add(req.context)
                        .min(lines.len().saturating_sub(1));
                    for line_idx in start..=end {
                        selected.insert(line_idx);
                    }
                }
                for line_idx in selected {
                    matches.push(GrepMatch {
                        path: rel.clone(),
                        line: (line_idx + 1) as u64,
                        text: lines[line_idx]
                            .trim_end_matches('\n')
                            .trim_end_matches('\r')
                            .to_string(),
                    });
                }
            } else if req.want_files {
                if files.len() >= req.limit {
                    truncated = true;
                    break;
                }
                files.push(rel.clone());
            } else if req.want_count {
                if counts.len() >= req.limit {
                    truncated = true;
                    break;
                }
                counts.push((rel.clone(), per_file));
            }
        }

        if truncated && req.want_content {
            break;
        }
    }
    Ok(RipgrepResult {
        matches,
        files,
        counts,
        walked,
        truncated,
    })
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
    // ignore::WalkBuilder is what sharkdp/fd uses for the directory walk:
    // parallel-traversal-capable, gitignore-aware (we disable that with
    // standard_filters(false) so build artifacts stay visible — agents
    // often need to grep/find them).
    let walker = ignore::WalkBuilder::new(&target)
        .follow_links(false)
        .standard_filters(false)
        .build();
    for ent in walker.flatten() {
        walked += 1;
        // Same 4× over-walk cap as before — wildcard patterns like `**`
        // on deep trees would otherwise scan unbounded.
        if walked > req.limit.saturating_mul(4).max(20_000) {
            truncated = true;
            break;
        }
        let rel = match ent.path().strip_prefix(&cwd) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => continue,
        };
        if glob.is_match(&rel) {
            paths.push(rel);
            if paths.len() >= req.limit {
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
    cmd.arg("-c").arg(&req.command);
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    // chroot path: don't set current_dir() — pre_exec chroots first and
    // then chdirs to the in-chroot session path. Without chroot, set
    // current_dir() to the daemon-side cwd as before.
    let chroot_data: Option<(std::ffi::CString, std::ffi::CString)> =
        match state.chroot_root.as_ref() {
            Some(rootfs) => {
                let rootfs_c = std::ffi::CString::new(rootfs.to_string_lossy().as_bytes())
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cstr: {e}")))?;
                let cwd_in_chroot = std::ffi::CString::new(format!("/sessions/{}", sid))
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cstr: {e}")))?;
                Some((rootfs_c, cwd_in_chroot))
            }
            None => {
                cmd.current_dir(&cwd);
                None
            }
        };
    let filter = state.seccomp_filter.clone();
    if chroot_data.is_some() || filter.is_some() {
        // SAFETY: pre_exec runs post-fork, pre-execve. The operations we
        // perform — libc::chroot, libc::chdir, prctl, seccomp filter
        // install — are all documented async-signal-safe.
        unsafe {
            cmd.pre_exec(move || {
                if let Some((rootfs_c, cwd_c)) = &chroot_data {
                    apply_chroot(rootfs_c.as_ptr(), cwd_c.as_ptr())?;
                }
                if let Some(f) = &filter {
                    apply_bash_isolation(f)?;
                }
                Ok(())
            });
        }
    }
    let timeout = Duration::from_secs(req.timeout.clamp(1, 300));
    let cgroup_dir = state
        .isolation
        .cgroup_root
        .as_ref()
        .map(|root| root.join(format!("mindbox-{}", sid)));

    let child = cmd
        .spawn()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")))?;
    if let (Some(dir), Some(pid)) = (cgroup_dir.as_ref(), child.id()) {
        cgroup_attach_pid(dir, pid);
    }
    let (stdout, stderr, exit_code, timed_out) =
        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(o)) => (
                String::from_utf8_lossy(&o.stdout).into_owned(),
                String::from_utf8_lossy(&o.stderr).into_owned(),
                o.status.code().unwrap_or(-1),
                false,
            ),
            Ok(Err(e)) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("wait: {e}"))),
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
    let isolation = IsolationCfg::from_env();
    tracing::info!(
        "isolation: chroot={} seccomp={} cgroup={} (cgroup_root={:?})",
        isolation.chroot,
        isolation.seccomp,
        isolation.cgroup,
        isolation.cgroup_root,
    );
    let seccomp_filter = if isolation.seccomp {
        match build_seccomp_filter() {
            Ok(f) => {
                tracing::info!("seccomp filter built ({} BPF insns)", f.len());
                Some(Arc::new(f))
            }
            Err(e) => {
                tracing::warn!("seccomp requested but build failed: {}", e);
                None
            }
        }
    } else {
        None
    };
    // Choose session_root + (optionally) prepare a shared chroot rootfs.
    // If chroot prep fails we degrade to no-chroot rather than refusing
    // to start; the daemon log warns clearly.
    let (session_root, chroot_root) = if isolation.chroot {
        let rootfs = sandbox_root.join(".rootfs");
        match prepare_shared_rootfs(&rootfs) {
            Ok(()) => {
                let sr = rootfs.join("sessions");
                std::fs::create_dir_all(&sr)?;
                (sr, Some(rootfs))
            }
            Err(e) => {
                tracing::warn!(
                    "chroot requested but prepare_shared_rootfs failed: {}; running without chroot",
                    e
                );
                (sandbox_root.clone(), None)
            }
        }
    } else {
        (sandbox_root.clone(), None)
    };
    Ok(Arc::new(AppState {
        sandbox_root,
        session_root,
        chroot_root,
        sessions: DashMap::new(),
        isolation,
        seccomp_filter,
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
            session_root: td.path().to_path_buf(),
            chroot_root: None,
            sessions: DashMap::new(),
            isolation: IsolationCfg::default(),
            seccomp_filter: None,
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

    #[test]
    fn bash_request_accepts_canonical_and_legacy_command() {
        let canonical: BashReq = serde_json::from_value(serde_json::json!({
            "command": "echo hi"
        }))
        .unwrap();
        assert_eq!(canonical.command, "echo hi");
        assert_eq!(canonical.timeout, 30);

        let legacy: BashReq = serde_json::from_value(serde_json::json!({
            "cmd": "echo old",
            "timeout": 7
        }))
        .unwrap();
        assert_eq!(legacy.command, "echo old");
        assert_eq!(legacy.timeout, 7);
    }

    #[test]
    fn edit_request_accepts_canonical_and_legacy_shapes() {
        let canonical: EditReq = serde_json::from_value(serde_json::json!({
            "path": "f.txt",
            "edits": [{ "oldText": "a", "newText": "b" }]
        }))
        .unwrap();
        assert_eq!(canonical.edits.len(), 1);
        assert_eq!(canonical.edits[0].old_text, "a");
        assert!(canonical.old_string.is_none());

        let legacy: EditReq = serde_json::from_value(serde_json::json!({
            "path": "f.txt",
            "old_string": "a",
            "new_string": "b",
            "replace_all": true
        }))
        .unwrap();
        assert_eq!(legacy.old_string.as_deref(), Some("a"));
        assert_eq!(legacy.new_string.as_deref(), Some("b"));
        assert!(legacy.replace_all);
    }

    #[test]
    fn find_request_accepts_limit_and_legacy_max_results() {
        let canonical: FindReq = serde_json::from_value(serde_json::json!({
            "pattern": "*.rs",
            "limit": 11
        }))
        .unwrap();
        assert_eq!(canonical.path, ".");
        assert_eq!(canonical.limit, 11);

        let legacy: FindReq = serde_json::from_value(serde_json::json!({
            "pattern": "*.rs",
            "max_results": 12
        }))
        .unwrap();
        assert_eq!(legacy.limit, 12);
    }

    #[test]
    fn grep_request_accepts_camel_case_ignore_case() {
        let req: GrepReq = serde_json::from_value(serde_json::json!({
            "pattern": "needle",
            "ignoreCase": true,
            "literal": true,
            "limit": 9
        }))
        .unwrap();
        assert!(req.ignore_case);
        assert!(req.literal);
        assert_eq!(req.limit, 9);
        assert_eq!(req.output_mode, "content");
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
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(read.0.content, "hi there");
        assert_eq!(read.0.bytes, 8);
    }

    #[tokio::test]
    async fn read_supports_offset_and_limit() {
        let (_td, state) = tmp_state();
        let sid = "read-slice-sid".to_string();
        let cwd = state.sandbox_root.join(&sid);
        tokio::fs::create_dir_all(&cwd).await.unwrap();
        tokio::fs::write(cwd.join("lines.txt"), "one\ntwo\nthree\nfour\n")
            .await
            .unwrap();
        state.sessions.insert(sid.clone(), cwd);

        let read = tool_read(
            State(state.clone()),
            Path(sid.clone()),
            Json(ReadReq {
                path: "lines.txt".into(),
                offset: Some(2),
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        assert_eq!(read.0.content, "two\nthree\n");
        assert_eq!(read.0.bytes, "two\nthree\n".len());

        let empty = tool_read(
            State(state),
            Path(sid),
            Json(ReadReq {
                path: "lines.txt".into(),
                offset: Some(99),
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        assert_eq!(empty.0.content, "");
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
                command: "ls".into(),
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
                command: "sleep 5".into(),
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

    #[test]
    fn isolation_from_env_unset() {
        std::env::remove_var("TOOLS_ISOLATION");
        let c = IsolationCfg::from_env();
        assert!(!c.chroot && !c.seccomp && !c.cgroup);
    }

    #[test]
    fn isolation_from_env_chroot_seccomp() {
        std::env::set_var("TOOLS_ISOLATION", "chroot,seccomp");
        let c = IsolationCfg::from_env();
        assert!(c.chroot && c.seccomp);
        assert!(!c.cgroup);
        std::env::remove_var("TOOLS_ISOLATION");
    }

    #[test]
    fn isolation_from_env_handles_whitespace_and_empty() {
        std::env::set_var("TOOLS_ISOLATION", "  chroot  , , seccomp ,  ");
        let c = IsolationCfg::from_env();
        assert!(c.chroot && c.seccomp);
        std::env::remove_var("TOOLS_ISOLATION");
    }

    #[test]
    fn detect_cgroup_v2_no_panic() {
        // We can't pretend to be on a host with cgroup v2 in unit tests;
        // exercise the function and assert it doesn't panic.
        let _ = detect_cgroup_v2();
    }

    #[test]
    fn build_seccomp_filter_succeeds() {
        let f = build_seccomp_filter().expect("seccomp filter should build");
        assert!(
            f.len() > 4,
            "filter is suspiciously small: {} insns",
            f.len()
        );
    }

    #[test]
    fn prepare_shared_rootfs_idempotent_and_creates_bash() {
        let td = tempfile::tempdir().unwrap();
        let rootfs = td.path().join("rootfs");
        prepare_shared_rootfs(&rootfs).expect("rootfs prep should succeed");
        if std::path::Path::new("/bin/bash").exists() {
            assert!(rootfs.join("bin/bash").exists());
        }
        // Idempotent — second call early-returns Ok.
        prepare_shared_rootfs(&rootfs).expect("rootfs prep should be idempotent");
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
        let resp = tool_ls(
            State(state),
            Path(sid),
            Json(LsReq {
                path: ".".into(),
                limit: None,
            }),
        )
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
    async fn ls_respects_limit_after_sorting() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "ls-limit-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("b.txt"), "").await.unwrap();
        tokio::fs::write(cwd.join("a.txt"), "").await.unwrap();
        let resp = tool_ls(
            State(state),
            Path(sid),
            Json(LsReq {
                path: ".".into(),
                limit: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.entries.len(), 1);
        assert_eq!(resp.0.entries[0].name, "a.txt");
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
                edits: vec![EditReplacement {
                    old_text: "beta".into(),
                    new_text: "BETA".into(),
                }],
                old_string: None,
                new_string: None,
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
    async fn edit_applies_multiple_replacements_against_original() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "edit-multi-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("f.txt"), "alpha beta gamma delta")
            .await
            .unwrap();
        let resp = tool_edit(
            State(state.clone()),
            Path(sid),
            Json(EditReq {
                path: "f.txt".into(),
                edits: vec![
                    EditReplacement {
                        old_text: "beta".into(),
                        new_text: "BETA".into(),
                    },
                    EditReplacement {
                        old_text: "delta".into(),
                        new_text: "DELTA".into(),
                    },
                ],
                old_string: None,
                new_string: None,
                replace_all: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.replacements, 2);
        let body = tokio::fs::read_to_string(cwd.join("f.txt")).await.unwrap();
        assert_eq!(body, "alpha BETA gamma DELTA");
    }

    #[tokio::test]
    async fn edit_rejects_overlapping_canonical_replacements() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "edit-overlap-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::write(cwd.join("f.txt"), "abcdef").await.unwrap();
        let err = tool_edit(
            State(state),
            Path(sid),
            Json(EditReq {
                path: "f.txt".into(),
                edits: vec![
                    EditReplacement {
                        old_text: "abc".into(),
                        new_text: "X".into(),
                    },
                    EditReplacement {
                        old_text: "bcd".into(),
                        new_text: "Y".into(),
                    },
                ],
                old_string: None,
                new_string: None,
                replace_all: false,
            }),
        )
        .await;
        assert!(matches!(err, Err((StatusCode::BAD_REQUEST, _))));
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
                edits: Vec::new(),
                old_string: Some("ab".into()),
                new_string: Some("Z".into()),
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
                edits: Vec::new(),
                old_string: Some("ab".into()),
                new_string: Some("Z".into()),
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
                glob: None,
                ignore_case: false,
                literal: false,
                context: 0,
                limit: 100,
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
    async fn grep_supports_canonical_filters_and_context() {
        let (_td, state) = tmp_state();
        let sid = make_sid(&state, "grep-canon-sid").await;
        let cwd = state.sessions.get(&sid).unwrap().clone();
        tokio::fs::create_dir_all(cwd.join("src")).await.unwrap();
        tokio::fs::write(
            cwd.join("src/a.txt"),
            "before\nHello.*\nafter\nlater\nHello.* second\n",
        )
        .await
        .unwrap();
        tokio::fs::write(cwd.join("src/b.md"), "Hello.*\n")
            .await
            .unwrap();
        let resp = tool_grep(
            State(state),
            Path(sid),
            Json(GrepReq {
                pattern: "hello.*".into(),
                path: ".".into(),
                glob: Some("**/*.txt".into()),
                ignore_case: true,
                literal: true,
                context: 1,
                limit: 1,
                output_mode: "content".into(),
                max_files: 100,
            }),
        )
        .await
        .unwrap();
        let lines: Vec<_> = resp
            .0
            .matches
            .iter()
            .map(|m| (m.path.as_str(), m.line, m.text.as_str()))
            .collect();
        assert_eq!(
            lines,
            vec![
                ("src/a.txt", 1, "before"),
                ("src/a.txt", 2, "Hello.*"),
                ("src/a.txt", 3, "after"),
            ]
        );
        assert!(resp.0.truncated);
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
                glob: None,
                ignore_case: false,
                literal: false,
                context: 0,
                limit: 100,
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
                limit: 100,
            }),
        )
        .await
        .unwrap();
        let mut paths = resp.0.paths;
        paths.sort();
        assert_eq!(paths, vec!["a.rs".to_string(), "sub/b.rs".to_string()]);
    }

    #[test]
    fn find_legacy_max_results_deserializes_to_limit() {
        let req: FindReq = serde_json::from_value(serde_json::json!({
            "pattern": "*.txt",
            "max_results": 3
        }))
        .unwrap();
        assert_eq!(req.limit, 3);
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
                limit: 3,
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp.0.paths.len(), 3);
        assert!(resp.0.truncated);
    }
}
