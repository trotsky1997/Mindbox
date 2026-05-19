// api-rust v0.3: protobuf over unix socket between api and worker containers.

mod tools_forward;

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json as AxumJson},
    routing::{get, post},
    Json, Router,
};
use bollard::container::{
    CreateContainerOptions, ListContainersOptions, RemoveContainerOptions, StartContainerOptions,
};
use bollard::models::HostConfig;
use bollard::Docker;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Semaphore;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/inspect.rs"));
}

use axum::extract::Path as AxumPath;

const MAX_FRAME: usize = 64 * 1024 * 1024;
/// Returns the per-template socket parent directory. Configured via
/// INSPECT_API_SOCKETS_DIR; defaults to /opt/inspect-api/sockets. The
/// path MUST be visible to the docker daemon (in dev-container deployments
/// where the mindbox container runs against a path-rewriting docker proxy,
/// pick a path the proxy can resolve — e.g. /tmp/inspect-api/sockets).
fn socket_root() -> &'static std::path::Path {
    use std::sync::LazyLock;
    static ROOT: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
        std::path::PathBuf::from(
            std::env::var("INSPECT_API_SOCKETS_DIR")
                .unwrap_or_else(|_| "/opt/inspect-api/sockets".into()),
        )
    });
    &ROOT
}

// ---- config types --------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
struct TemplateConfig {
    name: String,
    #[serde(default = "default_base_image")]
    #[allow(dead_code)]
    base_image: String,
    #[serde(default)]
    prewarm: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    extra_pip: Vec<String>,
    #[serde(default = "default_pool_size")]
    pool_size: usize,
    #[serde(default = "default_containers")]
    containers: usize,
    #[serde(default = "default_memory_reservation")]
    memory_reservation: String,
    #[serde(default = "default_pids_limit")]
    pids_limit: i64,
    #[serde(default = "default_engine")]
    #[allow(dead_code)]
    engine: String,
}
fn default_base_image() -> String {
    "python:3.12-slim".into()
}
fn default_pool_size() -> usize {
    32
}
fn default_containers() -> usize {
    1
}
fn default_memory_reservation() -> String {
    "4g".into()
}
fn default_pids_limit() -> i64 {
    4096
}
fn default_engine() -> String {
    "rust".into()
}

#[derive(Deserialize)]
struct HotExecRequest {
    code: String,
    #[serde(default = "default_template")]
    template: String,
    #[serde(default = "default_timeout")]
    timeout: u32,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    files: HashMap<String, String>,
    #[serde(default)]
    persist_changes: bool,
    #[serde(default)]
    persist_root_label: String,
}
fn default_template() -> String {
    "default".into()
}
fn default_timeout() -> u32 {
    10
}

#[derive(Serialize)]
struct ExecResponse {
    stdout: String,
    stderr: String,
    exit_code: i32,
    elapsed_ms: u64,
    container_id: String,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    output_files: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    deleted_files: Vec<String>,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    output_files_b64: HashMap<String, String>,
}

// ---- runtime state ------------------------------------------------------

struct TemplateRuntime {
    cfg: TemplateConfig,
    #[allow(dead_code)]
    container_ids: Vec<String>,
    socket_paths: Vec<PathBuf>,
    rr: AtomicUsize,
    sem: Arc<Semaphore>,
}
impl TemplateRuntime {
    fn pick_path(&self) -> &Path {
        let i = self.rr.fetch_add(1, Ordering::Relaxed);
        &self.socket_paths[i % self.socket_paths.len()]
    }
}

struct AppState {
    registry: Arc<PagedRegistry>,
    unix_pools: dashmap::DashMap<PathBuf, Mutex<VecDeque<UnixStream>>>,
    child_fd_pools: dashmap::DashMap<PathBuf, Arc<Mutex<VecDeque<UnixStream>>>>,
    lease_locks: dashmap::DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>,
    pool_cap_per_path: usize,
    max_timeout: u32,
}

// ---- Paged registry: lazy start + LRU eviction --------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tier {
    Hot,
    Warm,
}

struct RegistryState {
    /// Runtime exists for both Hot and Warm tiers; gone when Cold.
    runtimes: HashMap<String, (Arc<TemplateRuntime>, Tier, std::time::Instant)>,
    /// LRU order across Hot + Warm; newest at back.
    lru: VecDeque<String>,
}

struct PagedRegistry {
    docker: Docker,
    configs: HashMap<String, TemplateConfig>,
    state: tokio::sync::Mutex<RegistryState>,
    name_locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    hot_limit: usize,
    warm_limit: usize,
}

impl PagedRegistry {
    fn new(
        docker: Docker,
        configs: HashMap<String, TemplateConfig>,
        hot_limit: usize,
        warm_limit: usize,
    ) -> Self {
        Self {
            docker,
            configs,
            state: tokio::sync::Mutex::new(RegistryState {
                runtimes: HashMap::new(),
                lru: VecDeque::new(),
            }),
            name_locks: std::sync::Mutex::new(HashMap::new()),
            hot_limit,
            warm_limit,
        }
    }

    fn name_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.name_locks.lock().unwrap();
        locks
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn touch_lru_locked(state: &mut RegistryState, name: &str) {
        if let Some(pos) = state.lru.iter().position(|n| n == name) {
            state.lru.remove(pos);
        }
        state.lru.push_back(name.to_string());
    }

    fn count_by_tier(state: &RegistryState, tier: Tier) -> usize {
        state
            .runtimes
            .values()
            .filter(|(_, t, _)| *t == tier)
            .count()
    }

    /// Get the runtime, starting/unpausing on demand. Cascading eviction.
    async fn acquire(&self, name: &str) -> anyhow::Result<Arc<TemplateRuntime>> {
        // Fast path: already Hot.
        {
            let mut state = self.state.lock().await;
            if let Some((rt, tier, _)) = state.runtimes.get(name).cloned() {
                if tier == Tier::Hot {
                    Self::touch_lru_locked(&mut state, name);
                    state.runtimes.insert(
                        name.to_string(),
                        (rt.clone(), Tier::Hot, std::time::Instant::now()),
                    );
                    return Ok(rt);
                }
            }
        }
        // Slow path: serialize transitions by name.
        let lock = self.name_lock(name);
        let _guard = lock.lock().await;

        // Re-check after acquiring per-name lock.
        let current = {
            let state = self.state.lock().await;
            state.runtimes.get(name).cloned()
        };
        if let Some((rt, tier, _)) = current {
            if tier == Tier::Hot {
                let mut state = self.state.lock().await;
                Self::touch_lru_locked(&mut state, name);
                state.runtimes.insert(
                    name.to_string(),
                    (rt.clone(), Tier::Hot, std::time::Instant::now()),
                );
                return Ok(rt);
            }
            // Warm → unpause and promote.
            let cids: Vec<String> = rt.container_ids.clone();
            for cid in &cids {
                if let Err(e) = self.docker.unpause_container(cid).await {
                    eprintln!(
                        "[paged] unpause {} failed: {}",
                        &cid[..12.min(cid.len())],
                        e
                    );
                }
            }
            // Need room in Hot (may cascade Warm).
            self.evict_hot_to_warm_if_needed_excluding(name).await;
            {
                let mut state = self.state.lock().await;
                state.runtimes.insert(
                    name.to_string(),
                    (rt.clone(), Tier::Hot, std::time::Instant::now()),
                );
                Self::touch_lru_locked(&mut state, name);
            }
            let hot_n = Self::count_by_tier(&*self.state.lock().await, Tier::Hot);
            eprintln!("[paged] WARM→HOT {} (hot_count={})", name, hot_n);
            return Ok(rt);
        }

        // Cold start.
        let cfg = self
            .configs
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown template: {}", name))?;
        self.evict_hot_to_warm_if_needed_excluding(name).await;
        let tag = format!("inspect-tpl-{}:latest", cfg.name);
        if !docker_image_exists(&self.docker, &tag).await? {
            return Err(anyhow::anyhow!(
                "template {} image {} not built",
                cfg.name,
                tag
            ));
        }
        let mut container_ids = Vec::new();
        let mut socket_paths = Vec::new();
        for i in 0..cfg.containers {
            let (cid, sock) = start_template_container(&self.docker, &cfg, i).await?;
            wait_ready(&sock, std::time::Duration::from_secs(30)).await?;
            container_ids.push(cid);
            socket_paths.push(sock);
        }
        let rt = Arc::new(TemplateRuntime {
            sem: Arc::new(tokio::sync::Semaphore::new(
                cfg.containers.max(1) * cfg.pool_size.max(1),
            )),
            container_ids,
            socket_paths,
            rr: std::sync::atomic::AtomicUsize::new(0),
            cfg,
        });
        {
            let mut state = self.state.lock().await;
            state.runtimes.insert(
                name.to_string(),
                (rt.clone(), Tier::Hot, std::time::Instant::now()),
            );
            Self::touch_lru_locked(&mut state, name);
        }
        let hot_n = Self::count_by_tier(&*self.state.lock().await, Tier::Hot);
        eprintln!("[paged] COLD→HOT {} (hot_count={})", name, hot_n);
        Ok(rt)
    }

    /// Demote oldest Hot templates to Warm (pause) until Hot count is within limit.
    /// May cascade to evict oldest Warm if Warm pool overfills.
    async fn evict_hot_to_warm_if_needed_excluding(&self, exclude: &str) {
        loop {
            // Find oldest Hot != exclude.
            let victim = {
                let state = self.state.lock().await;
                let hot_count = Self::count_by_tier(&state, Tier::Hot);
                if hot_count < self.hot_limit {
                    return;
                }
                // Scan LRU front-to-back for first Hot != exclude
                let mut found = None;
                for n in state.lru.iter() {
                    if n == exclude {
                        continue;
                    }
                    if let Some((_, Tier::Hot, _)) = state.runtimes.get(n) {
                        found = Some(n.clone());
                        break;
                    }
                }
                found
            };
            let Some(name) = victim else {
                return;
            };
            // Pause and mark Warm.
            let rt = {
                let state = self.state.lock().await;
                state.runtimes.get(&name).cloned()
            };
            if let Some((rt, _, _)) = rt {
                for cid in &rt.container_ids {
                    let _ = self.docker.pause_container(cid).await;
                }
                {
                    let mut state = self.state.lock().await;
                    state
                        .runtimes
                        .insert(name.clone(), (rt, Tier::Warm, std::time::Instant::now()));
                }
                let warm_n = Self::count_by_tier(&*self.state.lock().await, Tier::Warm);
                eprintln!("[paged] HOT→WARM {} (warm_count={})", name, warm_n);
                // After demoting Hot→Warm, may need to drop oldest Warm to Cold.
                self.evict_warm_to_cold_if_needed().await;
            }
        }
    }

    /// Remove (Cold) oldest Warm templates beyond warm_limit.
    async fn evict_warm_to_cold_if_needed(&self) {
        loop {
            let victim = {
                let state = self.state.lock().await;
                let warm_count = Self::count_by_tier(&state, Tier::Warm);
                if warm_count <= self.warm_limit {
                    return;
                }
                let mut found = None;
                for n in state.lru.iter() {
                    if let Some((_, Tier::Warm, _)) = state.runtimes.get(n) {
                        found = Some(n.clone());
                        break;
                    }
                }
                found
            };
            let Some(name) = victim else {
                return;
            };
            let rt = {
                let mut state = self.state.lock().await;
                state.lru.retain(|n| n != &name);
                state.runtimes.remove(&name)
            };
            if let Some((rt, _, _)) = rt {
                // Unpause first then remove (otherwise docker rm of paused may error).
                for cid in &rt.container_ids {
                    let _ = self.docker.unpause_container(cid).await;
                    let _ = self
                        .docker
                        .remove_container(
                            cid,
                            Some(bollard::container::RemoveContainerOptions {
                                force: true,
                                ..Default::default()
                            }),
                        )
                        .await;
                }
                let warm_n = Self::count_by_tier(&*self.state.lock().await, Tier::Warm);
                eprintln!("[paged] WARM→COLD {} (warm_count={})", name, warm_n);
            }
        }
    }

    /// Snapshot of currently-hot templates (for stats/metrics/list).
    async fn hot_snapshot(&self) -> Vec<(String, Arc<TemplateRuntime>)> {
        let state = self.state.lock().await;
        state
            .runtimes
            .iter()
            .filter(|(_, (_, t, _))| *t == Tier::Hot)
            .map(|(k, (v, _, _))| (k.clone(), v.clone()))
            .collect()
    }

    /// Snapshot of Warm templates too (for /templates).
    async fn full_snapshot(&self) -> Vec<(String, Arc<TemplateRuntime>, Tier)> {
        let state = self.state.lock().await;
        state
            .runtimes
            .iter()
            .map(|(k, (v, t, _))| (k.clone(), v.clone(), *t))
            .collect()
    }
}

// ---- frame I/O ----------------------------------------------------------

async fn send_frame(stream: &mut UnixStream, payload: &[u8]) -> std::io::Result<()> {
    let n = (payload.len() as u32).to_be_bytes();
    stream.write_all(&n).await?;
    stream.write_all(payload).await
}
async fn recv_frame(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    let n = u32::from_be_bytes(hdr) as usize;
    if n > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn worker_call_oneshot(
    socket_path: &Path,
    req: &pb::Request,
    timeout: Duration,
) -> Result<pb::Response> {
    let stream = tokio::time::timeout(timeout, UnixStream::connect(socket_path))
        .await
        .context("connect timeout")?
        .context("connect")?;
    let mut stream = stream;
    let body = req.encode_to_vec();
    tokio::time::timeout(timeout, send_frame(&mut stream, &body))
        .await
        .context("send timeout")?
        .context("send")?;
    let resp_bytes = tokio::time::timeout(timeout, recv_frame(&mut stream))
        .await
        .context("recv timeout")?
        .context("recv")?;
    Ok(pb::Response::decode(&*resp_bytes)?)
}

async fn worker_call_pooled(
    state: &AppState,
    socket_path: &Path,
    req: &pb::Request,
    timeout: Duration,
) -> Result<pb::Response> {
    let stream_opt = {
        state
            .unix_pools
            .get(socket_path)
            .and_then(|entry| entry.lock().unwrap().pop_front())
    };
    let mut stream = match stream_opt {
        Some(s) => s,
        None => tokio::time::timeout(timeout, UnixStream::connect(socket_path))
            .await
            .context("connect timeout")?
            .context("connect")?,
    };

    let body = req.encode_to_vec();
    let send_result = tokio::time::timeout(timeout, async {
        send_frame(&mut stream, &body).await?;
        recv_frame(&mut stream).await
    })
    .await;

    match send_result {
        Ok(Ok(resp_bytes)) => {
            let entry = state
                .unix_pools
                .entry(socket_path.to_path_buf())
                .or_insert_with(|| Mutex::new(VecDeque::new()));
            let mut pool = entry.lock().unwrap();
            if pool.len() < state.pool_cap_per_path {
                pool.push_back(stream);
            }
            Ok(pb::Response::decode(&*resp_bytes)?)
        }
        Ok(Err(e)) => {
            drop(stream);
            Err(e.into())
        }
        Err(_) => {
            drop(stream);
            Err(anyhow!("request timeout"))
        }
    }
}

// ---- docker + template helpers ------------------------------------------

fn load_templates(dir: &Path) -> Result<Vec<TemplateConfig>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let toml_path = e.path().join("template.toml");
        if !toml_path.exists() {
            continue;
        }
        let raw = std::fs::read_to_string(&toml_path)?;
        let cfg: TemplateConfig =
            toml::from_str(&raw).with_context(|| format!("parse {}", toml_path.display()))?;
        out.push(cfg);
    }
    Ok(out)
}

async fn docker_image_exists(docker: &Docker, tag: &str) -> Result<bool> {
    match docker.inspect_image(tag).await {
        Ok(_) => Ok(true),
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn cleanup_old_hot_containers(docker: &Docker) -> Result<()> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    filters.insert("label".into(), vec!["inspect-api-hot=1".into()]);
    let containers = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: true,
            filters,
            ..Default::default()
        }))
        .await?;
    for c in containers {
        if let Some(id) = c.id {
            let _ = docker
                .remove_container(
                    &id,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;
        }
    }
    if let Ok(entries) = std::fs::read_dir(socket_root()) {
        for e in entries.flatten() {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
    Ok(())
}

fn parse_memory(s: &str) -> Result<i64> {
    let s = s.trim().to_lowercase();
    let (num, mult): (&str, i64) =
        if let Some(n) = s.strip_suffix("gb").or_else(|| s.strip_suffix('g')) {
            (n, 1024 * 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("mb").or_else(|| s.strip_suffix('m')) {
            (n, 1024 * 1024)
        } else if let Some(n) = s.strip_suffix("kb").or_else(|| s.strip_suffix('k')) {
            (n, 1024)
        } else {
            (s.as_str(), 1)
        };
    Ok(num.trim().parse::<i64>()? * mult)
}

async fn start_template_container(
    docker: &Docker,
    cfg: &TemplateConfig,
    idx: usize,
) -> Result<(String, PathBuf)> {
    let tag = format!("inspect-tpl-{}:latest", cfg.name);
    let mem = parse_memory(&cfg.memory_reservation)?;

    let dir_id = format!(
        "{}-{}-{}",
        cfg.name,
        idx,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let host_dir = socket_root().join(&dir_id);
    std::fs::create_dir_all(&host_dir)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&host_dir, std::fs::Permissions::from_mode(0o777))?;

    let mut labels = HashMap::new();
    labels.insert("inspect-api-hot".to_string(), "1".to_string());
    labels.insert("inspect-tpl".to_string(), cfg.name.clone());
    labels.insert(
        "inspect-sockdir".to_string(),
        host_dir.to_string_lossy().into_owned(),
    );

    let bind_spec = format!("{}:/sockets", host_dir.display());

    // Forward selected env vars from the api-rust process to each worker
    // container. Whitelist only — never blanket-pass the env (would leak
    // unrelated secrets). These enable TOS / S3-compatible stdout offload
    // via boto3 inside the worker.
    let mut env: Vec<String> = Vec::new();
    for k in [
        "WORKER_STDOUT_S3_THRESHOLD",
        "WORKER_STDOUT_S3_BUCKET",
        "WORKER_STDOUT_S3_PREFIX",
        "WORKER_STDOUT_S3_ENDPOINT_URL",
        "WORKER_STDOUT_S3_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ] {
        if let Ok(v) = std::env::var(k) {
            if !v.is_empty() {
                env.push(format!("{}={}", k, v));
            }
        }
    }
    // TOS_* are the human-facing names in .env; map them to the AWS / WORKER names.
    if let (Ok(ak), Ok(sk)) = (
        std::env::var("TOS_ACCESS_KEY"),
        std::env::var("TOS_SECRET_KEY"),
    ) {
        if !env.iter().any(|s| s.starts_with("AWS_ACCESS_KEY_ID=")) {
            env.push(format!("AWS_ACCESS_KEY_ID={}", ak));
        }
        if !env.iter().any(|s| s.starts_with("AWS_SECRET_ACCESS_KEY=")) {
            env.push(format!("AWS_SECRET_ACCESS_KEY={}", sk));
        }
    }
    if let Ok(ep) = std::env::var("TOS_S3_ENDPOINT") {
        if !ep.is_empty()
            && !env
                .iter()
                .any(|s| s.starts_with("WORKER_STDOUT_S3_ENDPOINT_URL="))
        {
            env.push(format!("WORKER_STDOUT_S3_ENDPOINT_URL={}", ep));
        }
    }
    if let Ok(r) = std::env::var("TOS_REGION") {
        if !r.is_empty()
            && !env
                .iter()
                .any(|s| s.starts_with("WORKER_STDOUT_S3_REGION="))
        {
            env.push(format!("WORKER_STDOUT_S3_REGION={}", r));
        }
    }
    let env_opt = if env.is_empty() { None } else { Some(env) };

    let net_mode = std::env::var("WORKER_NETWORK_MODE").unwrap_or_else(|_| "none".into());

    // Mount /tmp (and /var/tmp) as tmpfs so sandbox_helper's per-request
    // mkdtemp + Python stdlib temp writes have a writable path while the
    // rest of the rootfs stays read-only.
    let tmpfs_size = std::env::var("WORKER_TMPFS_SIZE").unwrap_or_else(|_| "1g".into());
    let tmpfs = std::collections::HashMap::from([
        ("/tmp".to_string(), format!("size={}", tmpfs_size)),
        ("/var/tmp".to_string(), format!("size={}", tmpfs_size)),
    ]);

    let config = bollard::container::Config::<String> {
        image: Some(tag),
        labels: Some(labels),
        env: env_opt,
        host_config: Some(HostConfig {
            binds: Some(vec![bind_spec]),
            memory_reservation: Some(mem),
            cpu_shares: Some(1024),
            pids_limit: Some(cfg.pids_limit),
            oom_score_adj: Some(500),
            security_opt: Some(vec!["no-new-privileges".into()]),
            network_mode: Some(net_mode),
            // Read-only container rootfs blocks user code from writing
            // to / clobbering anything in the worker image; sibling
            // requests on the same child can't see each other's
            // container-fs writes. Tmpfs above carves out /tmp for the
            // per-request mkdtemp + general Python temp use.
            readonly_rootfs: Some(true),
            tmpfs: Some(tmpfs),
            ..Default::default()
        }),
        ..Default::default()
    };
    let created = docker
        .create_container(None::<CreateContainerOptions<String>>, config)
        .await
        .context("create_container")?;
    docker
        .start_container(&created.id, None::<StartContainerOptions<String>>)
        .await
        .context("start_container")?;
    Ok((created.id, host_dir.join("worker.sock")))
}

async fn wait_ready(socket_path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_err: Option<String> = None;
    while !socket_path.exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !socket_path.exists() {
        return Err(anyhow!("socket {} did not appear", socket_path.display()));
    }
    while Instant::now() < deadline {
        let req = pb::Request {
            cmd: "health".into(),
            ..Default::default()
        };
        match worker_call_oneshot(socket_path, &req, Duration::from_secs(2)).await {
            Ok(r) => {
                if let Some(h) = r.health {
                    if h.idle > 0 {
                        return Ok(());
                    }
                }
                last_err = Some("idle=0".into());
            }
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!(
        "worker at {} not ready: {}",
        socket_path.display(),
        last_err.unwrap_or_default()
    ))
}

// ---- HTTP handlers ------------------------------------------------------

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let full = state.registry.full_snapshot().await;
    let mut hot_names: Vec<String> = vec![];
    let mut warm_names: Vec<String> = vec![];
    for (name, _rt, tier) in &full {
        if *tier == Tier::Hot {
            hot_names.push(name.clone());
        } else {
            warm_names.push(name.clone());
        }
    }
    hot_names.sort();
    warm_names.sort();
    AxumJson(json!({
        "ok": true,
        "configs_total": state.registry.configs.len(),
        "hot": hot_names,
        "warm": warm_names,
        "hot_count": hot_names.len(),
        "warm_count": warm_names.len(),
        "hot_limit": state.registry.hot_limit,
        "warm_limit": state.registry.warm_limit,
    }))
}

async fn list_templates(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let full = state.registry.full_snapshot().await;
    let hot_set: std::collections::HashSet<String> = full
        .iter()
        .filter(|(_, _, t)| *t == Tier::Hot)
        .map(|(n, _, _)| n.clone())
        .collect();
    let warm_set: std::collections::HashSet<String> = full
        .iter()
        .filter(|(_, _, t)| *t == Tier::Warm)
        .map(|(n, _, _)| n.clone())
        .collect();
    let hot_map: HashMap<String, Arc<TemplateRuntime>> = full
        .into_iter()
        .filter(|(_, _, t)| *t == Tier::Hot)
        .map(|(n, rt, _)| (n, rt))
        .collect();
    let mut out = Vec::new();
    let mut names: Vec<&String> = state.registry.configs.keys().collect();
    names.sort();
    for name in names {
        let cfg = &state.registry.configs[name];
        let _is_hot = hot_set.contains(name);
        let mut idle = 0i64;
        if let Some(rt) = hot_map.get(name) {
            for p in &rt.socket_paths {
                let req = pb::Request {
                    cmd: "health".into(),
                    ..Default::default()
                };
                if let Ok(r) = worker_call_pooled(&state, p, &req, Duration::from_secs(2)).await {
                    if let Some(h) = r.health {
                        idle += h.idle as i64;
                    }
                }
            }
        }
        let tier = if hot_set.contains(name) {
            "hot"
        } else if warm_set.contains(name) {
            "warm"
        } else {
            "cold"
        };
        out.push(json!({
            "name": name,
            "image": format!("inspect-tpl-{}:latest", name),
            "containers": cfg.containers,
            "pool_size": cfg.pool_size,
            "concurrency": cfg.containers * cfg.pool_size,
            "idle": if hot_set.contains(name) { idle } else { -1 },
            "prewarm": cfg.prewarm,
            "tier": tier,
        }));
    }
    AxumJson(json!({
        "templates": out,
        "hot_count": hot_map.len(),
        "hot_limit": state.registry.hot_limit,
    }))
}

async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut by_template = serde_json::Map::new();
    let snap = state.registry.hot_snapshot().await;
    for (name, rt) in snap.iter() {
        let mut per_container = Vec::new();
        let mut agg_req = 0u64;
        let mut agg_forks = 0u64;
        let mut agg_kt = 0u64;
        let mut agg_ka = 0u64;
        let mut agg_ki = 0u64;
        let mut agg_idle = 0u64;
        let mut agg_active = 0u64;
        let mut agg_respawns: HashMap<String, u64> = HashMap::new();
        for p in &rt.socket_paths {
            let req = pb::Request {
                cmd: "stats".into(),
                ..Default::default()
            };
            match worker_call_pooled(&state, p, &req, Duration::from_secs(3)).await {
                Ok(r) => {
                    if let Ok(j) = serde_json::from_str::<Value>(&r.json) {
                        agg_req += j
                            .pointer("/lifetime/requests_total")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        agg_forks += j
                            .pointer("/lifetime/forks_total")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        agg_idle += j.get("idle").and_then(|v| v.as_u64()).unwrap_or(0);
                        agg_active += j.get("active_approx").and_then(|v| v.as_u64()).unwrap_or(0);
                        agg_kt += j
                            .pointer("/lifetime/kills_hard_timeout")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        agg_ka += j
                            .pointer("/lifetime/kills_reaper_age")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        agg_ki += j
                            .pointer("/lifetime/kills_reaper_idle")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        if let Some(rmap) = j
                            .pointer("/lifetime/respawns_by_reason")
                            .and_then(|v| v.as_object())
                        {
                            for (k, v) in rmap {
                                if let Some(n) = v.as_u64() {
                                    *agg_respawns.entry(k.clone()).or_insert(0) += n;
                                }
                            }
                        }
                        per_container.push(j);
                    }
                }
                Err(e) => per_container
                    .push(json!({"socket": p.to_string_lossy(), "error": e.to_string()})),
            }
        }
        by_template.insert(
            name.clone(),
            json!({
                "image": format!("inspect-tpl-{}:latest", name),
                "containers": rt.socket_paths.len(),
                "pool_size": rt.cfg.pool_size,
                "concurrency": rt.cfg.containers * rt.cfg.pool_size,
                "prewarm": rt.cfg.prewarm,
                "aggregate": {
                    "idle": agg_idle, "active_approx": agg_active,
                    "requests_total": agg_req, "forks_total": agg_forks,
                    "kills_hard_timeout": agg_kt,
                    "kills_reaper_age": agg_ka,
                    "kills_reaper_idle": agg_ki,
                    "respawns_by_reason": agg_respawns,
                },
                "containers_detail": per_container,
            }),
        );
    }
    AxumJson(json!({"templates": by_template}))
}

#[derive(Deserialize)]
struct DrainBody {
    template: Option<String>,
    pid: Option<u32>,
    reason: Option<String>,
}

async fn admin_drain(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DrainBody>,
) -> impl IntoResponse {
    let snap = state.registry.hot_snapshot().await;
    let hot_map: HashMap<String, Arc<TemplateRuntime>> = snap.into_iter().collect();
    let templates_to_drain: Vec<String> = match &body.template {
        Some(t) => vec![t.clone()],
        None => hot_map.keys().cloned().collect(),
    };
    let mut results = Vec::new();
    for tpl in templates_to_drain {
        if let Some(rt) = hot_map.get(&tpl) {
            for p in &rt.socket_paths {
                let req = pb::Request {
                    cmd: "drain".into(),
                    drain: Some(pb::DrainReq {
                        pid: body.pid,
                        reason: body.reason.clone().unwrap_or_default(),
                    }),
                    ..Default::default()
                };
                match worker_call_pooled(&state, p, &req, Duration::from_secs(5)).await {
                    Ok(r) => {
                        let parsed: Value = serde_json::from_str(&r.json).unwrap_or(json!({}));
                        results.push(json!({"socket": p.to_string_lossy(), "template": tpl.clone(), "result": parsed}));
                    }
                    Err(e) => results.push(json!({"socket": p.to_string_lossy(), "template": tpl.clone(), "error": e.to_string()})),
                }
            }
        }
    }
    AxumJson(json!({"results": results}))
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use axum::http::header;
    struct TplAgg {
        name: String,
        req: u64,
        forks: u64,
        idle: u64,
        active: u64,
        kt: u64,
        ka: u64,
        ki: u64,
        respawns: HashMap<String, u64>,
    }
    let mut all: Vec<TplAgg> = Vec::new();
    let snap = state.registry.hot_snapshot().await;
    for (name, rt) in snap.iter() {
        let mut a = TplAgg {
            name: name.clone(),
            req: 0,
            forks: 0,
            idle: 0,
            active: 0,
            kt: 0,
            ka: 0,
            ki: 0,
            respawns: HashMap::new(),
        };
        for p in &rt.socket_paths {
            let req = pb::Request {
                cmd: "stats".into(),
                ..Default::default()
            };
            if let Ok(r) = worker_call_pooled(&state, p, &req, Duration::from_secs(3)).await {
                if let Ok(j) = serde_json::from_str::<Value>(&r.json) {
                    a.req += j
                        .pointer("/lifetime/requests_total")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    a.forks += j
                        .pointer("/lifetime/forks_total")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    a.idle += j.get("idle").and_then(|v| v.as_u64()).unwrap_or(0);
                    a.active += j.get("active_approx").and_then(|v| v.as_u64()).unwrap_or(0);
                    a.kt += j
                        .pointer("/lifetime/kills_hard_timeout")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    a.ka += j
                        .pointer("/lifetime/kills_reaper_age")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    a.ki += j
                        .pointer("/lifetime/kills_reaper_idle")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    if let Some(rmap) = j
                        .pointer("/lifetime/respawns_by_reason")
                        .and_then(|v| v.as_object())
                    {
                        for (k, v) in rmap {
                            if let Some(n) = v.as_u64() {
                                *a.respawns.entry(k.clone()).or_insert(0) += n;
                            }
                        }
                    }
                }
            }
        }
        all.push(a);
    }
    let mut out = String::new();
    out.push_str("# HELP inspect_requests_total Total /exec_hot requests served.\n# TYPE inspect_requests_total counter\n");
    for t in &all {
        out.push_str(&format!(
            "inspect_requests_total{{template=\"{}\"}} {}\n",
            t.name, t.req
        ));
    }
    out.push_str("\n# HELP inspect_forks_total Forks.\n# TYPE inspect_forks_total counter\n");
    for t in &all {
        out.push_str(&format!(
            "inspect_forks_total{{template=\"{}\"}} {}\n",
            t.name, t.forks
        ));
    }
    out.push_str("\n# HELP inspect_idle Currently idle children.\n# TYPE inspect_idle gauge\n");
    for t in &all {
        out.push_str(&format!(
            "inspect_idle{{template=\"{}\"}} {}\n",
            t.name, t.idle
        ));
    }
    out.push_str(
        "\n# HELP inspect_active Currently active children.\n# TYPE inspect_active gauge\n",
    );
    for t in &all {
        out.push_str(&format!(
            "inspect_active{{template=\"{}\"}} {}\n",
            t.name, t.active
        ));
    }
    out.push_str("\n# HELP inspect_kills_total Children killed by reason.\n# TYPE inspect_kills_total counter\n");
    for t in &all {
        out.push_str(&format!(
            "inspect_kills_total{{template=\"{}\",reason=\"hard_timeout\"}} {}\n",
            t.name, t.kt
        ));
        out.push_str(&format!(
            "inspect_kills_total{{template=\"{}\",reason=\"reaper_age\"}} {}\n",
            t.name, t.ka
        ));
        out.push_str(&format!(
            "inspect_kills_total{{template=\"{}\",reason=\"reaper_idle\"}} {}\n",
            t.name, t.ki
        ));
    }
    out.push_str("\n# HELP inspect_respawns_total Child respawns by reason.\n# TYPE inspect_respawns_total counter\n");
    for t in &all {
        for (reason, count) in &t.respawns {
            out.push_str(&format!(
                "inspect_respawns_total{{template=\"{}\",reason=\"{}\"}} {}\n",
                t.name, reason, count
            ));
        }
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}

use std::os::unix::io::AsRawFd as _AsRawFd;
use std::os::unix::io::FromRawFd as _FromRawFd;
use std::os::unix::io::RawFd as _RawFd;

/// Synchronously connect to worker and lease N child fds via SCM_RIGHTS.
/// Returns the leased UnixStreams (already async-ready).
fn lease_child_fds_blocking(socket_path: &Path, count: usize) -> anyhow::Result<Vec<UnixStream>> {
    use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};
    use std::io::IoSliceMut;
    use std::io::Write as _;

    // Connect (blocking std stream).
    let mut conn = std::os::unix::net::UnixStream::connect(socket_path)
        .with_context(|| format!("connect {}", socket_path.display()))?;

    // Send lease request: 4-byte length + protobuf body.
    let req = pb::Request {
        cmd: "lease".into(),
        lease_count: count as u32,
        ..Default::default()
    };
    let body = req.encode_to_vec();
    conn.write_all(&(body.len() as u32).to_be_bytes())?;
    conn.write_all(&body)?;

    // recvmsg the WHOLE frame (header + payload) in one syscall — SCM_RIGHTS
    // cmsg is attached to the first byte of the message, so we must not split
    // it across two reads (the first read would silently drop the cmsg).
    let mut buf = vec![0u8; 64 * 1024];
    let mut cmsg_buf = nix::cmsg_space!([_RawFd; 128]);
    let (bytes_read, cmsg_iter): (usize, Vec<ControlMessageOwned>) = {
        let mut iov = [IoSliceMut::new(&mut buf)];
        let res = recvmsg::<()>(
            conn.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buf),
            MsgFlags::empty(),
        )
        .map_err(|e| anyhow!("recvmsg: {}", e))?;
        let cmsgs: Vec<ControlMessageOwned> =
            res.cmsgs().map_err(|e| anyhow!("cmsgs: {}", e))?.collect();
        (res.bytes, cmsgs)
    };
    if bytes_read < 4 {
        anyhow::bail!("short recvmsg: {} bytes", bytes_read);
    }
    let payload_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if 4 + payload_len > bytes_read {
        anyhow::bail!(
            "recvmsg got {} bytes, payload says {}",
            bytes_read,
            payload_len
        );
    }

    let mut fds: Vec<_RawFd> = Vec::new();
    for cmsg in cmsg_iter {
        if let ControlMessageOwned::ScmRights(rights) = cmsg {
            fds.extend(rights);
        }
    }
    if fds.is_empty() {
        anyhow::bail!("lease returned no fds");
    }

    let mut streams = Vec::with_capacity(fds.len());
    for fd in fds {
        let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
        std_stream.set_nonblocking(true)?;
        let async_stream = UnixStream::from_std(std_stream)?;
        streams.push(async_stream);
    }
    Ok(streams)
}

async fn acquire_child_fd(
    state: &AppState,
    socket_path: &Path,
    batch: usize,
) -> anyhow::Result<UnixStream> {
    // Get/create the pool for this socket path.
    let pool = state
        .child_fd_pools
        .entry(socket_path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
        .clone();
    // Fast path: pop from pool.
    if let Some(fd) = pool.lock().unwrap().pop_front() {
        return Ok(fd);
    }
    // Slow path: lease more, serialized by lease_locks.
    let lease_lock = state
        .lease_locks
        .entry(socket_path.to_path_buf())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _g = lease_lock.lock().await;
    // Re-check pool after acquiring lock.
    if let Some(fd) = pool.lock().unwrap().pop_front() {
        return Ok(fd);
    }
    let path = socket_path.to_path_buf();
    let streams = tokio::task::spawn_blocking(move || lease_child_fds_blocking(&path, batch))
        .await
        .map_err(|e| anyhow!("spawn_blocking: {}", e))??;
    let mut iter = streams.into_iter();
    let first = iter.next().ok_or_else(|| anyhow!("empty lease"))?;
    let mut pool_lock = pool.lock().unwrap();
    for s in iter {
        pool_lock.push_back(s);
    }
    Ok(first)
}

fn return_child_fd(state: &AppState, socket_path: &Path, fd: UnixStream) {
    let pool = state
        .child_fd_pools
        .entry(socket_path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
        .clone();
    pool.lock().unwrap().push_back(fd);
}

async fn exec_direct(
    state: &AppState,
    socket_path: &Path,
    job: pb::Job,
    batch: usize,
    timeout: Duration,
) -> anyhow::Result<pb::ExecResult> {
    let mut child = acquire_child_fd(state, socket_path, batch).await?;
    let job_bytes = job.encode_to_vec();
    let send_recv = async {
        send_frame(&mut child, &job_bytes).await?;
        recv_frame(&mut child).await
    };
    let resp_bytes = match tokio::time::timeout(timeout, send_recv).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            drop(child);
            return Err(anyhow!("child rpc: {}", e));
        }
        Err(_) => {
            drop(child);
            return Err(anyhow!("child timeout"));
        }
    };
    let cresp = pb::ChildResponse::decode(&*resp_bytes)?;
    if !cresp.expire {
        return_child_fd(state, socket_path, child);
    } else {
        drop(child); // close → child exits → worker's reaper refills
    }
    Ok(pb::ExecResult {
        stdout: cresp.stdout,
        stderr: cresp.stderr,
        exit_code: cresp.exit_code,
        elapsed_ms: 0,
        output_files: cresp.output_files,
        deleted_files: cresp.deleted_files,
        output_files_b64: cresp.output_files_b64,
    })
}

async fn exec_hot(
    State(state): State<Arc<AppState>>,
    Json(req): Json<HotExecRequest>,
) -> Result<AxumJson<ExecResponse>, (StatusCode, String)> {
    let rt = state.registry.acquire(&req.template).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("acquire template '{}': {}", req.template, e),
        )
    })?;
    let timeout_s = req.timeout.min(state.max_timeout).max(1) as u64;
    let start = Instant::now();
    let _permit = rt
        .sem
        .acquire()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let socket = rt.pick_path().to_path_buf();
    let job = pb::Job {
        code: req.code,
        timeout: req.timeout,
        env: req.env,
        files: req.files,
        persist_changes: req.persist_changes,
        persist_root_label: req.persist_root_label,
    };
    let batch = rt.cfg.pool_size as usize;
    let exec = exec_direct(
        &state,
        &socket,
        job,
        batch,
        Duration::from_secs(timeout_s + 10),
    )
    .await
    .map_err(|e| (StatusCode::BAD_GATEWAY, format!("worker: {e}")))?;
    Ok(AxumJson(ExecResponse {
        stdout: exec.stdout,
        stderr: exec.stderr,
        exit_code: exec.exit_code,
        elapsed_ms: start.elapsed().as_millis() as u64,
        container_id: rt.cfg.name.clone(),
        output_files: exec.output_files,
        deleted_files: exec.deleted_files,
        output_files_b64: exec.output_files_b64,
    }))
}

async fn exec_cold() -> impl IntoResponse {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "/exec (cold path) not implemented; use /exec_hot with a template",
    )
}

// ---- /admin/build/:name (streaming log) ---------------------------------

#[derive(Deserialize, Default)]
struct BuildQuery {
    #[serde(default)]
    push: bool,
    #[serde(default)]
    push_tag: Option<String>,
}

async fn admin_build(
    AxumPath(name): AxumPath<String>,
    axum::extract::Query(q): axum::extract::Query<BuildQuery>,
) -> impl IntoResponse {
    use bytes::Bytes;
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt;
    use tokio::process::Command as TCommand;
    use tokio_stream::wrappers::ReceiverStream;

    // Sanity-check name (no /, ..).
    if name.is_empty() || name.contains('/') || name.contains("..") {
        return (StatusCode::BAD_REQUEST, "bad template name").into_response();
    }

    let mut args: Vec<String> = vec![name.clone()];
    if q.push {
        args.push("--push".into());
        if let Some(t) = q.push_tag {
            if !t.is_empty() {
                args.push("--push-tag".into());
                args.push(t);
            }
        }
    }

    let mut child = match TCommand::new("template-build")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("spawn template-build: {}", e),
            )
                .into_response()
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    let tx_out = tx.clone();
    tokio::spawn(async move {
        let mut br = tokio::io::BufReader::new(stdout).lines();
        while let Ok(Some(line)) = br.next_line().await {
            let _ = tx_out.send(Ok(Bytes::from(format!("{}\n", line)))).await;
        }
    });
    let tx_err = tx.clone();
    tokio::spawn(async move {
        let mut br = tokio::io::BufReader::new(stderr).lines();
        while let Ok(Some(line)) = br.next_line().await {
            let _ = tx_err
                .send(Ok(Bytes::from(format!("stderr: {}\n", line))))
                .await;
        }
    });
    tokio::spawn(async move {
        let status = child.wait().await;
        let exit = match status {
            Ok(s) => s.code().unwrap_or(-1),
            Err(_) => -1,
        };
        let _ = tx
            .send(Ok(Bytes::from(format!("\n=== exit {} ===\n", exit))))
            .await;
    });

    let body = axum::body::Body::from_stream(ReceiverStream::new(rx));
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-content-type-options", "nosniff")
        .body(body)
        .unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let templates_dir = PathBuf::from(
        std::env::var("INSPECT_API_TEMPLATES_DIR")
            .unwrap_or_else(|_| "/opt/inspect-api/templates".into()),
    );
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8000);
    let max_timeout: u32 = std::env::var("INSPECT_API_MAX_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let pool_cap_per_path: usize = std::env::var("INSPECT_API_UNIX_POOL_PER_PATH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let _ = std::fs::create_dir_all(socket_root());

    let configs = load_templates(&templates_dir)?;
    eprintln!("[api] loaded {} template configs", configs.len());

    let docker = Docker::connect_with_local_defaults().context("connect docker")?;
    cleanup_old_hot_containers(&docker).await.ok();

    let hot_limit: usize = std::env::var("INSPECT_API_HOT_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let warm_limit: usize = std::env::var("INSPECT_API_WARM_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(hot_limit * 4);
    let mut cfg_map: HashMap<String, TemplateConfig> = HashMap::new();
    for cfg in configs {
        cfg_map.insert(cfg.name.clone(), cfg);
    }
    eprintln!(
        "[api] paged registry: {} configs, hot_limit={}, warm_limit={}",
        cfg_map.len(),
        hot_limit,
        warm_limit
    );
    let registry = Arc::new(PagedRegistry::new(docker, cfg_map, hot_limit, warm_limit));

    let state = Arc::new(AppState {
        registry,
        unix_pools: dashmap::DashMap::new(),
        child_fd_pools: dashmap::DashMap::new(),
        lease_locks: dashmap::DashMap::new(),
        pool_cap_per_path,
        max_timeout,
    });

    let tools_state = std::sync::Arc::new(tools_forward::ToolsForwardState::from_env());
    if let Some(u) = tools_state.upstream.as_deref() {
        eprintln!("[api] tools forward upstream = {}", u);
    } else {
        eprintln!(
            "[api] TOOLS_DAEMON_URL unset; /v2/* routes will respond 503 (set it to enable forward)"
        );
    }
    let tools_router = tools_forward::router(tools_state);

    let app = Router::new()
        .route("/health", get(health))
        .route("/templates", get(list_templates))
        .route("/stats", get(stats_handler))
        .route("/metrics", get(metrics_handler))
        .route("/admin/drain", post(admin_drain))
        .route("/admin/build/:name", post(admin_build))
        .route("/exec_hot", post(exec_hot))
        .route("/exec", post(exec_cold))
        .with_state(state)
        .merge(tools_router);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = {
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        use std::os::fd::AsRawFd;
        let val: libc::c_int = 1;
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &val as *const _ as *const _,
                std::mem::size_of_val(&val) as libc::socklen_t,
            )
        };
        if rc != 0 {
            eprintln!(
                "[api] WARN SO_REUSEPORT setsockopt: {}",
                std::io::Error::last_os_error()
            );
        }
        socket.bind(addr)?;
        socket.listen(2048)?
    };
    let instance = std::env::var("INSPECT_API_INSTANCE").unwrap_or_else(|_| "0".into());
    eprintln!("[api] listening on {} (instance {})", addr, instance);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixStream as TokioUnixStream;

    // ---- parse_memory --------------------------------------------------

    #[test]
    fn parse_memory_units() {
        assert_eq!(parse_memory("4g").unwrap(), 4 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory("4GB").unwrap(), 4 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory("1024m").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_memory("2048k").unwrap(), 2048 * 1024);
        assert_eq!(parse_memory("1024").unwrap(), 1024);
        assert_eq!(parse_memory(" 2g ").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_memory_rejects_garbage() {
        assert!(parse_memory("abc").is_err());
        assert!(parse_memory("").is_err());
    }

    // ---- send_frame / recv_frame ---------------------------------------
    //
    // 4-byte u32 BE length prefix + payload. MAX_FRAME = 64 MiB.

    #[tokio::test]
    async fn frame_roundtrip_empty() {
        let (mut a, mut b) = TokioUnixStream::pair().unwrap();
        send_frame(&mut a, b"").await.unwrap();
        let got = recv_frame(&mut b).await.unwrap();
        assert_eq!(got, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn frame_roundtrip_1k() {
        let payload = vec![0xABu8; 1024];
        let (mut a, mut b) = TokioUnixStream::pair().unwrap();
        send_frame(&mut a, &payload).await.unwrap();
        let got = recv_frame(&mut b).await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn frame_recv_rejects_oversize_header() {
        // Manually craft a frame whose header claims > MAX_FRAME.
        let (mut a, mut b) = TokioUnixStream::pair().unwrap();
        use tokio::io::AsyncWriteExt;
        let oversize = (MAX_FRAME as u32 + 1).to_be_bytes();
        a.write_all(&oversize).await.unwrap();
        // Close to make the receiver fail fast.
        drop(a);
        let err = recv_frame(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn frame_recv_eof_on_truncated_payload() {
        let (mut a, mut b) = TokioUnixStream::pair().unwrap();
        use tokio::io::AsyncWriteExt;
        // Header says 100 bytes follow but we only send 5.
        a.write_all(&100u32.to_be_bytes()).await.unwrap();
        a.write_all(b"short").await.unwrap();
        drop(a);
        let err = recv_frame(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    // ---- RegistryState LRU --------------------------------------------

    fn empty_state() -> RegistryState {
        RegistryState {
            runtimes: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    #[test]
    fn touch_lru_appends_new_name() {
        let mut s = empty_state();
        PagedRegistry::touch_lru_locked(&mut s, "a");
        PagedRegistry::touch_lru_locked(&mut s, "b");
        PagedRegistry::touch_lru_locked(&mut s, "c");
        let order: Vec<_> = s.lru.iter().cloned().collect();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn touch_lru_moves_existing_to_back() {
        let mut s = empty_state();
        PagedRegistry::touch_lru_locked(&mut s, "a");
        PagedRegistry::touch_lru_locked(&mut s, "b");
        PagedRegistry::touch_lru_locked(&mut s, "c");
        PagedRegistry::touch_lru_locked(&mut s, "a"); // bump
        let order: Vec<_> = s.lru.iter().cloned().collect();
        assert_eq!(order, vec!["b", "c", "a"]);
    }

    #[test]
    fn touch_lru_idempotent_single_name() {
        let mut s = empty_state();
        for _ in 0..5 {
            PagedRegistry::touch_lru_locked(&mut s, "x");
        }
        assert_eq!(s.lru.len(), 1);
        assert_eq!(s.lru.front().unwrap(), "x");
    }

    // ---- load_templates ------------------------------------------------

    #[test]
    fn load_templates_empty_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let v = load_templates(dir.path()).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn load_templates_reads_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("hello");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(
            sub.join("template.toml"),
            "name = \"hello\"\npool_size = 8\ncontainers = 2\n",
        )
        .unwrap();
        let v = load_templates(dir.path()).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "hello");
        assert_eq!(v[0].pool_size, 8);
        assert_eq!(v[0].containers, 2);
    }

    #[test]
    fn load_templates_skips_dirs_without_toml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("no-toml-here")).unwrap();
        let v = load_templates(dir.path()).unwrap();
        assert!(v.is_empty());
    }

    // ---- pb::Request / Response round-trip ----------------------------

    #[test]
    fn pb_request_roundtrip() {
        let req = pb::Request {
            cmd: "lease".into(),
            lease_count: 8,
            job: Some(pb::Job {
                code: "print(1)".into(),
                timeout: 30,
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = req.encode_to_vec();
        let decoded = pb::Request::decode(&*bytes).unwrap();
        assert_eq!(decoded.cmd, "lease");
        assert_eq!(decoded.lease_count, 8);
        assert_eq!(decoded.job.as_ref().unwrap().timeout, 30);
    }

    #[test]
    fn pb_response_roundtrip() {
        let resp = pb::Response {
            kind: "exec".into(),
            exec: Some(pb::ExecResult {
                stdout: "hi".into(),
                stderr: "".into(),
                exit_code: 0,
                elapsed_ms: 42,
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = resp.encode_to_vec();
        let decoded = pb::Response::decode(&*bytes).unwrap();
        assert_eq!(decoded.kind, "exec");
        let e = decoded.exec.unwrap();
        assert_eq!(e.stdout, "hi");
        assert_eq!(e.exit_code, 0);
        assert_eq!(e.elapsed_ms, 42);
    }
}
