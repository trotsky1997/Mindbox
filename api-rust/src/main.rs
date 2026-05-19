// api-rust v0.3: protobuf over unix socket between api and worker containers.

mod tools_forward;

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::State,
    response::{IntoResponse, Json as AxumJson},
    routing::get,
    Router,
};
use bollard::container::{
    CreateContainerOptions, ListContainersOptions, RemoveContainerOptions, StartContainerOptions,
};
use bollard::models::HostConfig;
use bollard::Docker;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    /// "python-pool" (worker-rust fork-pool, legacy) or "tools" (tools-rust
    /// daemon for the 7-tool dispatcher). Decides which start_*_container
    /// helper acquire() calls during cold start.
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default = "default_base_image")]
    #[allow(dead_code)]
    base_image: String,
    #[serde(default)]
    #[allow(dead_code)]
    extra_pip: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    extra_apt: Vec<String>,
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
fn default_kind() -> String {
    "python-pool".into()
}
fn default_base_image() -> String {
    "python:3.12-slim".into()
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

// ---- runtime state ------------------------------------------------------

/// What a TemplateRuntime is actually backed by. python-pool uses unix
/// socket + child fd pool (worker-rust); tools uses a list of HTTP daemon
/// URLs (tools-rust).
struct TemplateRuntime {
    #[allow(dead_code)]
    container_ids: Vec<String>,
    daemon_urls: Vec<String>,
    rr: AtomicUsize,
}
impl TemplateRuntime {
    /// Returns the next daemon URL round-robin across this template's
    /// container replicas.
    fn pick_daemon_url(&self) -> Option<&str> {
        if self.daemon_urls.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed);
        Some(self.daemon_urls[i % self.daemon_urls.len()].as_str())
    }
}

struct AppState {
    registry: Arc<PagedRegistry>,
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
    /// Iterate over (template_name, TemplateConfig) pairs. Used by
    /// tools_forward to discover which templates are kind="tools".
    fn configs_iter(&self) -> impl Iterator<Item = (&String, &TemplateConfig)> {
        self.configs.iter()
    }

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

        let mut container_ids = Vec::new();
        let mut daemon_urls = Vec::new();
        for i in 0..cfg.containers.max(1) {
            let (cid, url) = start_tools_container(&self.docker, &cfg, i).await?;
            container_ids.push(cid);
            daemon_urls.push(url);
        }

        let rt = Arc::new(TemplateRuntime {
            container_ids,
            daemon_urls,
            rr: std::sync::atomic::AtomicUsize::new(0),
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

    /// Snapshot of all template runtimes including their tier.
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

/// Spawn a tools-rust daemon container for the given template. The image
/// must be tagged `inspect-tpl-tools-<cfg.name>:latest` (template-builder
/// `kind="tools"` adds the "tools-" prefix). The container exposes 8002 to
/// a random host port; we read that back via docker inspect and return it
/// as the daemon URL the api-rust /v2 forward should hit.
async fn start_tools_container(
    docker: &Docker,
    cfg: &TemplateConfig,
    idx: usize,
) -> Result<(String, String)> {
    let tag = format!("inspect-tpl-tools-{}:latest", cfg.name);
    if !docker_image_exists(docker, &tag).await? {
        return Err(anyhow!(
            "template {} tools image {} not built (try template-build {})",
            cfg.name,
            tag,
            cfg.name
        ));
    }

    let mem = parse_memory(&cfg.memory_reservation)?;

    let mut labels = HashMap::new();
    labels.insert("inspect-api-hot".to_string(), "1".to_string());
    labels.insert("inspect-tpl".to_string(), cfg.name.clone());
    labels.insert("inspect-kind".to_string(), "tools".to_string());

    // Map container :8002 → random host port. host_ip="127.0.0.1" keeps the
    // port loopback-only (this is api-rust → daemon on the same host).
    let mut port_bindings: HashMap<String, Option<Vec<bollard::models::PortBinding>>> =
        HashMap::new();
    port_bindings.insert(
        "8002/tcp".to_string(),
        Some(vec![bollard::models::PortBinding {
            host_ip: Some("127.0.0.1".into()),
            host_port: Some("".into()), // empty → docker picks a free port
        }]),
    );

    let mut exposed_ports: HashMap<String, HashMap<(), ()>> = HashMap::new();
    exposed_ports.insert("8002/tcp".to_string(), HashMap::new());

    let config = bollard::container::Config::<String> {
        image: Some(tag),
        labels: Some(labels),
        exposed_ports: Some(exposed_ports),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings),
            memory_reservation: Some(mem),
            cpu_shares: Some(1024),
            pids_limit: Some(cfg.pids_limit),
            oom_score_adj: Some(500),
            security_opt: Some(vec!["no-new-privileges".into()]),
            // tools container needs network for /v2 forward over loopback;
            // user code inside (via tools/bash) shouldn't see external
            // network. Bridge default — operators can override.
            network_mode: Some(
                std::env::var("TOOLS_CONTAINER_NETWORK_MODE").unwrap_or_else(|_| "bridge".into()),
            ),
            ..Default::default()
        }),
        ..Default::default()
    };

    let name = format!(
        "mindbox-tpl-tools-{}-{}-{}",
        cfg.name,
        idx,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let created = docker
        .create_container(
            Some(CreateContainerOptions {
                name: name.clone(),
                ..Default::default()
            }),
            config,
        )
        .await
        .context("create tools container")?;
    docker
        .start_container(&created.id, None::<StartContainerOptions<String>>)
        .await
        .context("start tools container")?;

    // Inspect to learn the assigned host port.
    let inspected = docker
        .inspect_container(&created.id, None)
        .await
        .context("inspect tools container")?;
    let host_port = inspected
        .network_settings
        .as_ref()
        .and_then(|ns| ns.ports.as_ref())
        .and_then(|p| p.get("8002/tcp").cloned().flatten())
        .and_then(|v| v.into_iter().next())
        .and_then(|pb| pb.host_port)
        .ok_or_else(|| anyhow!("tools container has no published 8002/tcp port"))?;
    let daemon_url = format!("http://127.0.0.1:{}", host_port);

    // Wait for /health to return 200.
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("reqwest client")?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last_err: Option<String> = None;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "tools daemon at {} not ready in 30s: {}",
                daemon_url,
                last_err.unwrap_or_else(|| "no probe attempts".into())
            ));
        }
        match client.get(format!("{}/health", daemon_url)).send().await {
            Ok(r) if r.status().is_success() => {
                eprintln!("[paged] tools daemon ready at {}", daemon_url);
                return Ok((created.id, daemon_url));
            }
            Ok(r) => last_err = Some(format!("HTTP {}", r.status())),
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
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
    let mut names: Vec<&String> = state.registry.configs.keys().collect();
    names.sort();
    let tpls: Vec<_> = names
        .iter()
        .map(|n| {
            let cfg = &state.registry.configs[*n];
            serde_json::json!({
                "name": n,
                "base_image": cfg.base_image,
                "containers": cfg.containers,
            })
        })
        .collect();
    AxumJson(serde_json::json!({ "templates": tpls }))
}

async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let s = state.registry.state.lock().await;
    let mut hot = Vec::new();
    let mut warm = Vec::new();
    for (name, (_, tier, _)) in s.runtimes.iter() {
        match tier {
            Tier::Hot => hot.push(name.clone()),
            Tier::Warm => warm.push(name.clone()),
        }
    }
    hot.sort();
    warm.sort();
    AxumJson(serde_json::json!({
        "hot": hot,
        "warm": warm,
        "configs_total": state.registry.configs.len(),
    }))
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
    let _max_timeout: u32 = std::env::var("INSPECT_API_MAX_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let _pool_cap_per_path: usize = std::env::var("INSPECT_API_UNIX_POOL_PER_PATH")
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

    let state = Arc::new(AppState { registry });

    let tools_state = std::sync::Arc::new(tools_forward::ToolsForwardState::new(state.clone()));
    let tools_template_count = state
        .registry
        .configs_iter()
        .filter(|(_, c)| c.kind == "tools")
        .count();
    if let Some(u) = tools_state.fallback_daemon.as_deref() {
        eprintln!(
            "[api] tools forward: {} tools template(s) configured + fallback daemon {}",
            tools_template_count, u
        );
    } else if tools_template_count > 0 {
        eprintln!(
            "[api] tools forward: {} tools template(s) configured (lazy-spawned on demand)",
            tools_template_count
        );
    } else {
        eprintln!("[api] no tools templates configured and TOOLS_DAEMON_URL unset; /v2/* will 503");
    }
    let tools_router = tools_forward::router(tools_state);

    let app = Router::new()
        .route("/health", get(health))
        .route("/templates", get(list_templates))
        .route("/stats", get(stats_handler))
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
}
