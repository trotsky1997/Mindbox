# inspect-api

Sandbox-as-a-service for executing user-supplied Python code with low latency.
Internal training use; isolation is process-level only (no security sandbox).

## Architecture

```
            ┌─────────────────┐
   client ─►│   api-rust      │   (axum, single binary, port 8000)
            │   :8000         │   loads templates/*/template.toml at boot,
            └────┬────────────┘   starts N hot containers per template,
                 │ unix socket     routes /exec_hot by template name.
                 │ 4-byte len + JSON frame (no HTTP)
       ┌─────────┼─────────────┐
       ▼         ▼             ▼
  ┌────────┐ ┌────────┐    ┌────────┐
  │ worker │ │ worker │ …  │ worker │   inspect-tpl-<name>:latest
  │ -rust  │ │ -rust  │    │ -rust  │   --network none + bind-mount /sockets/
  └───┬────┘ └───┬────┘    └───┬────┘   tokio runtime + std forker thread
      │ socketpair (sync framed)
      ▼ ▼ ▼ ▼ (POOL_SIZE child processes per container)
   ┌──┐ ┌──┐ ┌──┐ ┌──┐
   │py│ │py│ │py│ │py│  each child = fresh Python interpreter, forked from
   └──┘ └──┘ └──┘ └──┘  parent (embedded via PyO3, prewarm CoW-shared).
                       Each child handles up to MAX_REQS requests then exits;
                       parent's forker thread replenishes the pool.
```

## Endpoints (api-rust on :8000)

| Method | Path           | Body / Query                                    | Returns |
|--------|----------------|-------------------------------------------------|---------|
| GET    | /health        | —                                               | summary of templates |
| GET    | /templates     | —                                               | per-template list with idle counts |
| GET    | /stats         | —                                               | aggregate + per-container detail (JSON) |
| GET    | /metrics       | —                                               | Prometheus exposition |
| POST   | /admin/drain   | `{"template":"...","pid":N,"reason":"..."}`    | fans out drain to template's containers |
| POST   | /exec_hot      | `{"template":"...","code":"...","timeout":N,"env":{},"files":{}}` | stdout/stderr/exit_code/elapsed_ms |
| POST   | /exec          | (cold path, currently 503) | — |

Per-container worker-rust also exposes the same /health /stats /admin/drain /exec_hot
routes on its randomly-assigned host port.

## Templates

Each template is a directory under `/opt/inspect-api/templates/`:

```
templates/data-science/
├── template.toml      # config (required)
└── Dockerfile         # optional; if absent, generated from template.toml
```

`template.toml`:

```toml
name = "data-science"          # also the image tag suffix: inspect-tpl-<name>
base_image = "python:3.12-slim"
prewarm = []                   # parent processes import these on boot
extra_pip = ["numpy","pandas"] # pip install but DON'T prewarm-import
pool_size = 16                 # children per container
containers = 4                 # number of hot containers
memory_reservation = "6g"      # docker --memory-reservation (soft)
pids_limit = 4096
engine = "rust"                # rust (default) or python (fallback)
```

Build a template image:
```bash
cd /opt/inspect-api
template-build <name>           # build one
template-build default data-science  # multiple
```

The build script picks the worker binary from `worker-rust/target/release/worker-rust`.
If you change `worker-rust/src/*` or `sandbox_helper.py`, rebuild it first:
```bash
cd /opt/inspect-api/worker-rust && cargo build --release
```

## ENV vars

### api-rust
| Var | Default | Purpose |
|---|---|---|
| `PORT` | 8000 | HTTP port |
| `INSPECT_API_TEMPLATES_DIR` | `/opt/inspect-api/templates` | template scan dir |
| `INSPECT_API_MAX_TIMEOUT` | 60 | max user code timeout (seconds) |

### worker-rust (baked into image via Dockerfile from template.toml)
| Var | Default | Purpose |
|---|---|---|
| `WORKER_PORT` | 8000 | HTTP port (container-internal) |
| `WORKER_POOL_SIZE` | 32 | pre-fork pool size |
| `WORKER_MAX_TIMEOUT` | 60 | hard cap on user code timeout |
| `WORKER_PREWARM_MODULES` | `""` | comma-separated module names to import in parent |
| `WORKER_REUSE_MAX_REQS` | 200 | child respawns after this many requests |
| `WORKER_REUSE_MAX_RSS_MB` | 1024 | child respawns if RSS exceeds |
| `WORKER_MAX_AGE_SECONDS` | 600 | reaper drains children older than this |
| `WORKER_MAX_IDLE_SECONDS` | 120 | reaper drains children idle longer than this |
| `WORKER_REAPER_INTERVAL_SEC` | 5 | reaper scan period |
| `WORKER_MAX_TOTAL_RSS_MB` | 0 (off) | reaper drains oldest if total idle RSS exceeds |
| `WORKER_GC_EVERY_N` | 5 | `gc.collect()` every N requests in each child |


## Wire protocol (api <-> worker)

api-rust connects to each worker container's unix socket
(`/opt/inspect-api/sockets/<tpl>-<idx>-<ns>/worker.sock`) and exchanges
length-prefixed JSON frames. No HTTP between them.

| `_cmd` value | Behavior |
|---|---|
| absent / `"exec"` | run user code (`code`, `timeout`, `env`, `files`) |
| `"health"` | return `{ok, pid, pool_size, idle, prewarm}` |
| `"stats"` | full stats blob |
| `"drain"` | drain children: `{pid?, reason?}` |

Connections are pooled per-socket-path in api-rust (cap
`INSPECT_API_UNIX_POOL_PER_PATH=64`).

## Operations

```bash
/opt/inspect-api/start.sh    # tmux session "inspect-api"
/opt/inspect-api/stop.sh     # kills tmux + removes hot containers
/opt/inspect-api/status.sh   # tmux state + /health + container list
tmux attach -t inspect-api   # see live logs
```

API log goes to `/opt/inspect-api/server.log`.

## Lifecycle invariants

- Each child handles ≤ `WORKER_REUSE_MAX_REQS` requests, then exits.
- After each request the helper restores `cwd`/`os.environ`, removes the
  per-request tmpdir, cancels SIGALRM, and resets the SIGALRM handler.
- Poison checks per request: thread count delta, fd count delta. Any
  positive delta → expire (`_expire: true`) with reason `threads:+N` or
  `fds:A->B`.
- Reaper drops idle children that exceed `MAX_AGE` or `MAX_IDLE`.
  Mechanism: parent drops its socketpair fd, child sees EOF in its
  `recv_frame_fd()`, calls `os._exit(0)`. `SIGCHLD=SIG_IGN` auto-reaps.
- HTTP handler hard-timeout (`request.timeout + 5`s) SIGKILLs the child
  if it wedges. Forker replenishes.
- Memory pressure (when `WORKER_MAX_TOTAL_RSS_MB > 0`): if total idle RSS
  exceeds budget AND pool is >half-idle, the OLDEST idle child is drained
  (one per reaper tick).

## Stats schema

`GET /stats` returns:

```json
{
  "templates": {
    "data-science": {
      "image": "inspect-tpl-data-science:latest",
      "containers": 4,
      "pool_size": 16,
      "concurrency": 64,
      "prewarm": [],
      "aggregate": {
        "idle": 64, "active_approx": 0,
        "requests_total": N, "forks_total": N,
        "kills_hard_timeout": N, "kills_reaper_age": N, "kills_reaper_idle": N,
        "respawns_by_reason": {"max_reqs": N, "idle_age": N, "drain_*": N, ...}
      },
      "containers_detail": [
        { "uptime_sec": N, "idle": N, "active_approx": N,
          "lifetime": { ...same shape as aggregate... },
          "config": { "max_age_sec": ..., "max_idle_sec": ..., ... },
          "children": [{ "pid": N, "age_sec": N, "idle_sec": N,
                         "requests_served": N, "last_rss_mb": N,
                         "state": "idle" }] }
      ]
    }
  }
}
```

## Benchmark baseline (recorded in `bench-baseline.txt`)

Hardware: 48 CPU (Xeon 8582C), 25 GiB RAM, Docker via socket proxy + userns remap.

| Stage | RPS (wrk -t8 -c64 -d30s `print(1)`) | Notes |
|---|---|---|
| Python API + Python prefork | 685 | uvicorn 100% CPU |
| Rust API + Python worker | 750 | API now 13% CPU; fork-rate ~1K |
| Rust API + Rust worker (fork-per-req) | 750 | fork-rate ceiling: ~1K syscall/sec system-wide |
| Rust API + Rust worker REUSE | 5800 | child handles ≤200 reqs before respawn |
| + full lifecycle (gc, reaper, stats) | 5443 | gc.collect every 5 reqs |

## Troubleshooting

**API won't start:** check `server.log` for "load template" warnings. Common cause:
template image not built. Run `template-build <name>`.

**High respawn rate of one reason:** check `/stats`. If `threads:+1` dominates, some
user code is leaking threads. If `fds:...` dominates, file descriptor leak. Either
indicates poisoning code that needs review.

**Latency P99 spike:** check `/stats` `kills_hard_timeout` — if non-zero, some user
code is hanging past `timeout+5s` and forcing parent SIGKILL.

**Empty `/health`:** the tmux session may have died. `status.sh` or
`tmux attach -t inspect-api` to see what happened.

## Files of interest

```
/opt/inspect-api/
├── api-rust/           Rust HTTP front-end (bollard for docker control)
├── worker-rust/        Rust sandbox worker (PyO3 + tokio + axum)
│   └── src/sandbox_helper.py   Python helper run inside each child
├── templates/<name>/   Per-template config + optional Dockerfile
├── template-build.py   Image builder
├── run.sh start.sh stop.sh status.sh
├── bench-baseline.txt  Historical benchmark results
└── server.log          tmux output of the API
```

## E2B-compatible shim (e2b-shim on :8001)

A drop-in [E2B](https://e2b.dev) API server, sitting in front of the hot pool. Lets
unmodified `e2b` Python/JS SDKs use this fleet as their backend.

```
       ┌─────────────────┐                ┌───────────────┐
e2b ─►│   e2b-shim      │ ── /exec_hot ─►│   api-rust    │
SDK    │   :8001         │                │   :8000       │
       └─────────────────┘                └───────────────┘
            │  sandbox fs:
            │  /var/lib/e2b-shim/sandboxes/<sid>/
            ▼
       host filesystem (sandbox-scoped dir per sandbox)
```

### Supported SDK surface

- `Sandbox.create(template=...)`, `.kill()`, `.set_timeout()`, `.get_info()`
- `commands.run("bash -c ...")` — wrapped into `subprocess.run(...)` and dispatched to hot pool
- `commands.list()` — returns empty list (we don't track long-running procs)
- `files.read / write / exists / list / make_dir / rename / remove`
- Connect protocol (HTTP/1.1 POST with envelope framing) — both `application/connect+json`
  and `application/connect+proto` codecs

### File persistence (Option C: write-back protocol)

Hot-pool execution is **process-scoped** (fresh fork child per request, no shared FS).
To make E2B's "files persist across commands" semantic work, we ride a protocol extension:

1. `commands.run(...)` → shim serializes the sandbox's host fs dir into the `/exec_hot` `files` map and sets `persist_changes: true`.
2. Worker child unpacks files into a tmpdir, `chdir`'s there, runs the command, then **scans the tmpdir** before and after exec.
3. Worker returns `output_files` (modified/created text files) and `deleted_files` to the shim.
4. Shim writes the diff back to `/var/lib/e2b-shim/sandboxes/<sid>/`.

Files are classified by NUL-byte sniff on the first 4 KiB:
- text → `output_files: map<string, string>` (raw UTF-8 content)
- binary → `output_files_b64: map<string, string>` (standard base64)

Both are written back to the host fs by the shim. Verified byte-exact roundtrip
for arbitrary binary content (e.g. `bytes(range(256))`). Hot path with
`persist_changes:false` remains a single-digit-ms.

### Multi-sandbox routing

Every E2B SDK request includes an `E2b-Sandbox-Id` header (the SDK already sets this
without any user intervention). The shim reads it and routes to that sandbox's host fs
dir. Verified isolation with 40 concurrent commands across 2 sandboxes / 4 threads —
no cross-contamination.

Fallback order:
1. `E2b-Sandbox-Id` header (set by every E2B SDK)
2. `X-Sandbox-Id` header (manual / curl)
3. Most recently created sandbox (single-sandbox convenience for direct REST calls)

### Sandbox metadata durability

Each `Sandbox.create()` persists a `<sid>.json` to `/var/lib/e2b-shim/registry/`.
On shim startup, all entries are loaded back into the in-memory registry — so a
shim restart preserves existing sandboxes and their host fs dirs. Confirmed with
e2e test: `commands.run()` and `files.read()` against an old sandbox_id succeed
after `pkill -9 shim` + relaunch.

A background sweeper runs every 60s and prunes sandboxes whose `endAt` has
passed. This covers:
- SDKs in debug mode where `sb.kill()` is a no-op
- Crashed clients that never called `kill()`
- Long-running fleets where some sandboxes leak

Disable by deleting `/var/lib/e2b-shim/registry/` before startup if a clean slate
is desired.

### SDK patch (debug-mode kill/timeout)

The upstream E2B SDK short-circuits `kill()` and `set_timeout()` to no-ops when
`debug=True` (its assumption: debug = local-only, nothing to clean up). Since
our shim *is* the local backend, that's exactly the wrong behavior — sandboxes
would never get deleted and timeouts could not be extended.

Run once after install (and after `pip install --upgrade e2b`):

```
./patch-e2b-sdk.sh
```

The script removes the two debug short-circuits from
`e2b/sandbox_{sync,async}/sandbox_api.py` and clears the `.pyc` cache. After
patching, `sb.kill()` → `DELETE /sandboxes/<id>` and `sb.set_timeout(N)` →
`POST /sandboxes/<id>/timeout`.



### Endpoints

```
POST   /sandboxes                         create   (returns SandboxRec)
GET    /sandboxes/:id                     fetch    (SandboxDetail)
DELETE /sandboxes/:id                     kill
POST   /process.Process/Start             stream   (Connect server-stream)
POST   /process.Process/List              unary
POST   /filesystem.Filesystem/{Stat,MakeDir,ListDir,Remove,Move}
GET    /files?path=...                    read
POST   /files?path=...                    write (multipart or raw)
```

Also mirrored under `/envd/*` for SDK variants that prepend `envd/`.

## Inspect-API protocol — file persistence fields

Added to `proto/inspect.proto`:

```proto
message Job {
    ...
    bool persist_changes = 5;
    string persist_root_label = 6;     // reserved; currently unused
}
message ExecResult {
    ...
    map<string, string> output_files = 5;
    repeated string deleted_files = 6;
}
```

`/exec_hot` JSON gains `persist_changes: bool` (default false) and returns `output_files` /
`deleted_files` only when non-empty.

## Service management

```
./start.sh        # starts api-rust + e2b-shim in tmux session "inspect-api"
./stop.sh         # kills tmux session, hot containers, stray shim
./status.sh       # tmux + container + api/shim health summary
INSPECT_API_REPLICAS=N ./start.sh        # multi-instance SO_REUSEPORT (N panes)
INSPECT_API_ENABLE_SHIM=0 ./start.sh     # skip e2b-shim pane
```

## Repo layout (current)

```
/opt/inspect-api/
├── api-rust/                Rust HTTP front-end (bollard + axum)
├── worker-rust/             Embedded-Python sandbox worker
│   └── src/sandbox_helper.py     Python helper run inside each fork child
├── e2b-shim/                E2B-compatible API server
├── template-builder/        Rust template image builder (replaces template-build.py)
├── proto/
│   ├── inspect.proto             api ↔ worker
│   └── envd/{process,filesystem}.proto    E2B envd protocol (vendored)
├── templates/<name>/        Per-template config (template.toml + optional Dockerfile)
├── sockets/                 Unix sockets bind-mounted into worker containers
├── run.sh start.sh stop.sh status.sh
├── grafana-dashboard.json
└── README.md
```
