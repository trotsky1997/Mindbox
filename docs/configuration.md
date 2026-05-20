# Configuration reference

Every env knob the runtime reads, with default and meaning. Knobs are
grouped by which component reads them; some are read by more than one.

Set knobs the same way you'd set any container env: `-e KEY=value` on
`docker run`, the `env:` block in a compose/k8s manifest, or `export`
before running the binaries directly.

## Controller (api-rust :8000)

| Env | Default | What it does |
|---|---|---|
| `PORT` | `8000` | HTTP listen port. |
| `INSPECT_API_TEMPLATES_DIR` | `/opt/inspect-api/templates` | Where to scan for `template.toml` files at startup. |
| `INSPECT_API_HOT_LIMIT` | `16` | PagedRegistry Hot tier capacity. Templates over this are LRU-evicted to Warm. |
| `INSPECT_API_WARM_LIMIT` | `hot_limit * 4` | Warm (docker-paused) tier capacity. Templates over this are removed entirely (Cold). |
| `INSPECT_API_SOCKETS_DIR` | `/opt/inspect-api/sockets` | Legacy unix-socket dir for tools daemon IPC. Most deployments leave this alone. |
| `INSPECT_API_INSTANCE` | `0` | Tag included in startup log for multi-replica deployments. |
| `INSPECT_API_MAX_TIMEOUT` | `60` | Reserved (deprecated; bash tool now caps timeout itself). |
| `INSPECT_API_UNIX_POOL_PER_PATH` | `64` | Reserved (legacy worker-pool knob). |

## Forward / template networking

| Env | Default | What it does |
|---|---|---|
| `MINDBOX_TOOLS_NETWORK` | `mindbox-tools` | Name of the user-defined docker bridge mindbox creates and attaches all tools containers to. |
| `TOOLS_CONTAINER_NETWORK_MODE` | unset | Override the `host_config.network_mode` for spawned tools containers. Set to e.g. `host` only if you really know your network model. Leaving it unset uses the `mindbox-tools` bridge attach. |
| `TOOLS_DAEMON_URL` | unset | Fallback for legacy single-daemon deployments. If set and no template config matches, `/v2/...` forwards here. Almost always unset in production. |
| `MINDBOX_TEMPLATE_REGISTRY` | `ghcr.io/trotsky1997/mindbox` | Registry the entrypoint's `ensure-templates` script pulls `tpl-<name>:latest` from. |
| `MINDBOX_SKIP_TEMPLATE_ENSURE` | unset | Skip the entrypoint's image-pull step. Useful for shim-only deployments that don't need template images locally. |

## tools-rust daemon (:8002 inside each tools container)

| Env | Default | What it does |
|---|---|---|
| `TOOLS_PORT` | `8002` | HTTP listen port inside the tools container. |
| `TOOLS_SANDBOX_ROOT` | `/sandboxes` | Parent directory under which each session cwd is created (`<root>/<sid>/`). |
| `TOOLS_ISOLATION` | unset | Comma-separated subset of `{chroot, seccomp, cgroup}`. Each layer is applied to bash subprocesses; silently no-ops if the host kernel doesn't support it. See `tools-rust/src/main.rs` for what each layer entails. |
| `TOOLS_SESSION_IDLE_REAP_SEC` | `3600` | Background reaper evicts sessions whose `last_touched` is older than this and reaps all their child processes. |
| `HOSTNAME` | n/a | Used by api-rust at startup to self-attach the controller container to `MINDBOX_TOOLS_NETWORK`. Read-only — set by docker. |
| `RUST_LOG` | `info` | Standard `tracing-subscriber` env filter. |

## Eighth `process` tool (trusted-only)

Two gates, **both must be on** to expose the tool to clients:

| Env | Where read | Default | What it does |
|---|---|---|---|
| `TOOLS_PROCESS_ENABLED` | tools-rust daemon | `0` | When unset, the route `/sessions/:sid/tools/process` returns 404 unconditionally. Daemon kill switch. |
| `TOOLS_EXPOSE_PROCESS` | api-rust forward | `0` | When unset, `/v2/sessions/:sid/tools/process` returns `403 process_forbidden` without contacting the daemon. Network-level gate. |

Resource caps and behavior knobs:

| Env | Default | What it does |
|---|---|---|
| `TOOLS_MAX_PROCESSES_PER_SESSION` | `32` | Reject `start` with `429` when the session already owns this many processes. |
| `TOOLS_PROCESS_BUFFER_BYTES` | `262144` (256 KiB) | Per-stream ring-buffer size. Older bytes are dropped on overflow; the next `read` sets `truncated: true`. |
| `TOOLS_PROCESS_KILL_GROUP` | `1` | When 1, `signal`/`stop` deliver to the process group (`kill(-pgid, sig)`); 0 sends only to the leader pid. |
| `TOOLS_PROCESS_FORCE_UNSUPPORTED` | `0` | Diagnostic. When 1, every process action returns `501 process_unsupported`. Use to flag environments where process support is known-broken. |

The 8th-tool design is detailed in `openspec/specs/tools-process-tool/spec.md`.

## E2B compat (e2b-shim :8001)

| Env | Default | What it does |
|---|---|---|
| `E2B_SHIM_PORT` | `8001` | HTTP listen port. |
| `E2B_SHIM_UPSTREAM` | `http://127.0.0.1:8000` | Where to forward `commands.run / files.read / files.write` etc. Usually the colocated api-rust. |
| `E2B_SHIM_API_KEY` | unset | If set, incoming requests must include this E2B-style API key. |
| `E2B_SHIM_DEFAULT_SANDBOX` | unset | Optional default sandbox id used when an SDK client doesn't supply one. |
| `E2B_SHIM_BASE_TEMPLATE` | `tools-default` | Template to use when the E2B SDK asks for its default `base` template. |
| `E2B_SHIM_VOLUMES_ROOT` | `/var/lib/e2b-shim/volumes` | Host directory where shim-managed E2B volume content is stored. |

## TOS / S3 object storage

Used by three features:
- e2b-shim sandbox snapshot endpoint (`POST /sandboxes/:id/snapshot`).
- `scripts/backup-image-to-tos.sh` backup tarball uploader.
- S3-backed volumes — same envs serve as the credential fallback when a
  `POST /volumes` body sets `backend: "s3"` without per-volume creds.

| Env | Default | What it does |
|---|---|---|
| `TOS_BUCKET` | unset (feature disabled when empty) | Bucket name for sandbox snapshots, backup tarballs, **and** the default bucket for S3 volumes when neither the volume body nor `S3_VOLUME_DEFAULT_BUCKET` sets one. |
| `TOS_S3_ENDPOINT` | unset | S3-compatible endpoint URL. Required for S3 volumes if the body doesn't specify `s3.endpoint`. |
| `TOS_REGION` | `cn-beijing` | Region for the S3 client. |
| `TOS_ACCESS_KEY` | unset | Access key. Required for S3 volumes when no per-volume `s3.accessKey` is supplied. |
| `TOS_SECRET_KEY` | unset | Secret key. Same fallback semantics as `TOS_ACCESS_KEY`. |

## S3 volume backend

Additional knobs for the S3 volume backend (independent of snapshot/backup):

| Env | Default | What it does |
|---|---|---|
| `S3_VOLUME_DEFAULT_BUCKET` | unset | Operator default bucket used when `POST /volumes` body has `backend: "s3"` but no `s3.bucket`. Overrides `TOS_BUCKET` for volumes. |
| `S3_VOLUME_DEFAULT_PREFIX` | unset | Operator default prefix. The resulting volume's S3 prefix becomes `<S3_VOLUME_DEFAULT_PREFIX>/<volume_id>` so multiple volumes don't collide. When unset, the volume prefix defaults to just the `volume_id`. |

S3 volume credential resolution chain (per-request, evaluated at
`POST /volumes` time):

1. Per-volume body: `s3.accessKey` + `s3.secretKey`.
2. Env: `TOS_ACCESS_KEY` + `TOS_SECRET_KEY`.

If neither layer resolves both credentials, `POST /volumes` returns
`400 { code: "s3_unconfigured" }` and creates no volume record.

See [`../openspec/changes/add-s3-volume-backend/proposal.md`] for the full
contract (consistency model, write-back semantics, deletion behavior).

## template-builder CLI

Used both as a CLI (`template-build <name>`) and when `ensure-templates`
falls back to local build.

| Env | Default | What it does |
|---|---|---|
| `PIP_INDEX_URL` | upstream pypi.org | Override for the `pip install` step inside template image builds. Set to a mirror (e.g. `https://mirrors.ivolces.com/pypi/simple/`) on networks where pypi is slow. |
| `PIP_TRUSTED_HOST` | `pypi.org` | Goes with `PIP_INDEX_URL` when its TLS is custom. |
| `NPM_REGISTRY_URL` | mirror derived from `PIP_TRUSTED_HOST` | npm/bun registry to bake into `tools-node-dev` images. |
| `REGISTRY_HOST` | unset | Image registry hostname for `--push`. |
| `REGISTRY_NAMESPACE` | unset | Namespace under the registry. |
| `REGISTRY_USER`, `REGISTRY_PASSWORD` | unset | Login creds for `docker push`. |

## Volcano MLPlatform SDK (deployment-side)

Not read by mindbox itself, but needed by `ops/devbox/*.py`:

| Env | Default | What it does |
|---|---|---|
| `VOLC_ACCESS_KEY_ID` | n/a | AK for the Volcano SDK. From the 火山引擎 → 访问控制 → API访问密钥 page. |
| `VOLC_SECRET_ACCESS_KEY` | n/a | SK for the Volcano SDK. |
| `VOLC_REGION` | `cn-beijing` | Region for MLPlatform API calls. |

## Process model summary

What flows where, expressed as env defaults:

```
client → :8000 api-rust ─► docker daemon ─► tools containers each running
                                            tools-rust on :8002 (per
                                            container, not host-published).
client → :8001 e2b-shim → :8000 api-rust  (E2B SDK compat path)
```

For deeper architecture, see [architecture.md](architecture.md).
