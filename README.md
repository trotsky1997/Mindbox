# Mindbox

A 7-tool agent sandbox runtime in Rust. Sessions live in cheap cwd
directories on a daemon; each session can call `read`, `write`, `edit`,
`ls`, `grep`, `find`, `bash` — the 7 built-in tools an LLM coding agent
typically needs. No Python interpreter pool, no fork prewarm, no
SCM_RIGHTS fd-passing.

## Architecture

```
client (HTTP / e2b SDK)
        │
        ▼
   api-rust :8000  ─────────────────────────► tools-rust daemon
   (PagedRegistry:                            (inside per-template container)
    lazy-spawn tools                            ├ /sessions       (CRUD)
    template containers,                        ├ /tools/read
    hot/warm/cold LRU,                          ├ /tools/write
    template name → daemon URL                  ├ /tools/edit
    + session sticky)                           ├ /tools/ls
                                                ├ /tools/grep
   e2b-shim :8001                               ├ /tools/find
   (E2B SDK compat: sandboxes,                  └ /tools/bash
    commands.run, files read/write)
```

* **api-rust** — HTTP entry on :8000. PagedRegistry lazy-spawns a
  tools-rust container per template on first use, manages hot/warm/cold
  LRU tiers, and reverse-proxies `/v2/sessions/*` calls to the right
  daemon.
* **e2b-shim** — E2B SDK compatibility layer on :8001. SDK clients use
  it as a drop-in cloud API; `commands.run()`, `files.read/write()`
  forward to api-rust `/v2`. `filesystem.list/stat/mkdir/move` return
  501 (use the tools API directly or `commands.run` with bash).
* **tools-rust** — the daemon that lives inside each template container.
  Single tokio multi-thread process serves N concurrent sessions; tools
  run as inline Rust functions, `bash` spawns a subprocess.
* **template-builder** — CLI baked into the mindbox image. Reads
  `templates/<name>/template.toml`, generates a Dockerfile, runs
  `docker build`, optionally pushes to a registry.

## Templates

Each template is a docker image containing the tools-rust daemon and
whatever toolchain the agent needs. Configuration lives in
`templates/<name>/template.toml`.

Shipped:
* `tools-default` — debian-slim with bash/ripgrep/fd-find/coreutils
* `tools-python-dev` — `python:3.12-slim` + git + uv/pytest/rich
* `tools-node-dev` — `node:20-slim` + git

Image tag: `inspect-tpl-tools-<name>:latest`. Build a new template with
`template-build <name>` (run from the mindbox image, with templates/
mounted).

### Template startup warmup

Templates may define startup warmup commands. api-rust runs them once per newly
started tools container after `/health` succeeds and before the template becomes
Hot in `PagedRegistry`:

```toml
[warmup]
commands = [
  "python - <<'PY'\nimport pytest, rich\nPY",
]
timeout_secs = 30
```

Warmup uses a transient tools-rust session and calls the `bash` tool with each
command. The transient session is deleted afterwards. Commands run as separate
`bash -c` invocations, so combine commands when shell state must persist.
Warmup reruns when a container is newly started from Cold; Docker pause/unpause
from Warm to Hot keeps the same warmed container and does not rerun warmup. This
is container/page-cache warmup, not fork/CRIU process prewarm.

## Quick start

The published image contains the controller binaries (`api-rust`, `e2b-shim`)
and the `template-build` CLI plus default `templates/` config. On startup
mindbox makes sure every configured template has an
`inspect-tpl-tools-<name>:latest` image present on the host docker daemon:

1. Skip when the tag already exists locally.
2. Try `docker pull` from `$MINDBOX_TEMPLATE_REGISTRY/tpl-<name>:latest`
   (defaults to `ghcr.io/trotsky1997/mindbox`).
3. Fall back to local `template-build <name>` if the pull does not resolve.

No manual `docker pull` / `docker tag` step is required:

```bash
docker run -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/trotsky1997/mindbox:latest

# Use the API
curl -s -X POST http://127.0.0.1:8000/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}'
# → {"session_id":"...","cwd":"/sandboxes/..."}

curl -s -X POST http://127.0.0.1:8000/v2/sessions/<sid>/tools/bash \
  -H 'Content-Type: application/json' \
  -d '{"command":"echo hi"}'
```

Override `MINDBOX_TEMPLATE_REGISTRY` to point at a private registry, or set
`MINDBOX_SKIP_TEMPLATE_ENSURE=1` for shim-only deployments without a docker
socket. Mount a custom `templates/` directory at
`/opt/inspect-api/templates` to override the bundled defaults.

### Tools network

mindbox spawned tools containers join a shared user-defined docker network
(default name `mindbox-tools`, overridable via `MINDBOX_TOOLS_NETWORK`) and
api-rust talks to them by container IP on port 8002. mindbox best-effort
self-attaches to that network at startup, so the same single `docker run`
works both on a normal host and inside a dev-container where the host
loopback is invisible from the mindbox container. No `-p 8002` publishing is
needed for the tools containers.

## Tool API

All seven tools share the same shape — `POST /v2/sessions/:sid/tools/:name`
with a JSON body. See `tools-rust/src/main.rs` for the exact request/
response structs. Quick reference:

| Tool   | Body                                                                | Returns                                  |
|--------|---------------------------------------------------------------------|------------------------------------------|
| read   | `{path,offset?,limit?}`                                             | `{content,bytes}` (UTF-8 lossy)           |
| write  | `{path,content}`                                                    | `{bytes}`                                 |
| edit   | `{path,edits:[{oldText,newText}]}`                                  | `{replacements}` / 400 on ambiguous      |
| ls     | `{path?,limit?}`                                                    | `{entries:[{name,kind,size}]}`            |
| grep   | `{pattern,path?,glob?,ignoreCase?,literal?,context?,limit?}`        | `{matches/files/counts,walked,truncated}` |
| find   | `{pattern,path?,limit?}`                                            | `{paths,walked,truncated}`                |
| bash   | `{command,timeout?}`                                                | `{stdout,stderr,exit_code,timed_out}`     |

Compatibility: legacy Mindbox fields are still accepted as deprecated aliases:
`bash.cmd`, `edit.old_string/new_string/replace_all`, `grep.output_mode/max_files`,
and `find.max_results`.

Path validation: absolute paths and `..` traversal are rejected at the
API boundary; everything is resolved relative to the session cwd.

### Process tool (eighth, trusted-only)

`POST /v2/sessions/:sid/tools/process` exposes a session-scoped persistent
process primitive that bridges harness/SDK callers (sandbox MCP, language
servers, dev-server smoke harnesses) to children running inside the
session sandbox. It is **not** part of the seven agent-facing tools and
is gated off by default:

- Daemon kill switch: `TOOLS_PROCESS_ENABLED=1` on `tools-rust`.
- Forward gate: `TOOLS_EXPOSE_PROCESS=1` on `api-rust`. Without it the
  `/v2/.../tools/process` route returns `403 process_forbidden`
  without contacting the daemon.

Shape (action-tagged per EFP RFC 0001):

```json
{"action":"start", "command":"/bin/sh", "args":["-c","echo hi"]}
{"action":"read",  "process_id":"...", "encoding":"utf-8|base64", "timeout_sec":0.5}
{"action":"write", "process_id":"...", "input":"...", "eof":true}
{"action":"signal","process_id":"...", "signal":"SIGTERM"}
{"action":"wait",  "process_id":"...", "timeout_sec":30}
{"action":"stop",  "process_id":"...", "timeout_sec":5}
{"action":"list"}
```

Lifetime contract: `process_id` is **session-scoped**. When the owning
session is gone — explicit `DELETE /v2/sessions/:sid`, daemon shutdown,
or the idle reaper (`TOOLS_SESSION_IDLE_REAP_SEC`, default 1h) — every
id it issued is immediately invalid. No checkpoint/restore, no
cross-session attach. Per-session limits: `TOOLS_MAX_PROCESSES_PER_SESSION`
(default 32) and `TOOLS_PROCESS_BUFFER_BYTES` (default 256 KiB) cap the
ring buffer for each of stdout/stderr.

## Isolation (opt-in)

`TOOLS_ISOLATION` env on the daemon picks any subset of
`{chroot, seccomp, cgroup}`. All default off; layers silently no-op
when the host environment doesn't support them (e.g. cgroup v2 not
mounted, no `CAP_SYS_ADMIN`).

* **chroot** — bash subprocess chroots into a shared sub-rootfs the
  daemon prepares once (bash + common coreutils + ldd-resolved libs).
* **seccomp** — bash subprocess gets `PR_SET_NO_NEW_PRIVS` + a BPF
  filter that denies a curated list of privilege-escalation / namespace
  / OOB-IO syscalls (mount, ptrace, setuid family, capset, bpf, etc.).
* **cgroup** — per-session cgroup v2 with memory.max / cpu.max /
  pids.max defaults.

## Workspace

```
api-rust/         HTTP entry + PagedRegistry + /v2 forward
e2b-shim/         E2B SDK compat layer
template-builder/ template.toml → docker image CLI
tools-rust/       The actual 7-tool daemon
templates/        Template configurations (one .toml per template)
proto/envd/       E2B SDK Connect protobuf definitions
.github/workflows/
  ci.yml          cargo fmt + clippy + test on every push
  release-image.yml mindbox image + all tpl-tools-* images → ghcr
```

## License

Apache-2.0. See LICENSE.
