# Architecture

Mindbox is a multi-process Rust runtime that lets an LLM agent run the
seven canonical coding tools (`read/write/edit/ls/grep/find/bash`) plus a
trusted-only eighth `process` primitive inside short-lived sessions.

This page explains the moving parts: the controller (api-rust), the
session-hosting daemon (tools-rust), how they find each other, and the
lifetimes you should mentally model.

## At a glance

```
┌─────────────────────────────────────────────────────────────────────────┐
│ external client (SDK / curl)                                             │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ HTTP
                                 ▼
            ┌──────────────────────────────────┐
            │ ingress (cloud NAT / host port)  │
            │ → :8000 api-rust                 │
            │ → :8001 e2b-shim                 │
            └────────────────┬─────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ mindbox controller container (ghcr.io/trotsky1997/mindbox:latest)        │
│                                                                          │
│   sshd :2222          (when launched as a dev-instance)                  │
│   api-rust :8000      (mindbox controller — HTTP gateway + scheduler)    │
│   e2b-shim :8001      (E2B SDK compatibility layer)                      │
│                                                                          │
│   /var/run/docker.sock  ──── bollard ───►  host docker daemon            │
└──────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ mindbox-tools docker network (user-defined bridge, 172.18.0.0/16)        │
│                                                                          │
│   inspect-tpl-tools-tools-default      :8002    172.18.0.2  ─┐           │
│   inspect-tpl-tools-tools-python-dev   :8002    172.18.0.3  ─┤           │
│   inspect-tpl-tools-tools-node-dev     :8002    172.18.0.4  ─┘           │
│                                                                          │
│   inside each tools container:                                           │
│     tools-rust daemon                                                    │
│       sessions:  uuid → /sandboxes/<sid>/                                │
│       7 tools + optional 8th process tool                                │
└──────────────────────────────────────────────────────────────────────────┘
```

## Components

### api-rust (controller, `:8000`)

HTTP gateway in front of every tools-rust daemon. Three responsibilities:

1. **Public HTTP surface.** Routes:
   - `POST /v2/sessions` — create a session against a named template
   - `POST /v2/sessions/:sid/tools/:name` — call any of the seven tools or the eighth `process` tool
   - `DELETE /v2/sessions/:sid` — destroy session, reap any owned processes
   - `GET /v2/templates`, `/v2/health`, `/templates`, `/stats`
2. **PagedRegistry.** Lazy-spawns one `inspect-tpl-tools-<name>` container
   per requested template. Tracks them in `Hot` (running, receiving traffic)
   or `Warm` (docker-paused, ready to resume) tiers with LRU eviction.
   Out-of-LRU templates are `Cold` (no container, image only).
3. **Session sticky routing.** When a session is created, api-rust picks
   one daemon URL via PagedRegistry round-robin and stores
   `(template, daemon_url)` keyed by `session_id`. Every subsequent tool
   call on that session goes to the same URL, so multi-instance templates
   never bounce a session between sibling daemons.

### tools-rust (daemon, `:8002` in each tools container)

The actual executor. One process serves N concurrent sessions. Each
session is a `uuid → SessionState` entry holding a sandbox cwd and an
optional process registry.

- The seven file tools (`read/write/edit/ls/grep/find/bash`) run inline
  inside the daemon process. `bash` spawns `/bin/bash -c` once per call.
- The eighth `process` tool (off by default, gated by env) supports
  start/write/read/wait/signal/stop/list with session-scoped lifetime.
- All file paths are resolved against the session cwd; absolute paths
  and `..` traversal are rejected at the API boundary.

### e2b-shim (compatibility, `:8001`)

Translates E2B SDK Connect protocol into mindbox `/v2/sessions/*` calls.
Lets unmodified E2B SDK code talk to mindbox. Bridges:
- `sandbox.commands.run()` → `bash`
- `sandbox.files.read/write()` → `read`/`write`
- `filesystem.list/stat/mkdir/move` → 501 (use bash or the tools API directly)

### template-builder

CLI inside the mindbox image. Reads `templates/<name>/template.toml`,
generates a Dockerfile, runs `docker build`, optionally pushes to a
registry. Invoked manually or by the release-image GitHub workflow.

### mindbox-tools docker network

A user-defined bridge mindbox creates at startup. Every spawned tools
container is attached here; api-rust reaches them by container IP on
`:8002`, with **no host port publishing**. Required because mindbox itself
is a container, and `127.0.0.1` from inside it does not see the host's
loopback published ports.

mindbox best-effort self-attaches its own container ID to the network
on startup. When mindbox runs as a non-container process (bare metal or
inside a dev-instance whose ENTRYPOINT isn't honored) the self-attach
silently no-ops; the network is still functional as long as the host's
docker bridge routes are reachable from where mindbox runs.

Override the network name with `MINDBOX_TOOLS_NETWORK` (default
`mindbox-tools`).

## A request flow, end to end

```
client: POST /v2/sessions {"template":"tools-python-dev"}
   │
   ▼ NAT to controller container :8000
api-rust v2_create_session
   │
   ▼ ToolsForwardState.daemon_for("tools-python-dev")
PagedRegistry.acquire("tools-python-dev")
   │  cold start path (first time this template is touched):
   ├─ docker.create_container(image=inspect-tpl-tools-tools-python-dev,
   │     networking_config.endpoints_config["mindbox-tools"]=default)
   ├─ docker.start_container
   ├─ docker.inspect_container → 172.18.0.X
   ├─ poll http://172.18.0.X:8002/health → ready
   ├─ run [warmup] commands from template.toml (if any)
   └─ insert (TemplateRuntime, Hot, now), daemon_url=http://172.18.0.X:8002
   │
   ▼ forward POST /sessions to daemon_url
tools-rust create_session
   ├─ uuid sid
   ├─ mkdir /sandboxes/<sid>
   └─ sessions.insert(sid, SessionState{cwd})
   │
   ▼ {session_id, cwd}
api-rust: st.sessions.insert(sid, (template, daemon_url))
   │
   ▼ pass through to client
client receives {"session_id":"...","cwd":"..."}
```

Subsequent tool calls (`POST /v2/sessions/<sid>/tools/bash` etc.) look up
the sticky `daemon_url` directly — no PagedRegistry round-robin involved.

## Lifetimes

A clean mental model for what disappears when:

```
template image                  persistent (built once, in registry)
tools template container        process lifetime: spawned on demand,
                                paused/removed by PagedRegistry LRU.
                                Reboots of mindbox lose all containers.

api-rust process                "shell session" — restarts forget all
                                sticky session URLs; clients re-create sessions.

SessionState (in tools-rust)    one agent task, minutes scale.
                                Created on POST /v2/sessions, dropped on
                                DELETE / idle reaper / daemon shutdown /
                                container teardown.

ProcessHandle (8th tool)        strictly ≤ session lifetime. A process
                                cannot outlive the session that owns it.
                                process_id is session-scoped; the daemon
                                does no checkpoint/restore.

session cwd /sandboxes/<sid>/   created on session create, removed on
                                session delete or container teardown.
```

The chain is "process ⊂ session ⊂ tools container ⊂ mindbox controller
lifetime". The eighth tool deliberately lives at the bottom of that chain
so we never need to reason about a process surviving its session.

## Why this shape

A few decisions worth knowing because they shape the deployment story:

- **Docker-out-of-Docker (DooD).** mindbox is itself a container, and it
  talks to the *host* docker daemon to spawn its sibling tools containers.
  This means mindbox needs `/var/run/docker.sock` access — either the
  platform mounts it for you (Volcano MLP dev-instance, normal `-v sock:sock`)
  or you run mindbox as a host process directly. There is no nested docker
  daemon (DinD) inside mindbox.
- **User-defined network, not host ports.** Earlier versions published
  each tools container's `:8002` on a random host loopback port. That
  fails when mindbox is itself in a container, since its `127.0.0.1`
  isn't the host's. Switching to a shared user-defined network was the
  fix and it kept the same dev-container friendly.
- **Session sticky daemon URL.** Round-robin at every request would bounce
  sessions between sibling daemons when `containers > 1`. So api-rust
  picks once at session-create time and reuses the URL.
- **Process is the eighth tool but not exposed by default.** Two gates
  (`TOOLS_PROCESS_ENABLED` on the daemon, `TOOLS_EXPOSE_PROCESS` on the
  forward) keep it off for agent-facing deployments. Harness/bridge
  callers flip both.

For deploying these shapes in practice, see [deployment.md](deployment.md).
For every env knob in the system, see [configuration.md](configuration.md).
For platform quirks (dev-instance ENTRYPOINT, glibc, nested mounts), see
[troubleshooting.md](troubleshooting.md).
