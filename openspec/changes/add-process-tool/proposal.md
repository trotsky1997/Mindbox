## Why

Harness/bridge code (sandbox MCP, language servers, dev servers, live-smoke
harnesses) needs to drive long-lived processes inside the same tools-rust
runtime where the seven canonical agent tools already run, but the current
`bash` tool is request/response only and cannot model stdin streaming, EOF,
incremental reads, signals, or graceful shutdown of an already-running
process. EFP RFC 0001 already defines the `process` primitive; we need a
backend implementation that bridges harness ↔ tools runtime without forcing
those callers to fall back to host-side `subprocess`.

## What Changes

The mental model: **a session is short-lived (one agent task, minutes-scale)
and a process lives strictly inside a session**. Compared to `bash`'s
one-shot RPC, `process` is the "moderately persistent" tool — multiple
stdin/stdout/wait interactions across a few tool calls, but always
bounded by the lifetime of its owning session. Once the session ends,
every `process_id` it issued is gone.

- Add an eighth tool, `process`, to the `tools-rust` HTTP surface at
  `POST /sessions/:sid/tools/process` with an action-tagged body
  (`start | write | read | signal | wait | stop | list`) per EFP RFC 0001.
- Track session-scoped processes by embedding the process registry inside
  each session's state, with stable `process_id`s, ring-buffered
  stdout/stderr, EOF flags, and an explicit state machine
  (`Spawning → Running → Exited/Terminated → Reaped`).
- Reuse the existing `bash` isolation pipeline (chroot / seccomp / cgroup
  hooks) so `process` does not invent a second sandbox layer.
- Wire process cleanup into `delete_session` and daemon graceful shutdown
  so a session ending — for any reason — leaves no surviving child
  processes and no leaked `process_id` entries.
- Define `process_id` lifetime as strictly **scoped to the owning
  session**: when the session is gone, every id it issued is
  immediately invalid.
- Add a per-session active process cap and per-stream buffer cap to
  bound resource usage; no separate "max process lifetime" knob because
  the session itself is the bound.
- Gate the tool at the `api-rust` `/v2` forward with
  `TOOLS_EXPOSE_PROCESS=0` by default so the eighth tool is **not**
  agent-facing; only trusted callers (harness, debug, live smoke) can
  reach it.
- Document that `process_id`s are session-scoped only — when the session
  ends (explicit delete, idle reap, or daemon restart) every id issued
  by that session is invalid. No checkpoint/restore, no cross-session
  attach.

## Capabilities

### New Capabilities
- `tools-process-tool`: long-lived process management inside a tools-rust
  session — schema, action semantics, lifecycle invariants, isolation
  reuse, gating, and limits.

### Modified Capabilities

## Impact

- **Code**: `tools-rust/src/main.rs` (new request/response types, registry,
  handler, router, session/shutdown cleanup); `api-rust/src/tools_forward.rs`
  + `api-rust/src/main.rs` (forward gating env knob); `README.md`,
  `tests/README.md`, `bench/README.md` for documentation only where
  relevant.
- **Runtime contract**: new HTTP endpoint and new daemon-internal state.
  No change to the existing seven tools or to `api-rust` template /
  network behavior.
- **Deployment**: extra env knobs (`TOOLS_PROCESS_ENABLED`,
  `TOOLS_EXPOSE_PROCESS`, `TOOLS_MAX_PROCESSES_PER_SESSION`,
  `TOOLS_PROCESS_BUFFER_BYTES`, `TOOLS_SESSION_IDLE_REAP_SEC`,
  `TOOLS_PROCESS_KILL_GROUP`); all default conservative so existing
  deployments behave identically until the knob is flipped. No per-process
  lifetime cap — the session is the lifetime bound.
- **PagedRegistry interaction**: not a concern. Because processes never
  outlive their session, normal Hot/Warm/Cold transitions and template
  evictions are correct by construction: when a template's container
  goes away, its sessions go away, and the processes they owned go with
  them. That is the intended failure mode (the agent task fails and
  re-runs), not a leak.
