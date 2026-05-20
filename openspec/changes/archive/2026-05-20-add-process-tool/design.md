## Context

`tools-rust` today exposes seven stateless tools (`read/write/edit/ls/grep/find/bash`)
backed by a `DashMap<sid, cwd>` session model. Sessions are cheap (mkdir +
hashmap insert), tools are short-lived (`bash` is a single `bash -c`),
and there is no per-process state in the daemon. EFP RFC 0001 defines a
`process` primitive that callers (sandbox MCP, language servers, dev
servers, live-smoke harness) need so they can run "moderately
persistent" processes — multiple stdin/stdout/wait interactions, but
strictly inside the lifetime of one agent task.

The mental model is essential to keep this design small:

- A **session** lives for one agent task. Minutes scale. Created on
  request, deleted (explicitly or by idle reaper) when the task ends.
- A **process** is owned by exactly one session and **MUST NOT outlive
  it**. It is more persistent than `bash` (which is one RPC) but still
  short by absolute standards — a handful of interactions, then gone
  with the session.

That bound is what lets the design be simple: no checkpoint/restore,
no cross-session attach, no PagedRegistry awareness, no "max process
lifetime" knob.

Constraints carried in from the existing codebase:

- `tool_bash` already wires chroot + seccomp + cgroup `pre_exec` hooks via
  `Command::pre_exec` and `apply_bash_isolation`. The new tool MUST reuse,
  not duplicate, that.
- `AppState.sessions: DashMap<String, PathBuf>` is the only session
  store. `delete_session` currently removes the cwd and the cgroup but
  knows nothing about live children — that becomes a bug the moment
  the eighth tool exists.
- The daemon does no graceful shutdown today; `tini` reaps zombies on
  container teardown. With session-scoped processes we still want
  explicit drain-on-shutdown so stdout/stderr buffers and exit codes
  are consistent inside the session that observes them.
- `api-rust` is a transparent JSON forward; gating is done by route
  filtering, not body inspection. The new tool's "trusted-only" flag
  belongs at the forward, not at the daemon's HTTP boundary.
- The runtime contract is single-daemon-process: no checkpoint/restore,
  no cross-restart `process_id` persistence (consistent with the
  earlier decision against CRIU on this codebase).

## Goals / Non-Goals

**Goals:**

- Implement the EFP RFC 0001 `process` action set inside `tools-rust`
  with one HTTP route, one schema, and a session-embedded registry.
- Make `process` reuse `bash`'s isolation pipeline so the sandbox layer
  has exactly one definition.
- Guarantee that ending a session (explicit delete, idle reap, daemon
  shutdown, or container loss) leaves no surviving child processes and
  no leaked `process_id` entries.
- Bound resource usage at the session granularity: per-session active
  process cap, per-stream ring buffer, conservative defaults.
- Gate the tool so agents do not see it by default; only trusted
  callers (`TOOLS_EXPOSE_PROCESS=1` at the forward) can reach it.
- Define `process_id` lifetime as session-scoped; once the session is
  gone, the id is invalid.

**Non-Goals:**

- Checkpoint/restore (CRIU) or any cross-session/cross-restart process
  survival.
- Long-lived daemon-spanning services hosted via `process`. If a use
  case needs "this should keep running after my agent task ends," it
  belongs in a different mechanism (a dedicated container, host
  service, etc.), not inside an agent session.
- PTY/TTY allocation, terminal resize, or interactive shell semantics
  beyond raw stdin/stdout/stderr byte pipes.
- A new isolation layer separate from `bash`'s; `process` is gated
  behind the same `TOOLS_ISOLATION` knob.
- PagedRegistry awareness of active processes. Because a process
  cannot outlive its session, and a session cannot outlive its
  template container, normal Hot/Warm/Cold eviction is correct by
  construction.
- A per-process "max lifetime" cap. The session is the bound.
- Streaming HTTP / SSE / chunked responses; `read` is plain JSON
  request/response with server-side cursors.
- Cross-session shared processes. A `process_id` belongs to exactly
  one `sid`.

## Decisions

### Action-tagged single endpoint, not seven routes

One `POST /sessions/:sid/tools/process` with `action` in the body, not
seven sibling routes. Rationale: EFP RFC 0001 already defines `process`
as one primitive with internal action verbs; matching that on the wire
keeps the schema 1:1 with the RFC and makes future RFC revisions a
single touch point. The handler is a small `match req.action` dispatch
into typed inner functions.

Alternative considered: seven routes (`tools/process/start` etc.).
Rejected because it doubles the surface for no behavior gain and
diverges from the RFC.

### Process registry lives inside the session

```rust
struct SessionState {
    cwd: PathBuf,
    last_touched: AtomicInstant,
    processes: DashMap<String /* pid */, Arc<ProcessHandle>>,
}

struct ProcessHandle {
    pid: u32,
    cmd_line: String,
    started_at: Instant,
    state: tokio::sync::Mutex<ProcessState>,
    stdin: tokio::sync::Mutex<Option<tokio::process::ChildStdin>>,
    stdout: Arc<RingBuffer>,           // bounded, with read cursor
    stderr: Arc<RingBuffer>,           // bounded, with read cursor
    cancel: tokio_util::sync::CancellationToken,
    wait_notify: tokio::sync::Notify,  // signaled when state → terminal
}

enum ProcessState {
    Running,
    Exited { code: i32 },
    Terminated { signal: i32 },
    Reaped { code: Option<i32>, signal: Option<i32> },
}
```

`AppState` upgrades `DashMap<String, PathBuf>` to
`DashMap<String, Arc<SessionState>>`. Processes are not a top-level
collection — they're a member of the session struct, so dropping the
session drops the registry, which drops the handles, which fires the
cancel token and reaps the children.

Background tasks per process (spawned inside the session):

- one `tokio::spawn` per stream draining child stdout/stderr into the
  ring buffer until EOF
- one `tokio::spawn` `child.wait()` that flips state to terminal,
  notifies `wait_notify`, closes the buffers' write side

Rationale for session-embedded vs top-level: the session is the
lifetime anchor, and embedding makes that anchor explicit in the type
system. There is no legitimate caller path that finds a `pid` without
first finding its `sid`, so the flat alternative would only add a
collection to keep in sync.

### Server-side read cursor

Per `(pid, stream)` the buffer tracks how many bytes the caller has
already consumed. `read` returns the next slice and advances the
cursor. EOF flags transition once the drain task closes the writer
side AND the cursor reaches the buffer tail.

Rationale: simpler than asking callers to track offsets and survives
out-of-order reads from different network paths to the same process.

Alternative considered: client-supplied `offset`. Rejected because the
ring buffer drops old bytes on overflow; offsets stop being globally
meaningful.

### Ring buffer with truncation flag

`TOOLS_PROCESS_BUFFER_BYTES` (default `262144`, i.e. 256 KiB) caps each
stream. On overflow the oldest bytes are dropped and a
`truncated_total_bytes` counter increments. The next `read` returns
`truncated:true` and exposes the byte count dropped since last read.

Rationale: a process tool whose lifetime is bounded by an agent task
does not need megabytes of buffering. 256 KiB covers normal LSP /
test-runner / build-tool output for the handful of interactions a
session sees; overflow is recoverable via the truncation flag.

### Reuse `bash` isolation pipeline

`process::start` SHALL build its `tokio::process::Command` through the
existing `apply_bash_isolation`/cgroup hooks already used by
`tool_bash`. We extract those into a small helper
`build_sandboxed_command(state, sid, cmd, args, env, cwd_override)`.
`bash` becomes a thin caller of that helper too, so the code path is
single-sourced.

Rationale: two isolation pipelines guarantee they drift. Verified by
inspecting `tool_bash` today — it already inlines the only logic we
need.

### Process group kill by default

Each child is started with `setsid()` (via `pre_exec`) so it lives in
its own process group. Stop, signal, delete-session, and shutdown all
default to `kill(-pgid, sig)`. `TOOLS_PROCESS_KILL_GROUP=0` reverts
to `kill(pid, sig)`.

Rationale: dev servers and language servers spawn helpers. Killing
only the leader leaves orphans for tini to reap, which is correct but
opaque to callers because the orphans keep emitting output and
holding ports.

### Session-scoped, non-persistent `process_id`

UUID simple form, never persisted, never reused across sessions or
daemon restarts. No on-disk state file. When the owning session is
deleted (explicitly, by idle reap, by daemon shutdown, or because the
template container went away), every `process_id` it issued becomes
immediately invalid (`404`).

Rationale: persistence implies checkpoint/restore semantics we
explicitly do not want. The lifetime of a process is the lifetime of
its session; the lifetime of a session is the lifetime of one agent
task. Callers (harness/bridge) treat session loss as task failure and
re-run.

### Default-off, double-gated

`TOOLS_PROCESS_ENABLED=0` at the daemon HTTP layer (route returns 404)
AND `TOOLS_EXPOSE_PROCESS=0` at the api-rust forward (route returns
403). Either being off blocks the tool. Operators flip one or both
depending on whether they want only-trusted callers (forward gate) or
totally-disabled (daemon kill switch).

Rationale: defense in depth. The forward gate is what makes the tool
"not agent-facing"; the daemon kill switch is what makes the tool
"not running at all" for paranoid deployments.

### Per-session idle reaper

A single `tokio::spawn` task wakes every `TOOLS_SESSION_IDLE_REAP_SEC
/ 4` seconds (cap: 60s), scans sessions, and evicts any whose
`last_touched` is older than the limit. Each tool call updates
`last_touched`. Eviction runs the same cleanup path as
`DELETE /sessions/:sid`, so any processes the session owned are
terminated as part of the eviction.

Rationale: the reaper exists to bound session lifetime when callers
forget to issue `DELETE /sessions/:sid` (network failure, harness
crash, etc.). It is not a separate "process killer" — by construction
killing the session kills its processes.

### Unsupported declared at runtime, not compile-time

The schema includes `process_unsupported` because EFP RFC 0001
requires backends to explicitly say "no" instead of falling back to
host. The daemon will probe its environment on first
`TOOLS_PROCESS_ENABLED` request (e.g., that it can `fork`+`pre_exec`
under the configured isolation) and cache a yes/no answer. Negative
cached → all future requests reply `501 process_unsupported` without
attempting anything.

Rationale: dev-container / restricted-syscall environments may permit
`bash` (one-shot) but not long-lived child supervision; saying so
plainly is better than partial work.

## Risks / Trade-offs

- [Long-lived `read` blocking under `timeout_sec`] → Each `read`
  acquires the buffer notify channel via `tokio::time::timeout`; we
  cap timeout at a hard daemon-side ceiling (`min(timeout_sec, 60s)`)
  to avoid axum slot starvation.
- [`tini` PID 1 reaping vs. our wait task] → `tokio::process::Child`
  retains the unreaped handle so `wait()` returns the exit code
  before tini reaps; we do not lose codes. Confirmed by reading the
  tokio docs and current `tool_bash` behavior.
- [Per-session cgroup pids.max collisions] → process tool shares the
  session cgroup, so the cap is enforced at the OS level too. The
  per-session software cap (default 32) is below typical
  `pids.max=4096` to keep the error mode "tool 429" instead of
  "fork EAGAIN deep in a child."
- [Base64 doubling stdout cost] → `read` returns base64 when
  requested, but the ring buffer stores raw bytes; the doubling
  happens only at the response edge. Buffer cap stays in raw bytes.
- [`SIGKILL` race with `wait`] → after we send `SIGKILL` the wait
  task observes `Terminated{signal=9}`; `wait_notify` fires and any
  parked `wait` returns. Verified manually by tracing the
  `tokio::process::Child::wait` future semantics.
- [Caller "loses" a process because the session got reaped] → this is
  the *intended* failure mode. The contract is "your processes only
  live as long as the session." Idle reaper, daemon shutdown, and
  template eviction all collapse to the same observable: session
  404, all of its `process_id`s 404. Callers re-run the agent task.
- [Test surface] → most failure modes are race conditions
  (kill-during-read, exit-during-write, EOF flag flipping). Test plan
  in `tasks.md` enumerates them with deterministic helpers (a
  helper-binary that ignores SIGTERM, another that sleeps then
  exits, etc.).

## Migration Plan

1. Land the daemon-side implementation behind `TOOLS_PROCESS_ENABLED=0`.
   Existing deployments are unaffected; new HTTP route returns 404.
2. Land the forward gate `TOOLS_EXPOSE_PROCESS=0`. Existing `/v2`
   callers are unaffected.
3. Flip `TOOLS_PROCESS_ENABLED=1` (daemon) and `TOOLS_EXPOSE_PROCESS=1`
   (forward) on trusted environments (harness, bridge) only.
4. Rollback path: unset either env. The daemon's drain-on-shutdown
   ensures no children survive a redeploy.

## Open Questions

- Should `wait` with no `timeout_sec` block indefinitely or use a
  daemon-side default ceiling? **Tentative**: ceiling at 5 minutes
  with a clear "still running" response, so HTTP connections do not
  pile up. Confirm during implementation.
- Do we want a `process_started` log line per spawn? Useful for
  audit; cheap; default yes, can be silenced with `RUST_LOG`.
