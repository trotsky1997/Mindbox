# tools-process-tool Specification

## Purpose
TBD - created by archiving change add-process-tool. Update Purpose after archive.
## Requirements
### Requirement: HTTP surface

The tools-rust daemon SHALL expose `POST /sessions/:sid/tools/process`
with a JSON body whose `action` field selects one of the EFP RFC 0001
process actions (`start`, `write`, `read`, `signal`, `wait`, `stop`,
`list`). The endpoint SHALL share routing, path resolution, and session
lookup with the existing seven tools.

#### Scenario: Action dispatch

- **WHEN** the daemon receives a request to `POST /sessions/:sid/tools/process`
  with body `{"action":"<one of the seven values>", ...}` for a session
  that exists
- **THEN** the daemon SHALL route to the corresponding action handler and
  return its `ProcessResult` as JSON with `200`

#### Scenario: Unknown action rejected

- **WHEN** the request body has `action` set to a value other than the
  seven defined values
- **THEN** the daemon SHALL respond with `400 Bad Request` and not
  spawn or touch any process

#### Scenario: Unknown session rejected

- **WHEN** the request references an `sid` that is not present in the
  session map
- **THEN** the daemon SHALL respond with `404 Not Found` and not
  create a process registry entry for that `sid`

### Requirement: Schema and action constraints

The `process` request schema SHALL match EFP RFC 0001 section "process
abstraction". Per-action mandatory fields SHALL be enforced and rejected
with `400` when violated, with the following bindings:

- `start` requires `command`; `process_id` MUST NOT be supplied.
- `write`, `read`, `signal`, `wait`, `stop` each require `process_id`.
- `list` requires neither `command` nor `process_id`.
- `cwd` SHALL be resolved relative to the session cwd and SHALL be
  rejected when it is absolute or contains `..` traversal.
- `encoding` SHALL accept `"utf-8"` (default) and `"base64"`; `base64`
  SHALL apply to the entire `input` on write and to the entire
  `stdout`/`stderr` payload on read.
- `timeout_sec` SHALL be a floating-point seconds value.

#### Scenario: start without command

- **WHEN** a `start` action is submitted without `command`
- **THEN** the daemon SHALL respond with `400` and not spawn a process

#### Scenario: write without process_id

- **WHEN** a `write` action is submitted without `process_id`
- **THEN** the daemon SHALL respond with `400` and not touch any
  process state

#### Scenario: cwd traversal rejected

- **WHEN** a `start` action specifies `cwd` as an absolute path or one
  containing `..` segments that escape the session cwd
- **THEN** the daemon SHALL respond with `400` and not spawn a process

#### Scenario: base64 round-trip

- **WHEN** a caller submits `write` with `encoding:"base64"` and a
  base64-encoded payload, then later submits `read` with
  `encoding:"base64"`
- **THEN** the bytes the child reads on stdin SHALL match the decoded
  payload, and the bytes returned as base64 stdout SHALL decode to the
  child's raw stdout

### Requirement: Process state model

Each `start` SHALL allocate a stable `process_id` and a per-process
state machine with states `Spawning → Running → Exited(code) |
Terminated(signal) → Reaped`. State transitions SHALL be visible to
all subsequent actions through `ProcessResult` fields (`running`,
`exit_code`).

#### Scenario: Stable process_id

- **WHEN** `start` succeeds
- **THEN** the response SHALL include a non-empty `process_id` that
  uniquely identifies the process within its daemon lifetime, and
  follow-up actions using that `process_id` SHALL route to the same
  process while it exists

#### Scenario: Operations on exited process

- **WHEN** the child has exited and a caller submits `write`, `read`,
  `signal`, `wait`, or `stop` for that `process_id`
- **THEN** the daemon SHALL NOT return an error solely because the
  process has exited; it SHALL return a successful `ProcessResult`
  with `running:false`, the recorded `exit_code` (and `signal` if any),
  and SHALL still serve any buffered stdout/stderr left to read

#### Scenario: Reaped process not found

- **WHEN** an action references a `process_id` that has been reaped and
  garbage-collected
- **THEN** the daemon SHALL respond with `404 Not Found`

### Requirement: stdout/stderr buffering

For every process the daemon SHALL drain stdout and stderr into separate
ring buffers in background tasks. Each stream SHALL have a server-side
read cursor that advances on `read`. Each stream SHALL have a maximum
buffered size (`TOOLS_PROCESS_BUFFER_BYTES`, default `1 048 576`); when
exceeded, the oldest bytes SHALL be discarded and the next `read`
SHALL set `truncated:true` for that stream.

#### Scenario: Incremental read

- **WHEN** a process emits more stdout, then a caller `read`s, then the
  process emits additional stdout, then the caller `read`s again
- **THEN** the first `read` SHALL return only the data emitted before
  it, and the second `read` SHALL return only the data emitted between
  the two reads

#### Scenario: Read after exit

- **WHEN** a process has exited but its stdout buffer still contains
  un-read bytes
- **THEN** `read` SHALL return those bytes with `running:false` and
  `eof_stdout:true` once they have all been delivered

#### Scenario: Buffer truncation

- **WHEN** a process produces more output on a single stream than
  `TOOLS_PROCESS_BUFFER_BYTES` before the caller reads
- **THEN** the next `read` for that stream SHALL set `truncated:true`,
  and the data returned SHALL contain the newest bytes, not the oldest

### Requirement: Stdin and EOF

`write` SHALL forward the JSON-decoded `input` bytes to the child's
stdin verbatim (no auto-appended newline, no shell escaping). When
`eof:true` is set, the daemon SHALL close stdin after the write.

#### Scenario: Verbatim write

- **WHEN** a caller submits `write` with `input:"hello"`
- **THEN** the child SHALL read exactly the bytes `hello` from stdin
  with no trailing newline added by the daemon

#### Scenario: EOF closes stdin

- **WHEN** a caller submits `write` with `eof:true`
- **THEN** the child SHALL observe stdin closed after that write
  completes, and any subsequent `write` for the same `process_id`
  SHALL be rejected with `400` (`stdin already closed`)

### Requirement: Signals and stop

`signal` SHALL accept POSIX signal names (`SIGTERM`, `SIGINT`,
`SIGKILL`, `SIGHUP`, `SIGUSR1`, `SIGUSR2`, etc.) and deliver them to
the child's process group when `TOOLS_PROCESS_KILL_GROUP=1` (default),
otherwise to the child pid only. `stop` SHALL attempt a graceful
termination first (`SIGTERM`, configurable grace), and SHALL escalate
to `SIGKILL` if the child is still running after the grace period.

#### Scenario: SIGTERM graceful

- **WHEN** a process is `Running` and a caller submits `stop`, and the
  child exits within the grace period after `SIGTERM`
- **THEN** the daemon SHALL NOT send `SIGKILL`, and the response SHALL
  report `running:false` with the recorded exit code or signal

#### Scenario: SIGKILL escalation

- **WHEN** a process ignores `SIGTERM` and is still `Running` at the
  end of the grace period
- **THEN** the daemon SHALL send `SIGKILL`, and the response SHALL
  report `running:false` and indicate it was killed

#### Scenario: Signal to exited process

- **WHEN** a caller `signal`s a process that has already exited but
  not yet been reaped
- **THEN** the daemon SHALL respond with a successful `ProcessResult`
  describing the current state and SHALL NOT return an error

### Requirement: Wait

`wait` SHALL block until the process exits or the optional
`timeout_sec` elapses. Non-zero exit codes SHALL NOT be reported as
HTTP errors; they SHALL be returned in `exit_code`. Timeout SHALL
return successfully with `running:true` and no `exit_code`.

#### Scenario: Wait until exit

- **WHEN** a caller submits `wait` for a running process and the child
  exits with code `0`
- **THEN** the daemon SHALL return `200` with `running:false` and
  `exit_code:0`

#### Scenario: Wait timeout

- **WHEN** a caller submits `wait` with `timeout_sec:1.0` for a
  long-running process
- **THEN** the daemon SHALL return `200` with `running:true` and no
  `exit_code` after at most ~1 second

#### Scenario: Non-zero exit not an error

- **WHEN** a process exits with code `42`
- **THEN** `wait` SHALL return `200` with `exit_code:42`, not an HTTP
  error

### Requirement: List per session

`list` SHALL return an array of every process currently tracked for the
calling session, including reaped-but-not-yet-gc'd entries. Each entry
SHALL include `process_id`, `running`, `exit_code` (when known),
`command`, and `started_at`.

#### Scenario: Multiple processes

- **WHEN** the session has two running processes and one exited
  process not yet reaped
- **THEN** `list` SHALL return exactly those three entries with their
  current state

### Requirement: Isolation reuse

The `process` tool SHALL reuse the same isolation pipeline as
`bash`. When `TOOLS_ISOLATION` selects `chroot`, `seccomp`, or
`cgroup`, it SHALL apply the same `pre_exec` hooks and the same
per-session cgroup attach to each spawned process. Each spawned
process SHALL run with the session cwd as its working directory,
optionally further constrained by the action's `cwd` field.

#### Scenario: Process inherits bash isolation

- **WHEN** the daemon is started with `TOOLS_ISOLATION=chroot,seccomp`
  and a caller `start`s a process
- **THEN** the spawned child SHALL be subject to the same chroot rootfs
  and seccomp filter that `bash` subprocesses receive

### Requirement: Session-bound lifecycle

`DELETE /sessions/:sid` SHALL terminate every process registered for
that session before returning, attempting `SIGTERM` followed by
`SIGKILL` after a short grace, and SHALL remove all per-session
process state from the registry.

#### Scenario: Delete session reaps processes

- **WHEN** a session has running processes and `DELETE /sessions/:sid`
  is invoked
- **THEN** the daemon SHALL terminate each child (graceful then
  forceful) and SHALL remove every `process_id` for that session from
  the registry before returning `204`

### Requirement: Daemon shutdown reaps processes

The daemon SHALL drain every tracked process when it receives a
graceful shutdown signal (e.g. `SIGTERM`). It SHALL stop accepting new
HTTP requests, terminate every tracked process across every session
(graceful then forceful), wait for them to be reaped, and then exit.

#### Scenario: SIGTERM cleanup

- **WHEN** the daemon process receives `SIGTERM` while sessions have
  running child processes
- **THEN** the daemon SHALL terminate every child and SHALL exit
  after every child has been reaped

### Requirement: Idle session reaper

A background task SHALL evict sessions whose last tool-call timestamp
is older than `TOOLS_SESSION_IDLE_REAP_SEC` (default `3600`). Eviction
SHALL run the same cleanup as `DELETE /sessions/:sid`, which by
construction terminates and reaps every process the session owned.

#### Scenario: Idle session cleaned up

- **WHEN** a session has not received any tool call for longer than
  `TOOLS_SESSION_IDLE_REAP_SEC`
- **THEN** the reaper SHALL terminate its processes and remove the
  session, and subsequent tool calls for that `sid` SHALL return
  `404 Not Found`

### Requirement: Resource caps

The daemon SHALL enforce a per-session active process cap
(`TOOLS_MAX_PROCESSES_PER_SESSION`, default `32`). `start` SHALL be
rejected with `429 Too Many Requests` when the cap is reached. The
daemon SHALL NOT enforce a separate per-process maximum lifetime;
the owning session is the lifetime bound.

#### Scenario: Process cap enforced

- **WHEN** a session already owns `TOOLS_MAX_PROCESSES_PER_SESSION`
  running processes and a caller submits `start`
- **THEN** the daemon SHALL respond with `429` and not spawn a
  process

### Requirement: Default disabled

The `process` tool SHALL be disabled by default. The daemon SHALL
serve `404 Not Found` for `POST /sessions/:sid/tools/process` unless
`TOOLS_PROCESS_ENABLED=1`. Independently, `api-rust` SHALL refuse to
forward `/v2/sessions/:sid/tools/process` unless
`TOOLS_EXPOSE_PROCESS=1`, returning `403 Forbidden` with a message
identifying that the tool is gated for trusted callers.

#### Scenario: Daemon kill switch

- **WHEN** `TOOLS_PROCESS_ENABLED` is unset and the daemon receives
  `POST /sessions/:sid/tools/process`
- **THEN** the daemon SHALL respond with `404 Not Found`

#### Scenario: Forward gate

- **WHEN** `TOOLS_EXPOSE_PROCESS` is unset and the api-rust forward
  receives `POST /v2/sessions/:sid/tools/process`
- **THEN** the forward SHALL respond with `403 Forbidden` and SHALL
  NOT contact the daemon

### Requirement: Session-bound id lifetime

`process_id` values SHALL be unique within the lifetime of one session
and SHALL be invalidated immediately when that session ends, for any
reason — explicit `DELETE /sessions/:sid`, idle reap, daemon graceful
shutdown, or loss of the underlying template container. The daemon
SHALL NOT attempt checkpoint/restore of running processes and SHALL
NOT allow a `process_id` to be reattached to a different session.

#### Scenario: Session delete invalidates ids

- **WHEN** a session is deleted and a caller subsequently submits any
  `process` action referencing a `process_id` that belonged to that
  session
- **THEN** the daemon SHALL respond with `404 Not Found`

#### Scenario: Daemon restart invalidates ids

- **WHEN** the daemon restarts
- **THEN** every previously-issued `process_id` SHALL be considered
  gone (because every session was destroyed), and tools-rust SHALL
  respond with `404` for any subsequent action referencing those ids

#### Scenario: Cross-session reuse rejected

- **WHEN** a caller passes a `process_id` issued by session A inside a
  request to session B
- **THEN** session B SHALL respond with `404 Not Found` and SHALL NOT
  contact session A's process

### Requirement: Unsupported backend signaling

The daemon SHALL signal unsupported environments explicitly. If it
detects a fatal startup constraint preventing process support (for
example, missing `/proc` for signal delivery), it SHALL respond to
every `process` request with `501 Not Implemented` and a
machine-parseable error body whose `code` is `process_unsupported`.
The daemon SHALL NOT silently degrade `process` requests to host-side
execution.

#### Scenario: Unsupported declared

- **WHEN** the daemon was started with process support compiled but
  cannot satisfy a backend invariant at runtime
- **THEN** the daemon SHALL reply with `501` and
  `code:"process_unsupported"` to every `process` request, and SHALL
  NOT execute the requested command in any other context

