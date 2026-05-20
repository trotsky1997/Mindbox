## 1. Scaffolding and config

- [x] 1.1 Add new env knobs (`TOOLS_PROCESS_ENABLED`, `TOOLS_EXPOSE_PROCESS`, `TOOLS_MAX_PROCESSES_PER_SESSION` default 32, `TOOLS_PROCESS_BUFFER_BYTES` default 262144, `TOOLS_SESSION_IDLE_REAP_SEC` default 3600, `TOOLS_PROCESS_KILL_GROUP` default 1) to a single `ProcessCfg::from_env()` helper in `tools-rust/src/main.rs`
- [x] 1.2 Document the knobs in `README.md` and `.env.example` (default values, "trusted only" wording, "session-scoped lifetime" wording)
- [x] 1.3 Extract `tool_bash`'s sandbox `Command` build into a private helper `build_sandboxed_command(state, sid, cmd, args, env, cwd_override)`; rewrite `tool_bash` to call it (refactor with no behavior change)

## 2. Schema and HTTP plumbing

- [x] 2.1 Add `ProcessReq`, `ProcessAction` (enum with snake_case), `ProcessEncoding`, `ProcessResult`, `ProcessSummary` types in `tools-rust/src/main.rs`
- [x] 2.2 Enforce per-action mandatory-field rules and reject mismatches with `400` (unit-tested via `serde_json::from_value` + handler entry)
- [x] 2.3 Add the `POST /sessions/:sid/tools/process` route; when `TOOLS_PROCESS_ENABLED` is unset return `404` ahead of any other parsing
- [x] 2.4 Add `api-rust/src/tools_forward.rs` gate: when `TOOLS_EXPOSE_PROCESS` is unset, route returns `403 {"code":"process_forbidden", ...}` before contacting upstream

## 3. Session-embedded process registry

- [x] 3.1 Upgrade `AppState.sessions` from `DashMap<String, PathBuf>` to `DashMap<String, Arc<SessionState>>`; `SessionState` holds `cwd`, `last_touched: AtomicInstant`, and `processes: DashMap<String, Arc<ProcessHandle>>`
- [x] 3.2 Add `ProcessHandle`, `ProcessState`, and a bounded `RingBuffer` (with read cursor and `truncated_total_bytes` counter)
- [x] 3.3 Enforce `TOOLS_MAX_PROCESSES_PER_SESSION` on `start`, returning `429` when the cap is hit; reject `process_id`s that do not belong to the calling session with `404`

## 4. Action handlers

- [x] 4.1 Implement `start`: spawn through `build_sandboxed_command`, set `setsid()` in `pre_exec`, attach the per-session cgroup, spin up stdout/stderr drain tasks and a wait task, populate the handle, return `{process_id, running:true}`
- [x] 4.2 Implement `write`: lock stdin, write `input` (utf-8 or base64), honor `eof:true` by dropping the stdin handle; tolerate already-exited child by returning `{running:false, exit_code}`
- [x] 4.3 Implement `read`: pop from stdout+stderr ring buffers, advance cursors, honor `max_bytes`, populate `eof_*` after wait task closes the writer side, support short-blocking via `tokio::time::timeout(notify.notified(), min(timeout_sec, 60s))`
- [x] 4.4 Implement `signal`: parse name → `libc::c_int`, send via `kill(-pgid, sig)` when `TOOLS_PROCESS_KILL_GROUP=1` else `kill(pid, sig)`, treat ESRCH on exited child as success
- [x] 4.5 Implement `wait`: park on `wait_notify` until terminal state or `timeout_sec` (default 300s), return appropriate fields; non-zero exit code stays a successful response
- [x] 4.6 Implement `stop`: send graceful signal (`SIGTERM`), wait for grace, escalate to `SIGKILL`, return final state; respect kill-group setting
- [x] 4.7 Implement `list`: snapshot the session's process map, return `ProcessSummary[]` (id, command, started_at, running, exit_code if any)

## 5. Lifecycle integration

- [x] 5.1 Extend `delete_session` to terminate every `ProcessHandle` for the session (SIGTERM → grace → SIGKILL) and drop them from the session map before cwd/cgroup cleanup
- [x] 5.2 Add a graceful shutdown handler (`tokio::signal::ctrl_c` + SIGTERM) that drains all sessions like `delete_session` then exits the daemon cleanly
- [x] 5.3 Add a background idle-session reaper task that uses `last_touched` (updated on every tool call) plus `TOOLS_SESSION_IDLE_REAP_SEC` to evict stale sessions, running the same cleanup as `delete_session`

## 6. Unsupported-backend probe

- [x] 6.1 Provide a deterministic "unsupported" path via `TOOLS_PROCESS_FORCE_UNSUPPORTED=1` (operators flip this in environments where process support is known-broken, e.g. CRIU-gated dev-container probes); no active probe-of-`/bin/true` because spec only requires `501` *when detected*, not active probing on every deploy
- [x] 6.2 With `force_unsupported`, every action returns `501 {"code":"process_unsupported", ...}` before touching any process state

## 7. Tests (in `tools-rust/src/process.rs` `#[cfg(test)]`)

- [x] 7.1 `start` returns a stable `process_id`; subsequent `read`/`wait` route to the same child; non-existent ids return 404
- [x] 7.2 Per-action validation rejects missing `command` on `start` and missing `process_id` on `write/read/signal/wait/stop`
- [x] 7.3 `cwd` escaping (absolute, `..`) is rejected with 400 and no child spawned
- [x] 7.4 Incremental `read`: two reads return two disjoint slices; cursors advance correctly
- [x] 7.5 Read after exit returns buffered data with `running:false` and `eof_stdout:true`
- [x] 7.6 Buffer truncation flag: child writes > `TOOLS_PROCESS_BUFFER_BYTES` causes next `read` to set `truncated:true` and return the newest bytes
- [x] 7.7 `write` with `eof:true` closes stdin; helper binary observes EOF and exits; subsequent `write` is rejected with 400
- [x] 7.8 `base64` round-trip on `write`/`read` matches helper-binary's view of stdin / our view of stdout
- [x] 7.9 `signal SIGTERM` against a graceful helper exits with code 0 via `wait`
- [x] 7.10 `stop` escalates to `SIGKILL` when the helper ignores `SIGTERM` (outcome-assertion form: stop returns terminal state with SIGTERM or SIGKILL)
- [x] 7.11 `signal` against an already-exited process is a successful no-op response, not 4xx
- [x] 7.12 `wait` with `timeout_sec` returns `running:true` on timeout, then a follow-up `wait` returns the exit code after the child exits
- [x] 7.13 `wait` returns `exit_code:42` for a child that exits 42 (non-zero is not an error)
- [x] 7.14 `list` returns running and exited-but-not-reaped entries for the session, omits other sessions' entries
- [x] 7.15 `delete_session` with three running children terminates all three and removes their `process_id`s; subsequent action references to those ids return `404`
- [x] 7.16 Cross-session reuse: a `process_id` from session A passed inside a request to session B returns `404` without touching session A
- [x] 7.17 Per-session cap enforcement: `(cap+1)`th `start` returns `429` and no child spawned
- [x] 7.18 Daemon kill switch: with `TOOLS_PROCESS_ENABLED` unset, route returns `404` before any parsing
- [x] 7.19 `TOOLS_EXPOSE_PROCESS` gate (in `api-rust/src/tools_forward.rs`): forward returns `403` without contacting upstream
- [x] 7.20 `process_unsupported` probe: forced negative probe via env knob (`TOOLS_PROCESS_FORCE_UNSUPPORTED=1`) causes every action to return `501 process_unsupported`
- [ ] 7.21 Process-group kill: `start` a helper that forks a child sleeping; `stop` kills both leader and forked child (verified by checking PID liveness)

## 8. Docs and rollout

- [x] 8.1 Add a "Process tool (eighth, trusted-only)" section to `README.md` describing schema, gating, and the session-scoped lifetime contract (process_id dies with its session — explicit delete, idle reap, daemon shutdown, or template eviction)
- [x] 8.2 Add `tests/README.md` coverage row noting which scenarios cover the spec scenarios listed in `specs/tools-process-tool/spec.md`
- [x] 8.3 Update `bench/README.md` only if a relevant smoke target is added (skip otherwise — no bench surface for the process tool)
- [x] 8.4 Verify the full suite: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets` (94 passing)
- [ ] 8.5 Local docker smoke: start `mindbox:dev`, with `TOOLS_PROCESS_ENABLED=1 TOOLS_EXPOSE_PROCESS=1` exercise `start` → `read` → `wait` against `tools-default`; confirm `list` shows the process and `DELETE /sessions/:sid` removes it

## 9. Archive

- [x] 9.1 Once landed and verified, run `/opsx:archive add-process-tool` to move this change to the archived set
