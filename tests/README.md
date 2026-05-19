# Tests

Two layers:

## Unit tests (this dir + inline `#[cfg(test)] mod tests`)

Pure functions only. No Docker, no network, no TOS, no live worker container.
Run on every PR via `.github/workflows/ci.yml`.

```bash
# Rust — workspace test (all 4 crates)
cargo test --workspace

# Python — sandbox_helper.py via pytest
pip install -r tests/python/requirements.txt
pytest tests/python -v
```

### What's covered

| Crate / file | What | Count |
|---|---|---|
| `api-rust/src/main.rs` | `parse_memory`, `load_templates`, `send_frame`/`recv_frame` framing, LRU touch, pb Request/Response round-trip | 14 tests |
| `e2b-shim/src/main.rs` | `general_b64`/`general_b64_decode`, `urlencoding_*`, Connect envelope/`extract_body`/`codec_from_content_type`, `sanitize_name`, `find_subseq`, `SandboxRec` serde | 22 tests |
| `worker-rust/src/main.rs` | `send_frame_async`/`recv_frame_async`, pb Job/ChildResponse round-trip | 6 tests |
| `template-builder/src/main.rs` | `TemplateCfg` TOML parsing | 3 tests |
| `worker-rust/src/sandbox_helper.py` | `_compile_cached`, `_build_resp`, `_maybe_offload`, `_get_rss_mb`, `_count_fds` | 21 tests + 4 skipped (P2 snapshot diff) |
| **Total** | | **66 tests** |

### Why these are valuable

The wire-format functions (base64, Connect envelope, frame headers, ProcessEvent
JSON, ChildResponse protobuf) are what every E2B SDK request and worker IPC
roundtrip depends on. If any of them silently breaks, integration tests would
catch it eventually, but only after wasting hours of e2e debugging. Unit tests
catch the regression in <1s.

## Integration / e2e tests (NOT in this dir)

These require a Docker daemon and (sometimes) TOS credentials, real worker
containers, real api-rust process. They live as ad-hoc Python scripts in
`/tmp/` on the dev host — categorised:

- **Sandbox e2e** — `/tmp/e2e_persist.py`, `/tmp/e2e_concurrent.py`, `/tmp/e2e_binary.py`, `/tmp/e2e_coverage.py`
- **Lifecycle / durability** — `/tmp/test_durability.py`, `/tmp/test_kill_timeout.py`, `/tmp/test_cleanup.py`, `/tmp/test_restart.py`
- **Load** — `/tmp/swe_load.py`, `/tmp/bench_*.py`, `/tmp/bench_jobs_payload.lua` (wrk)
- **TOS / snapshot** — `/tmp/snapshot_test.py`

### Explicitly out of scope for the unit-test plan

| Area | Why deferred |
|---|---|
| docker daemon — `start_template_container`, registry tier transitions, template-builder shell-outs | Needs running dockerd; integration territory |
| TOS / S3 credentials — snapshot upload, `_maybe_offload` against real bucket | Needs Volcano TOS AK/SK; unit version uses a mock |
| worker child process — `socketpair`+`fork`+PyO3, `_run_sandbox_pb` full path with SIGALRM | Needs real fork supervisor |
| FD passing — `SCM_RIGHTS` between api-rust and hot container | Needs running worker |
| axum router e2e, multipart upload, full Connect streaming | The `axum::Router` is wired in `main`; unit-testing requires refactor or `axum-test` harness |
| 20K RPS bench | See `bench-baseline.txt`; not a unit test concern |
| TLS / auth header / CORS | End-to-end only |

About 60% of `main.rs` line count belongs to integration territory; this
unit-test suite covers 100% of the listed pure functions.

## CI

`.github/workflows/ci.yml` runs:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets -- -D warnings` (advisory for now —
   `continue-on-error: true` until existing warnings are swept)
3. `cargo test --workspace --all-targets`
4. `pip install -r tests/python/requirements.txt`
5. `pytest tests/python -v`

Cold: ~3-5 min. Cached: <1 min. Only system dep is `protobuf-compiler` +
`libpython3-dev` (worker-rust embeds Python via PyO3).
