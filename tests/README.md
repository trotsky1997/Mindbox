# Tests

The repository is Rust-only at runtime. Unit tests live inline in each crate under
`#[cfg(test)]`; there is no Python test suite or legacy worker test path.

Run the full suite:

```bash
cargo test --workspace --all-targets
```

CI runs the same checks used before merging:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

## Coverage areas

| Crate | Coverage |
|---|---|
| `api-rust` | template loading, warmup config/result helpers, memory parsing, PagedRegistry state transitions |
| `e2b-shim` | E2B-compatible request/response helpers, Connect envelopes, path/body parsing |
| `template-builder` | template config/warmup parsing and image-name logic |
| `tools-rust` | session path validation, seven-tool schemas, read/write/edit/ls/grep/find/bash behavior, isolation helpers |

## Integration testing

Docker-backed tests and remote deployment checks are still manual/integration
work. They require a Docker daemon and built template images, so they are not run
as unit tests. Useful smoke coverage is:

1. Build `api-rust`, `e2b-shim`, `template-builder`, and `tools-rust`.
2. Build at least `templates/tools-default`.
3. Start `api-rust` with Docker access.
4. Create a `/v2/sessions` session.
5. Exercise all seven `/v2/sessions/:sid/tools/:tool` endpoints with canonical request bodies.
6. For warmup, add a temporary `[warmup]` command to a template and confirm logs show health, warmup success, then `COLD→HOT`.
7. Test a failing warmup command and confirm the failed container is removed instead of registered Hot.
8. Optionally verify `e2b-shim` with `commands.run`, `files.read`, and `files.write`.
