## 1. Schema and parsing

- [ ] 1.1 Introduce `VolumeBackend` enum (`Local`, `S3(S3VolumeCfg)`), `S3VolumeCfg { bucket, prefix, region, endpoint }`. Keep credentials OUT of the rec; hold them in `AppState.s3_creds: Arc<S3CredsResolver>`.
- [ ] 1.2 Add `backend: VolumeBackend` field to `VolumeRec` with `#[serde(default = "VolumeBackend::local")]` so existing records load as Local.
- [ ] 1.3 Extend `CreateVolumeBody` with `backend: Option<String>` and `s3: Option<S3CreateBody>` (untagged camelCase serde).
- [ ] 1.4 In `volumes_create`, build `VolumeBackend` from body. On `s3`, resolve creds/region/endpoint via per-volume → env → operator-default chain. Reject with `400 s3_unconfigured` when bucket can't be resolved.
- [ ] 1.5 Optional sanity check: after constructing the `rust-s3 Bucket`, call `head_bucket()`; on error map to `400 s3_unauthorized` with the upstream code in the message.

## 2. S3 ops module

- [ ] 2.1 Add `s3_ops` module/section in `main.rs`: `s3_put_file(cfg, key, bytes)`, `s3_get_file(cfg, key) -> Vec<u8>`, `s3_delete_file(cfg, key)`, `s3_delete_prefix(cfg, prefix)`, `s3_list_prefix(cfg, prefix) -> Vec<S3Entry>`, `s3_copy(cfg, src, dst)`. Each is a thin async wrapper over `rust-s3`.
- [ ] 2.2 Implement key canonicalization: `s3_key(prefix, rel) -> String` that joins prefix + rel and strips duplicate `/`. Unit-test edge cases (empty prefix, empty rel, prefix with/without trailing `/`).
- [ ] 2.3 Implement `S3Entry { key, size, last_modified, is_prefix }` and `list_prefix` paginated walk via `list_objects_v2` (rust-s3 returns continuation tokens).

## 3. Volume content dispatch

- [ ] 3.1 In each volume content handler (`volume_file_put`, `volume_file_get`, `volume_dir_post`, `volume_dir_get`, `volume_path_get`, `volume_path_delete`, `volume_path_patch`), dispatch on `rec.backend`:
  - `Local` → existing path, unchanged.
  - `S3(cfg)` → call into s3_ops, additionally mirror to scratch where the local code wrote to scratch.
- [ ] 3.2 Confirm `entry_stat_json` is reused for S3 entries by writing the file to scratch first (so stat semantics match local volumes). If scratch write fails after a successful S3 put, log and return the bytes-only stat (size from S3 response, timestamps from S3 LastModified).

## 4. Sandbox mount + mirror dispatch

- [ ] 4.1 Refactor `copy_volume_to_tools_session` into `copy_volume_to_tools_session_local` and `copy_volume_to_tools_session_s3`, dispatching on `rec.backend`. The S3 path iterates `s3_list_prefix` and pipes each object through `write_tools_file`. Strip `cfg.prefix` from each key to derive the relative path inside the mount.
- [ ] 4.2 Refactor `mirror_write_to_volume` to look up the volume's backend and, for S3, perform `s3_put_file(cfg, rel, bytes)` after writing scratch. Failures log and continue (best-effort, consistent with current local behavior on non-fatal mirror errors).
- [ ] 4.3 Refactor `mirror_delete_from_volume` to dispatch identically: scratch rm first, then `s3_delete_file` (or `s3_delete_prefix` if the deleted entry was a directory).

## 5. Configuration

- [ ] 5.1 Add `S3_VOLUME_DEFAULT_BUCKET`, `S3_VOLUME_DEFAULT_PREFIX` env knobs (read in `volumes_create` if body doesn't specify bucket).
- [ ] 5.2 `TOS_*` env vars (`TOS_ACCESS_KEY`, `TOS_SECRET_KEY`, `TOS_S3_ENDPOINT`, `TOS_REGION`) double as the credential fallback. Document this — they're shared with the snapshot path.
- [ ] 5.3 Update `docs/configuration.md` with the new envs, the per-volume-body shape, and the fallback chain.

## 6. Tests (in `e2b-shim/src/main.rs` `#[cfg(test)]`)

- [ ] 6.1 `volumes_create_local_default` — POST body without `backend` → rec has `Local`.
- [ ] 6.2 `volumes_create_s3_full_body` — POST body with `backend:"s3"` + all s3 fields → rec has `S3(cfg)` with the right values.
- [ ] 6.3 `volumes_create_s3_env_fallback` — POST body with `backend:"s3"` + just `bucket`, env has creds → succeeds with env-resolved creds.
- [ ] 6.4 `volumes_create_s3_no_creds` — POST body with `backend:"s3"` + no env → `400 s3_unconfigured`.
- [ ] 6.5 `s3_key_canonicalization` — `s3_key("p/", "a/b")` → `"p/a/b"`; `s3_key("", "/x")` → `"x"`; `s3_key("p", "")` → `"p"`.
- [ ] 6.6 `volume_mirror_for_s3_routes_to_s3_ops` — using a mock S3 trait, confirm `mirror_write_to_volume` calls `s3_put_file` for an S3 volume and not for a Local volume.
- [ ] 6.7 `volume_mirror_for_s3_writes_scratch_too` — confirm scratch dir gets the file after the S3 put.
- [ ] 6.8 `s3_volume_delete_does_not_touch_bucket` — `DELETE /volumes/:vid` removes the rec + scratch but does NOT call any s3_delete (use the mock).

## 7. Live S3 smoke (gated)

- [ ] 7.1 Add a script `tests/s3_volume_e2e.sh` parallel to `tests/volume_e2e.sh` but creating the volume with `backend:s3` and verifying mirror-back to a real bucket via `aws s3 ls` / `head_object`. Skipped by default; run with `E2B_SHIM_LIVE_S3=1` + creds in env.
- [ ] 7.2 Document the script invocation in `tests/README.md`.

## 8. Docs and rollout

- [ ] 8.1 README: add a "S3-backed volumes" subsection under Volumes covering the SDK shape (still `volume_mounts={path: volume}`, only the volume-create body changes), the consistency contract, and the env fallback chain.
- [ ] 8.2 `docs/configuration.md`: env table updates (already-touched in 5.3); add a short note that local and S3 volumes coexist in the same shim.
- [ ] 8.3 `docs/architecture.md`: a one-paragraph note on the backend dispatch model so future readers don't re-derive it from main.rs.
- [ ] 8.4 Verify: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets` pass.
- [ ] 8.5 Live verification on dev-instance: pull new GHCR image, extract fresh `e2b-shim` binary, run `tests/s3_volume_e2e.sh` against the configured `TOS_BUCKET`.

## 9. Archive

- [ ] 9.1 Once landed and verified, run the archive flow to move this change under `openspec/changes/archive/<date>-add-s3-volume-backend/`.
