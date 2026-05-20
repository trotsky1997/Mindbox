# Design — S3 volume backend

## Goals and non-goals

**Goals.** Let `volumeMounts` in an `AsyncSandbox.create` call point at a
volume whose authoritative storage is an S3-compatible bucket+prefix
(Volcano TOS, AWS S3, MinIO, anything `rust-s3` already speaks). Make the
SDK contract bit-identical to local volumes — same `POST /volumes`, same
`PUT /volumecontent/:vid/file`, same sandbox mount path semantics. Write-back
from a running sandbox mirrors to both the scratch dir and the bucket.

**Non-goals.** FUSE / live mount. Cross-sandbox real-time sync. S3 events.
Object-versioning awareness. Multi-region replication. Strong consistency
across sandboxes mounted on the same prefix concurrently.

## Backend dispatch model

Today `VolumeRec` is a flat struct with `{volume_id, name, token, size_mb,
created_at}`. All operations (PUT/GET volumecontent, create-time copy,
write-back mirror, delete mirror) assume the volume lives at
`volume_fs_dir(vid)` on the host. The change adds a tagged enum:

```rust
enum VolumeBackend {
    Local,
    S3(S3VolumeCfg),
}

struct S3VolumeCfg {
    bucket: String,
    prefix: String,        // canonicalized: no leading "/", trailing "/" optional
    region: String,        // resolved at create-time
    endpoint: String,      // resolved at create-time
    // creds live in AppState, not the rec — see "Secrets" below
}
```

`VolumeRec` gains a `backend: VolumeBackend` field. Existing volumes
default to `Local` on deserialize (`#[serde(default)]`), so no
on-disk-registry migration is needed (the volume registry isn't persisted
across restart today; if persistence is added later the default keeps old
records valid).

Every code path that does volume I/O calls a dispatcher:

```rust
match &rec.backend {
    VolumeBackend::Local => local_ops(...),
    VolumeBackend::S3(cfg) => s3_ops(state, cfg, ...).await,
}
```

The scratch dir at `volume_fs_dir(vid)` is **kept** for both backends:

- Local: it's the authoritative storage (unchanged).
- S3: it's a working copy. `mirror_write_to_volume` always writes to scratch
  (so subsequent local reads via `volume.read_file` are cheap) AND pushes to
  S3. `copy_volume_to_tools_session` for S3 ignores scratch and walks the S3
  prefix directly so a freshly-created sandbox always sees the current
  S3 state, not stale scratch.

## API surface

### `POST /volumes` request body

Existing:
```json
{ "name": "my-vol", "sizeMB": 1024 }
```

New (additive, backwards-compatible):
```json
{
  "name": "my-vol",
  "backend": "s3",
  "s3": {
    "bucket": "mindbox-data",
    "prefix": "agents/run-0521/",
    "endpoint": "https://tos-s3-cn-beijing.ivolces.com",   // optional, env fallback
    "region": "cn-beijing",                                 // optional, env fallback
    "accessKey": "...",                                     // optional, env fallback
    "secretKey": "..."                                      // optional, env fallback
  }
}
```

Validation:
- `backend` absent or `"local"` → `VolumeBackend::Local` (current behavior).
- `backend == "s3"` requires `s3.bucket`. `prefix` defaults to `""`.
- Credentials/region/endpoint resolve in order: per-volume body → env
  (`TOS_*` family) → fail with `400 s3_unconfigured`.
- An empty bucket is rejected with `400 bad_request`.

### Response

`VolumeRec` JSON gains:

```json
{
  "volumeID": "vol202605...",
  "name": "my-vol",
  "token": "uuid...",
  "sizeMB": 1024,
  "createdAt": "...",
  "backend": "s3",
  "s3": { "bucket": "mindbox-data", "prefix": "agents/run-0521/", "region": "cn-beijing", "endpoint": "https://tos-s3-cn-beijing.ivolces.com" }
}
```

Secrets are **not** echoed. The token still gates volumecontent operations.

### `/volumecontent/:vid/*`

Dispatch table:

| Op | Local | S3 |
|---|---|---|
| `PUT /file` | write scratch | put_object + write scratch |
| `GET /file` | read scratch | get_object (always); falls back to scratch on transient error |
| `POST /dir` | mkdir scratch | put_object with empty body at `<prefix>/<rel>/` (S3 directory marker) + mkdir scratch |
| `GET /dir` | walk scratch | list_objects + map to entries |
| `GET /path` | stat scratch | head_object → entry json; on miss, list_objects with prefix to see if it's a "dir" |
| `DELETE /path` | rm scratch | delete_object (recursive list+delete for dirs) + rm scratch |
| `PATCH /path` (rename) | rename scratch | copy_object new → delete_object old + rename scratch |

For now, the S3 file-write *always* updates scratch synchronously after the
S3 put returns; if S3 succeeds and scratch fails, we log and return 201
(consistent with the "scratch is a cache" model).

### `volume_mounts` materialization

`copy_volume_to_tools_session`:

- Local: unchanged. `cp -r` from scratch into session cwd.
- S3: list_objects with the prefix; for each key, `get_object` and pipe
  into `write_tools_file(state, sid, child_rel, bytes)`. Reuses the same
  `mkdir_cmd + write_tools_file` plumbing as local so there's no new
  tools-rust dependency.

Empty prefix → mount creates an empty dir at the mount path (the existing
`rm -rf && mkdir -p` prelude already handles that).

### Write-back

`mirror_write_to_volume` for S3:

1. Write to scratch (existing path).
2. `put_object_with_content_type(rel, bytes, content_type)` to
   `<prefix>/<rel>`. content_type defaulting to `application/octet-stream`
   is fine — sandboxes generally don't care.
3. Log non-fatal errors. The sandbox's `files.write` already succeeded;
   we don't fail it because of S3 hiccups.

`mirror_delete_from_volume` for S3:

1. Remove from scratch.
2. `delete_object(<prefix>/<rel>)`. For dir deletes, list with
   `<prefix>/<rel>/` and bulk-delete the page (one page is enough for
   typical agent workloads; multi-page is a TODO).

## Secrets

Three layers, resolved in order at `POST /volumes` time:

1. **Per-volume body**: `s3.accessKey` + `s3.secretKey`. Useful for
   multi-tenant deployments and tests. Stored ONLY in memory on
   `S3VolumeCfg` — they don't go into the response.
2. **Env**: `TOS_ACCESS_KEY` / `TOS_SECRET_KEY` (already used by snapshot).
   This means a single-tenant deployment can `backend:s3` with just
   `bucket` + `prefix` in the body.
3. **Operator default**: `S3_VOLUME_DEFAULT_BUCKET` /
   `S3_VOLUME_DEFAULT_PREFIX` let an operator make `backend:s3` valid
   with literally just `{"backend":"s3"}` in the body. Each volume then
   gets `<default_prefix>/<volume_id>/` so volumes don't collide.

The credential bundle is held in `AppState` as a single
`Arc<S3CredsResolver>` so all volumes share the same `rust-s3 Bucket`
factory but per-volume bucket/region/endpoint are still allowed (each
`VolumeRec` instantiates its own `Bucket` lazily on first use via a
DashMap cache keyed by `(bucket, endpoint, region)`).

## Consistency contract (documented to users)

- **Create-time snapshot**: a sandbox mounting an S3 volume sees the
  prefix's state at the moment of `AsyncSandbox.create`. Files added to
  S3 afterwards are not visible inside that sandbox.
- **Eventually consistent write-back**: writes inside the sandbox land
  in scratch synchronously and are PUT to S3 asynchronously (within the
  same request handler — typically < 1s on a healthy connection, but if
  S3 errors, the local write is kept and the S3 side is logged).
  Concurrent readers of the bucket may briefly see stale state.
- **No cross-mount sync**: two sandboxes mounting the same S3 prefix
  do not share a live view. Each gets its create-time snapshot; writes
  flow back to S3 but don't propagate sideways.
- **Volume deletion** removes the registry record AND the scratch dir.
  It does **not** delete the S3 prefix — operators expect S3 to outlive
  the shim. (A future flag could opt in.)

## Failure modes

| Scenario | Behavior |
|---|---|
| S3 unreachable at create-time | `POST /volumes` succeeds (the registry record is stored); first PUT/GET fails with 502 from rust-s3, logged. |
| S3 unreachable mid-write | `files.write` returns 200 (the local tools-rust write succeeded); the mirror PUT fails and logs. |
| S3 returns 403/404 on get | `GET /volumecontent/:vid/file` returns 404 to the client and logs the upstream code. |
| Per-volume creds wrong | `POST /volumes` returns `400 s3_unauthorized` (we do a sanity `head_bucket` after constructing the client). |
| Prefix is huge (10k+ objects) | Create-time copy-in iterates list_objects pagination. Acceptable; a separate future change can opt into parallelism if needed. |

## Test strategy

Unit tests (no live S3):
- `POST /volumes` parses `backend:"s3"` with full s3 config.
- `POST /volumes` parses `backend:"local"` / missing-field as Local.
- `POST /volumes` with `backend:"s3"` and no creds → `400 s3_unconfigured`.
- Backend dispatch helper (mock S3 via a trait) routes ops correctly.
- `volume_mirror_for` results feed into the right backend.

Live (gated by `E2B_SHIM_LIVE_S3=1`):
- Create S3 volume against a real bucket.
- Seed via PUT volumecontent → confirm visible via S3 `head_object`.
- Create sandbox with mount → confirm seeded file visible inside.
- Sandbox writes → confirm visible via S3 `get_object`.
- Delete in sandbox → confirm S3 object gone.

E2E on dev-instance (the canonical smoke):
- TOS bucket already set up via env. Drive the same flow as
  `tests/volume_e2e.sh` but with `backend:s3` and confirm scratch +
  bucket both have the file after the sandbox writes.

## Open questions / TODOs

- **Multi-page deletes for prefix-delete**: current design deletes one
  list_objects page (1000 keys). Real workloads occasionally exceed that.
  Acceptable for v1; track as follow-up.
- **Caching get_object**: we always pull from S3 on `GET /volumecontent`.
  Could memoize against scratch if it becomes a hot path. Defer.
- **At-rest encryption / SSE-C**: not exposed yet. Bucket-default policies
  cover most use cases.
- **Volume registry persistence**: today not persisted. If a future change
  persists it, S3 volume creds must NOT be serialized — they live only in
  AppState.
