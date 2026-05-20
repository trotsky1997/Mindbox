## Why

The `add-volume-mounts` change (already shipped) gave e2b-shim local-disk
volumes: each volume lives under `/var/lib/e2b-shim/volumes/<vid>/` and is
copied into a sandbox's cwd on create, with sandbox writes mirrored back to
the same host dir (B+C hybrid).

Agent workflows increasingly want to mount **already-existing S3/TOS
prefixes** — pre-baked corpora, model artifacts, shared workspaces — without
first downloading them out-of-band and PUT-ing every file through
`/volumecontent/:vid/file`. The current backend is local-only and has no way
to seed from or sync to an S3 bucket.

We want a second volume backend (`s3`) that drops in transparently: same
`POST /volumes` + `volume_mounts={path: volume}` SDK surface, the only
difference is the volume's storage. Local volumes keep behaving exactly as
they do today.

## What Changes

The mental model: a volume is a **named bytes-bag** with a backend that knows
how to materialize into a session cwd and how to absorb writes back. Today's
implementation has one backend (Local); we add a second (S3) and route every
read/write/list/copy operation through a backend trait. The SDK contract
(`AsyncVolume.create → write_file → AsyncSandbox.create(volume_mounts=...)`)
stays bit-identical — only the volume record carries an extra field.

- Add a `backend` field on `POST /volumes` accepting `local` (default,
  current behavior) or `s3`. When `s3`, the body also carries
  `s3: { bucket, prefix, endpoint?, region?, access_key?, secret_key? }`.
  Missing `s3` env values fall back to the same `TOS_*` envs the snapshot
  path already uses.
- Persist a `VolumeBackend` discriminator on `VolumeRec` so the registry
  knows how to handle subsequent calls. Local volumes keep the same
  on-disk path; S3 volumes use the same `volume_fs_dir(vid)` as a
  **scratch dir** (working copy) and the real source of truth is the S3
  prefix.
- Volume content operations (`PUT/GET /volumecontent/:vid/file`,
  `path` ops) dispatch on `backend`. For S3:
  - PUT file → upload to `s3://bucket/prefix/<rel>` AND mirror to scratch.
  - GET file → if scratch is fresh, serve scratch; otherwise stream from S3
    via `get_object`. (Keep semantics simple: always pull fresh from S3
    on GET, miss the scratch cache.)
  - delete/move → S3 `delete_object` / copy+delete.
- Sandbox create-time mount (`copy_volume_to_tools_session`) for an S3
  volume:
  - List S3 prefix → for each key, stream into session cwd at the mount
    point. (Existing `write_tools_file` pipe — no new tools-rust API.)
- Sandbox write-back (`mirror_write_to_volume`) for an S3 volume:
  - Mirror into scratch (so subsequent local `volume.read_file` works)
    AND `put_object` to S3 at `<prefix>/<rel>`.
- Sandbox delete-back (`mirror_delete_from_volume`) for an S3 volume:
  - Mirror remove on scratch AND `delete_object` on S3.
- Document the contract:
  - S3 volumes are eventually consistent — write-back is best-effort and
    asynchronous from S3's view. A failed S3 put logs and continues; the
    sandbox sees its write succeed locally either way.
  - Mount semantics are still **copy-in at create time**, not FUSE. Files
    appearing in S3 after a sandbox starts are NOT auto-pulled. This is
    the same trade as the local backend.
- Gate the feature: if no S3 credentials resolve (neither per-volume nor
  env fallback), `POST /volumes` with `backend:s3` returns
  `400 { code: "s3_unconfigured", ... }` rather than silently downgrading.

## Capabilities

### New Capabilities
- `s3-volume-backend`: S3/TOS as the storage backend for an e2b-shim volume;
  per-volume bucket+prefix, env fallback, create-time copy-in, write-back
  mirror to both scratch and S3, eventual-consistency contract.

### Modified Capabilities
- e2b-shim volume API (`POST /volumes`, `PUT/GET /volumecontent/:vid/*`):
  accepts `backend` discriminator; existing local behavior unchanged when
  field absent or `local`.

## Impact

- **Code**: `e2b-shim/src/main.rs` — `VolumeRec` gains `backend: VolumeBackend`
  enum, `volumes_create` parses the new field, content handlers dispatch via
  `volume_backend_ops`, `copy_volume_to_tools_session` and
  `mirror_write_to_volume`/`mirror_delete_from_volume` dispatch identically.
  No tools-rust or api-rust changes.
- **Runtime contract**: new optional fields on `POST /volumes` request body;
  existing clients ignore them. Public response includes `backend` and a
  redacted `s3 { bucket, prefix, region }` echo so callers can confirm.
- **Deployment**: `TOS_*` env knobs continue to drive snapshot upload AND
  now also serve as the fallback for S3 volume creds. Adds optional
  per-volume override env (`S3_VOLUME_DEFAULT_BUCKET`,
  `S3_VOLUME_DEFAULT_PREFIX`) if operators want to default `backend:s3`
  with their bucket but no prefix.
- **Consistency**: weaker than local-disk volumes. Documented as such.
  Concurrent sandboxes mounting the same S3 prefix do NOT see each other's
  writes mid-flight; create-time is the snapshot point.
- **Tests**: unit tests cover backend dispatch and request parsing; live
  S3 calls are gated behind `E2B_SHIM_LIVE_S3=1` so CI doesn't need
  credentials. The dev-instance smoke run uses real TOS as the live
  verification step.
