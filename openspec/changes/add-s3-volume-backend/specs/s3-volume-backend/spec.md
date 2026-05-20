# s3-volume-backend Specification

## Purpose

Let e2b-shim volumes use an S3-compatible bucket+prefix as authoritative
storage while preserving the existing volume_mounts SDK contract. The local
backend remains the default; selecting S3 is an opt-in field on `POST
/volumes`.

## Requirements

### Requirement: Backend discriminator on volume create

The `POST /volumes` endpoint SHALL accept an optional `backend` string
field with values `"local"` (default when absent) and `"s3"`. When
`"s3"` is requested, the body SHALL also accept an `s3` object holding
`bucket`, optional `prefix`, optional `endpoint`, optional `region`,
optional `accessKey`, and optional `secretKey`.

The returned `VolumeRec` SHALL include a `backend` field echoing the
selected backend, and (for S3) an `s3` object echoing `bucket`,
`prefix`, `region`, `endpoint`. Credentials SHALL NOT appear in the
response.

#### Scenario: Default backend is local

- **WHEN** `POST /volumes` is called with no `backend` field
- **THEN** the resulting `VolumeRec` SHALL have `backend: "local"` and
  behave identically to volumes created before this change

#### Scenario: S3 backend echoes config

- **WHEN** `POST /volumes` is called with `backend: "s3"` and a full
  `s3` object
- **THEN** the response SHALL include `backend: "s3"` and an `s3`
  object with `bucket`, `prefix`, `region`, `endpoint` matching the
  request (or env fallbacks where the request omitted them)
- **AND** the response SHALL NOT include `accessKey` or `secretKey`

### Requirement: Credential resolution chain

When the backend is S3, credentials and connection parameters SHALL
resolve in this order:

1. Per-volume `s3.accessKey` / `s3.secretKey` / `s3.endpoint` / `s3.region`.
2. Environment: `TOS_ACCESS_KEY`, `TOS_SECRET_KEY`, `TOS_S3_ENDPOINT`,
   `TOS_REGION`.
3. Operator defaults: `S3_VOLUME_DEFAULT_BUCKET` and
   `S3_VOLUME_DEFAULT_PREFIX` apply only to `bucket` and `prefix`.

If `bucket` is not resolvable through any layer, `POST /volumes` SHALL
return `400 { code: "s3_unconfigured", ... }`. If credentials are not
resolvable, `POST /volumes` SHALL return `400 { code: "s3_unconfigured", ... }`.

#### Scenario: Env fallback succeeds

- **GIVEN** `TOS_ACCESS_KEY`, `TOS_SECRET_KEY`, `TOS_S3_ENDPOINT` are set
- **WHEN** `POST /volumes` is called with `{backend: "s3", s3: {bucket: "X"}}`
- **THEN** the volume SHALL be created using env-resolved credentials

#### Scenario: No creds anywhere is rejected

- **GIVEN** none of the env vars or operator defaults are set
- **WHEN** `POST /volumes` is called with `{backend: "s3", s3: {bucket: "X"}}`
  and no per-volume creds
- **THEN** the response SHALL be `400` with `code: "s3_unconfigured"`
- **AND** no `VolumeRec` SHALL be created

### Requirement: Volume content operations dispatch on backend

`PUT/GET /volumecontent/:vid/file`, `POST/GET /volumecontent/:vid/dir`,
and `GET/DELETE/PATCH /volumecontent/:vid/path` SHALL dispatch on the
volume's `backend` field:

- `local` → operate on `volume_fs_dir(vid)` as today.
- `s3` → operate on the configured S3 bucket+prefix.

For an S3 volume, every successful write (`PUT /file`, `POST /dir`) SHALL
also write into `volume_fs_dir(vid)` so subsequent reads can use a
local cache and `entry_stat_json` matches the local-backend shape.

#### Scenario: PUT file on S3 volume puts to bucket

- **GIVEN** a volume with `backend: "s3"` and configured bucket+prefix
- **WHEN** `PUT /volumecontent/:vid/file?path=/x.txt` is called with body `b"hello"`
- **THEN** an object at `<prefix>/x.txt` SHALL exist in the bucket with
  body `b"hello"`
- **AND** the response SHALL be `201` with a stat JSON

#### Scenario: GET file on S3 volume reads from bucket

- **GIVEN** an object at `<prefix>/x.txt` exists in the bucket
- **WHEN** `GET /volumecontent/:vid/file?path=/x.txt` is called
- **THEN** the response SHALL stream the object body with `200`

### Requirement: Sandbox mount materialization

When a sandbox is created with `volumeMounts: [{name, path}]` referencing
an S3 volume, the e2b-shim SHALL materialize the configured prefix into
the session cwd at the mount path before returning success. Files
present in the bucket at create time SHALL be visible inside the sandbox.

#### Scenario: Seeded S3 file visible inside sandbox

- **GIVEN** an S3 volume with `<prefix>/data.txt` already in the bucket
- **WHEN** `POST /sandboxes` is called with `volumeMounts: [{name: "v", path: "/mnt"}]`
  pointing at that volume
- **THEN** `GET /files?path=/mnt/data.txt` with the new sandbox's id SHALL
  return the bucket content

### Requirement: Write-back mirror to bucket

When a sandbox writes a file under a mounted S3 volume path (via
`POST /files` or `Filesystem.Write`), e2b-shim SHALL mirror the write to
the S3 bucket at `<prefix>/<rel>` in addition to the existing scratch
mirror. Failures of the bucket put SHALL be logged and SHALL NOT cause
the originating write to fail.

#### Scenario: Sandbox write reaches the bucket

- **GIVEN** a running sandbox with an S3 volume mounted at `/mnt`
- **WHEN** the sandbox calls `POST /files?path=/mnt/out.txt` with body
  `b"sandbox"`
- **THEN** an object at `<prefix>/out.txt` SHALL exist in the bucket with
  body `b"sandbox"` within the request lifetime

#### Scenario: Bucket put failure does not break sandbox write

- **GIVEN** the S3 endpoint is unreachable
- **WHEN** the sandbox calls `POST /files?path=/mnt/x.txt`
- **THEN** the response SHALL be `200` (tools-rust write succeeded)
- **AND** the failure SHALL be logged

### Requirement: Delete mirror to bucket

When a sandbox deletes a path under a mounted S3 volume, e2b-shim SHALL
delete the corresponding object (or prefix, for directories) from the
bucket and from the scratch dir.

#### Scenario: Sandbox delete removes the bucket object

- **GIVEN** an S3 volume mounted at `/mnt`, with `<prefix>/old.txt` in
  the bucket
- **WHEN** the sandbox calls `Filesystem.Remove` for `/mnt/old.txt`
- **THEN** no object SHALL remain at `<prefix>/old.txt`
- **AND** the scratch dir SHALL no longer contain `old.txt`

### Requirement: Volume deletion preserves bucket contents

`DELETE /volumes/:vid` SHALL remove the registry record AND
`volume_fs_dir(vid)`. For S3 volumes, it SHALL NOT delete any objects
under `<prefix>` in the bucket. Bucket cleanup is an operator
responsibility.

#### Scenario: Volume delete leaves bucket intact

- **GIVEN** an S3 volume with several objects under `<prefix>`
- **WHEN** `DELETE /volumes/:vid` is called
- **THEN** the response SHALL be `204`
- **AND** the bucket's objects under `<prefix>` SHALL remain readable
  by other clients using the bucket

### Requirement: Consistency contract

The shim SHALL document and uphold these consistency guarantees for S3
volumes:

- Sandbox mount is a **create-time snapshot**. Bucket changes after
  sandbox create SHALL NOT auto-propagate into the running sandbox.
- Write-back SHALL be **eventually consistent** from the bucket's
  perspective: the sandbox sees its write succeed before the S3 put
  is acknowledged.
- Two sandboxes mounting the same prefix SHALL NOT share a live view.

These guarantees are weaker than local-disk volumes and SHALL be
explicitly mentioned in `README.md` and `docs/architecture.md`.
