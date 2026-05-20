# Mindbox

A coding-agent sandbox runtime in Rust. mindbox runs the seven canonical
agent tools — `read`, `write`, `edit`, `ls`, `grep`, `find`, `bash` —
inside isolated, cwd-scoped sessions, and an eighth trusted-only
`process` primitive for harness/SDK callers. It is deployed either as a
single image you give to a cloud platform (Volcano MLPlatform "开发机"),
or as a normal docker container on any VM with a docker daemon.

## What you get

```
client (curl / E2B SDK / your agent)
        │
        ▼
api-rust :8000  ──► PagedRegistry lazy-spawns one
                    inspect-tpl-tools-<name>:latest container
                    per template, joined to the mindbox-tools
                    docker network. Sessions stick to one daemon.
                            │
                            ▼
e2b-shim :8001        tools-rust daemon (inside template container)
(E2B SDK compat)        ├ sessions  uuid → /sandboxes/<sid>/
                        ├ 7 tools   inline Rust functions
                        └ 8th       process tool (gated, off by default)
```

For the full picture see [`docs/architecture.md`](docs/architecture.md).

## Quick start (any VM / laptop with docker)

```bash
docker pull ghcr.io/trotsky1997/mindbox:latest
docker run -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/trotsky1997/mindbox:latest

curl http://127.0.0.1:8000/v2/templates
curl -X POST http://127.0.0.1:8000/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}'
# → {"session_id":"...","cwd":"/sandboxes/..."}
```

For Volcano MLPlatform dev-instance deployments (the canonical
production form) and other shapes, see [`docs/deployment.md`](docs/deployment.md).

## Tool API

Everything lives at `POST /v2/sessions/:sid/tools/:name` with a JSON
body. Quick reference:

| Tool   | Body                                                                | Returns                                  |
|--------|---------------------------------------------------------------------|------------------------------------------|
| read   | `{path,offset?,limit?}`                                             | `{content,bytes}` (UTF-8 lossy)           |
| write  | `{path,content}`                                                    | `{bytes}`                                 |
| edit   | `{path,edits:[{oldText,newText}]}`                                  | `{replacements}` / 400 on ambiguous      |
| ls     | `{path?,limit?}`                                                    | `{entries:[{name,kind,size}]}`            |
| grep   | `{pattern,path?,glob?,ignoreCase?,literal?,context?,limit?}`        | `{matches/files/counts,walked,truncated}` |
| find   | `{pattern,path?,limit?}`                                            | `{paths,walked,truncated}`                |
| bash   | `{command,timeout?}`                                                | `{stdout,stderr,exit_code,timed_out}`     |

The 8th `process` tool is documented in
[`openspec/specs/tools-process-tool/spec.md`](openspec/specs/tools-process-tool/spec.md);
it is gated off by default (`TOOLS_PROCESS_ENABLED=0` + `TOOLS_EXPOSE_PROCESS=0`).

Compatibility: legacy field names (`bash.cmd`,
`edit.old_string/new_string/replace_all`, `grep.output_mode/max_files`,
`find.max_results`) are still accepted as deprecated aliases.

Path validation: absolute paths and `..` traversal are rejected at the
API boundary; everything is resolved relative to the session cwd.

## Templates

Each template is a docker image containing the tools-rust daemon plus
whatever toolchain that template needs. Configuration:
`templates/<name>/template.toml`.

Shipped:
- `tools-default` — debian-slim + bash/ripgrep/fd-find/coreutils
- `tools-python-dev` — `python:3.12-slim` + git + uv/pytest/rich
- `tools-node-dev` — `node:20-slim` + git

Each template image is built and pushed to GHCR by the `release-image`
workflow. The mindbox image's entrypoint runs `ensure-templates` on
startup to pull and retag them to the local
`inspect-tpl-tools-<name>:latest` names PagedRegistry expects.

To add a new template:

```bash
# inside any environment with template-build (e.g. the mindbox image)
template-build my-template
```

Reads `templates/my-template/template.toml`, generates a Dockerfile,
runs `docker build`. Optional `[warmup]` block in the toml runs
configured bash commands inside a transient session right after the
template's daemon comes up, so the first user-facing session benefits
from a primed page-cache.

## Isolation (opt-in)

`TOOLS_ISOLATION` on the daemon picks any subset of
`{chroot, seccomp, cgroup}`. All default off; layers silently no-op
when the host environment doesn't support them.

- **chroot** — bash subprocess chroots into a shared sub-rootfs the
  daemon prepares once.
- **seccomp** — bash subprocess gets `PR_SET_NO_NEW_PRIVS` plus a BPF
  filter denying privilege-escalation / namespace / OOB-IO syscalls.
- **cgroup** — per-session cgroup v2 with memory.max / cpu.max /
  pids.max defaults.

## Volumes

The e2b-shim implements the E2B SDK's `Volumes` API. A volume is a named
bytes-bag that survives across sandboxes; mounting one into a sandbox at
create time copies its contents into the session's cwd, and writes inside
the mount mirror back to the volume.

Two backends:

- **local** (default) — volume content lives in
  `/var/lib/e2b-shim/volumes/<volume_id>/` on the shim host. Reads and
  writes are local-disk.
- **s3** — authoritative content lives in an S3/TOS prefix. Same SDK
  surface; the only change is one extra field on `POST /volumes`:

  ```jsonc
  {
    "name": "my-vol",
    "backend": "s3",
    "s3": {
      "bucket": "mindbox-data",
      "prefix": "agents/run-0521",      // optional
      // endpoint/region/accessKey/secretKey optional;
      // fall back to TOS_* env (see docs/configuration.md)
    }
  }
  ```

  Sandboxes mounting an S3 volume see the prefix's state at create time
  (snapshot) and sandbox writes mirror back to the bucket. Consistency
  contract: create-time snapshot for reads, eventual-consistency for
  write-back. Two sandboxes sharing one prefix do NOT share a live view.

  S3 credentials/endpoint resolve in this order: per-volume body →
  `TOS_*` env. If neither layer has both AK/SK, `POST /volumes` returns
  `400 s3_unconfigured`.

  Deleting an S3 volume removes the registry record and the scratch
  cache; it does NOT touch any object in the bucket.

See [`openspec/changes/add-s3-volume-backend/proposal.md`] for the design
rationale and [`docs/configuration.md`] for the env knobs.

## Workspace

```
api-rust/         HTTP entry + PagedRegistry + /v2 forward
e2b-shim/         E2B SDK compat layer
template-builder/ template.toml → docker image CLI
tools-rust/       The actual 7-tool + process daemon
templates/        Template configurations (one .toml per template)
bench/            local micro-benchmarks (numpy matmul cold vs warm)
proto/envd/       E2B SDK Connect protobuf definitions
ops/devbox/       Volcano MLPlatform helper scripts
scripts/          image-side entrypoint, registry helpers
docs/
  architecture.md     full component / lifetime model
  deployment.md       Volcano MLP / VM / local — step by step
  configuration.md    every env knob the runtime reads
  operations.md       runbook: provision, restart, inspect, tear down
  troubleshooting.md  known limitations and recovery
openspec/
  specs/tools-process-tool/spec.md  spec for the 8th tool
  changes/archive/                  historical change proposals
.github/workflows/
  ci.yml          cargo fmt + clippy + test on every push
  release-image.yml  mindbox image + tpl-tools-* images → GHCR
```

## License

Apache-2.0. See LICENSE.
