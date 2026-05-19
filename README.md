# Mindbox

A 7-tool agent sandbox runtime in Rust. Sessions live in cheap cwd
directories on a daemon; each session can call `read`, `write`, `edit`,
`ls`, `grep`, `find`, `bash` — the 7 built-in tools an LLM coding agent
typically needs. No Python interpreter pool, no fork prewarm, no
SCM_RIGHTS fd-passing.

## Architecture

```
client (HTTP / e2b SDK)
        │
        ▼
   api-rust :8000  ─────────────────────────► tools-rust daemon
   (PagedRegistry:                            (inside per-template container)
    lazy-spawn tools                            ├ /sessions       (CRUD)
    template containers,                        ├ /tools/read
    hot/warm/cold LRU,                          ├ /tools/write
    template name → daemon URL                  ├ /tools/edit
    + session sticky)                           ├ /tools/ls
                                                ├ /tools/grep
   e2b-shim :8001                               ├ /tools/find
   (E2B SDK compat: sandboxes,                  └ /tools/bash
    commands.run, files read/write)
```

* **api-rust** — HTTP entry on :8000. PagedRegistry lazy-spawns a
  tools-rust container per template on first use, manages hot/warm/cold
  LRU tiers, and reverse-proxies `/v2/sessions/*` calls to the right
  daemon.
* **e2b-shim** — E2B SDK compatibility layer on :8001. SDK clients use
  it as a drop-in cloud API; `commands.run()`, `files.read/write()`
  forward to api-rust `/v2`. `filesystem.list/stat/mkdir/move` return
  501 (use the tools API directly or `commands.run` with bash).
* **tools-rust** — the daemon that lives inside each template container.
  Single tokio multi-thread process serves N concurrent sessions; tools
  run as inline Rust functions, `bash` spawns a subprocess.
* **template-builder** — CLI baked into the mindbox image. Reads
  `templates/<name>/template.toml`, generates a Dockerfile, runs
  `docker build`, optionally pushes to a registry.

## Templates

Each template is a docker image containing the tools-rust daemon and
whatever toolchain the agent needs. Configuration lives in
`templates/<name>/template.toml`.

Shipped:
* `tools-default` — debian-slim with bash/ripgrep/fd-find/coreutils
* `tools-python-dev` — `python:3.12-slim` + git + uv/pytest/rich
* `tools-node-dev` — `node:20-slim` + git

Image tag: `inspect-tpl-tools-<name>:latest`. Build a new template with
`template-build <name>` (run from the mindbox image, with templates/
mounted).

## Quick start

```bash
docker run -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $PWD/templates:/opt/inspect-api/templates:ro \
  ghcr.io/trotsky1997/mindbox:latest

# Pull a template image so api-rust can lazy-spawn it
docker pull ghcr.io/trotsky1997/mindbox/tpl-tools-default:latest
docker tag  ghcr.io/trotsky1997/mindbox/tpl-tools-default:latest \
            inspect-tpl-tools-tools-default:latest

# Use the API
curl -s -X POST http://127.0.0.1:8000/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}'
# → {"session_id":"...","cwd":"/sandboxes/..."}

curl -s -X POST http://127.0.0.1:8000/v2/sessions/<sid>/tools/bash \
  -H 'Content-Type: application/json' \
  -d '{"command":"echo hi"}'
```

## Tool API

All seven tools share the same shape — `POST /v2/sessions/:sid/tools/:name`
with a JSON body. See `tools-rust/src/main.rs` for the exact request/
response structs. Quick reference:

| Tool   | Body                                                                | Returns                                  |
|--------|---------------------------------------------------------------------|------------------------------------------|
| read   | `{path,offset?,limit?}`                                             | `{content,bytes}` (UTF-8 lossy)           |
| write  | `{path,content}`                                                    | `{bytes}`                                 |
| edit   | `{path,edits:[{oldText,newText}]}`                                  | `{replacements}` / 400 on ambiguous      |
| ls     | `{path?,limit?}`                                                    | `{entries:[{name,kind,size}]}`            |
| grep   | `{pattern,path?,glob?,ignoreCase?,literal?,context?,limit?}`        | `{matches/files/counts,walked,truncated}` |
| find   | `{pattern,path?,limit?}`                                            | `{paths,walked,truncated}`                |
| bash   | `{command,timeout?}`                                                | `{stdout,stderr,exit_code,timed_out}`     |

Compatibility: legacy Mindbox fields are still accepted as deprecated aliases:
`bash.cmd`, `edit.old_string/new_string/replace_all`, `grep.output_mode/max_files`,
and `find.max_results`.

Path validation: absolute paths and `..` traversal are rejected at the
API boundary; everything is resolved relative to the session cwd.

## Isolation (opt-in)

`TOOLS_ISOLATION` env on the daemon picks any subset of
`{chroot, seccomp, cgroup}`. All default off; layers silently no-op
when the host environment doesn't support them (e.g. cgroup v2 not
mounted, no `CAP_SYS_ADMIN`).

* **chroot** — bash subprocess chroots into a shared sub-rootfs the
  daemon prepares once (bash + common coreutils + ldd-resolved libs).
* **seccomp** — bash subprocess gets `PR_SET_NO_NEW_PRIVS` + a BPF
  filter that denies a curated list of privilege-escalation / namespace
  / OOB-IO syscalls (mount, ptrace, setuid family, capset, bpf, etc.).
* **cgroup** — per-session cgroup v2 with memory.max / cpu.max /
  pids.max defaults.

## Workspace

```
api-rust/         HTTP entry + PagedRegistry + /v2 forward
e2b-shim/         E2B SDK compat layer
template-builder/ template.toml → docker image CLI
tools-rust/       The actual 7-tool daemon
templates/        Template configurations (one .toml per template)
proto/envd/       E2B SDK Connect protobuf definitions
.github/workflows/
  ci.yml          cargo fmt + clippy + test on every push
  release-image.yml mindbox image + all tpl-tools-* images → ghcr
```

## License

Apache-2.0. See LICENSE.
