# Deployment

Three shapes are supported, in order of "most managed → most DIY":

1. [Volcano MLPlatform dev-instance](#1-volcano-mlplatform-dev-instance)
2. [Plain VM / ECS with docker](#2-plain-vm--ecs-with-docker)
3. [Local dev / smoke](#3-local-dev--smoke)

All three run the same image: `ghcr.io/trotsky1997/mindbox:latest`.

## 1. Volcano MLPlatform dev-instance

The MLPlatform "开发机" product takes a container image and runs it for
you on a managed Volcano backend. You SSH in to a long-lived shell; the
platform handles network ingress, SSH key trust, volume mounts.

### Caveats specific to this platform

- **The image's `ENTRYPOINT` is replaced** by Volcano's init. You will
  have to start `api-rust` and `e2b-shim` manually after each
  dev-instance launch (see [operations.md](operations.md#manual-start)).
- **Nested `-v docker.sock` is rejected** by the Volcano docker proxy.
  Don't try to run mindbox as `docker run -v sock:sock mindbox` inside
  the dev-instance. Run the mindbox binaries directly as host processes
  of the dev-instance; that path mounts the proxy's docker.sock by
  default and is allowed.
- **Ports must be declared up front** as `custom` ports on the
  dev-instance spec, or added later via `update_dev_instance`. Use
  `internal_port=8000` (api-rust) and `internal_port=8001` (e2b-shim);
  Volcano allocates random external ports on a shared NAT IP.

### Provisioning via SDK

The recipe is the same for every new dev-instance. Helper scripts live
under [`ops/`](../ops):

```bash
export VOLC_ACCESS_KEY_ID=...
export VOLC_SECRET_ACCESS_KEY=...
export VOLC_REGION=cn-beijing

ops/devbox/create_dev_instance.py        # creates a new mindbox dev-instance
ops/devbox/add_ssh_port.py    DEV_ID=…   # adds SSH 2222 port to an existing one
ops/devbox/list_dev_instances.py         # lists every dev-instance in the project
```

After `create_dev_instance.py` returns, the external public ports for
`rust-api` (8000) and `e2b` (8001) are printed.

### Bringing mindbox up

Once you can SSH in:

```bash
ssh -p <ssh-external-port> root@115.190.235.210
```

then on the dev-instance shell:

```bash
# Required env: pick whichever templates you want enabled.
export INSPECT_API_TEMPLATES_DIR=/opt/inspect-api/templates
export PORT=8000
# Optional but recommended on a private deployment:
export TOOLS_PROCESS_ENABLED=1   # 8th tool, daemon-side gate
export TOOLS_EXPOSE_PROCESS=1    # 8th tool, forward-side gate

nohup api-rust    >/tmp/api-rust.log    2>&1 &
nohup e2b-shim    >/tmp/e2b-shim.log    2>&1 &
```

Then from your laptop:

```bash
curl http://115.190.235.210:<external-api-port>/health
curl -X POST http://115.190.235.210:<external-api-port>/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}'
```

If you want this to come back automatically after a dev-instance reboot,
see [operations.md → autostart](operations.md#autostart-after-reboot).

## 2. Plain VM / ECS with docker

Any Linux VM with docker installed runs mindbox unchanged. This is the
simplest topology and the only one where the image's own ENTRYPOINT
actually drives the lifecycle.

```bash
ssh root@<vm-ip>
apt update && apt install -y docker.io
systemctl enable --now docker

# Sanity check: confirm the daemon accepts sock mounts (Volcano dev-container
# does not, normal VMs do).
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock alpine ls /var/run/docker.sock

docker pull ghcr.io/trotsky1997/mindbox:latest
docker run -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TOOLS_PROCESS_ENABLED=1 \
  -e TOOLS_EXPOSE_PROCESS=1 \
  ghcr.io/trotsky1997/mindbox:latest
```

The container's entrypoint:
1. Calls `ensure-templates` — pulls `inspect-tpl-tools-<name>:latest` from
   `MINDBOX_TEMPLATE_REGISTRY` (default `ghcr.io/trotsky1997/mindbox`),
   falls back to building locally with `template-build`.
2. Starts `api-rust` and `e2b-shim` supervised by the entrypoint script.
3. PagedRegistry lazy-spawns the per-template tools containers on first
   request and attaches them to the `mindbox-tools` user-defined network.

Verify from the host:

```bash
curl http://localhost:8000/v2/templates
curl -X POST http://localhost:8000/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}'
```

### Exposing a public port

Use whatever you'd use for any HTTP service. Examples:

- Cloud firewall: open inbound TCP 8000 to `0.0.0.0/0` (or a trusted CIDR).
- Reverse proxy: put nginx/caddy in front of `:8000` with TLS.

`/v2/sessions/.../tools/process` is gated by `TOOLS_EXPOSE_PROCESS`; with
that env unset, the public surface is exactly the seven canonical tools.

## 3. Local dev / smoke

For one-shot validation on your laptop:

```bash
cd /path/to/mindbox
docker build -t mindbox:dev -f Dockerfile .
docker run --rm -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  mindbox:dev
```

A representative smoke run (mirrors the integration check used during
release verification):

```bash
SID=$(curl -sS -X POST http://127.0.0.1:8000/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"template":"tools-default"}' \
  | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
echo "sid=$SID"

# A bash echo
curl -sS -X POST "http://127.0.0.1:8000/v2/sessions/$SID/tools/bash" \
  -H 'Content-Type: application/json' \
  -d '{"command":"echo hi from $(hostname)"}'

# Process tool (only if TOOLS_PROCESS_ENABLED=1 + TOOLS_EXPOSE_PROCESS=1)
START=$(curl -sS -X POST "http://127.0.0.1:8000/v2/sessions/$SID/tools/process" \
  -H 'Content-Type: application/json' \
  -d '{"action":"start","command":"/bin/sh","args":["-c","echo alive; exit 7"]}')

# Delete
curl -sS -X DELETE "http://127.0.0.1:8000/v2/sessions/$SID"
```

For micro-benchmarks (numpy matmul cold vs warm), see [`bench/`](../bench).

## Common cross-cutting concerns

- **glibc.** All shipped binaries are built against the mindbox image's
  Debian 12 base (glibc 2.36). If you `cargo build` mindbox on your
  laptop and `scp` the binary to a dev-instance, you can run into
  `GLIBC_2.39 not found`. **Don't.** Pull the image, extract the binary
  with `docker cp`. See [troubleshooting.md](troubleshooting.md#glibc).
- **Template caching.** `MINDBOX_TEMPLATE_REGISTRY` defaults to
  `ghcr.io/trotsky1997/mindbox`; override to use a private CR. The
  controller pulls `tpl-<name>:latest` and re-tags it as the local
  `inspect-tpl-tools-<name>:latest` that PagedRegistry expects.
- **Idle reaper.** Sessions abandoned for more than
  `TOOLS_SESSION_IDLE_REAP_SEC` (default 1h) are reaped along with all
  their processes. Tweak per deployment.
- **Multi-template, multi-instance.** A `containers > 1` template gets N
  sibling daemons in PagedRegistry; sessions are sticky to one of them.
