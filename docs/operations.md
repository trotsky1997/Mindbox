# Operations runbook

How to provision, restart, inspect, and tear down a mindbox deployment.
Most steps below target the Volcano MLPlatform "开发机" product because
that's the only environment with bespoke ops. For plain VM / ECS, normal
`docker` / `systemctl` workflows apply.

## Volcano MLPlatform dev-instances

The helper scripts under `ops/devbox/` wrap
`volcenginesdkmlplatform20240701` so you don't have to remember the
SDK incantations. Always export AK/SK first:

```bash
export VOLC_ACCESS_KEY_ID=...
export VOLC_SECRET_ACCESS_KEY=...
export VOLC_REGION=cn-beijing
```

### List

```bash
ops/devbox/list_dev_instances.py
```

Prints id, name, image, state for each dev-instance, then dumps the
first one in full JSON so you can copy fields when authoring a new one.

### Create

```bash
ops/devbox/create_dev_instance.py
```

Creates a fresh `mindbox-smoke-<ts>` dev-instance mirroring the spec of
the original mindbox dev-instance (image, queue, zone, instance type,
trusted SSH keys, exposed 8000 + 8001 ports).

Output:

```
created id = di-2026XXXXXXXXX-XXXXX
[t1] state=Pending
[t2] state=Deploying
[t3] state=Running
  port rust-api: 192.168.X.Y:8000  public=115.190.235.210:NNNNN  state=Available
  port e2b:      192.168.X.Y:8001  public=115.190.235.210:NNNNN  state=Available
```

The platform doesn't add an SSH port by default if you don't ask. Use
`add_ssh_port.py` next.

### Add SSH access to an instance

```bash
DEV_ID=di-... ops/devbox/add_ssh_port.py
```

Adds a `SSH连接` (2222) port plus a `WebIDE` (10000) port to the
existing instance and polls until the platform allocates external ports.
The script writes the SSH command line at the end.

### Manual start

Because the platform overrides the image's ENTRYPOINT, you have to start
mindbox by hand after SSH'ing in:

```bash
ssh -p <ssh-port> root@115.190.235.210

# Pull / retag template images so PagedRegistry can find them
for t in tools-default tools-python-dev tools-node-dev; do
  docker pull ghcr.io/trotsky1997/mindbox/tpl-$t:latest
  docker tag  ghcr.io/trotsky1997/mindbox/tpl-$t:latest inspect-tpl-tools-$t:latest
done

# Process tool gates (omit both for agent-facing deployments)
export TOOLS_PROCESS_ENABLED=1
export TOOLS_EXPOSE_PROCESS=1

# Boot mindbox
nohup /usr/local/bin/api-rust >/tmp/api-rust.log 2>&1 &
echo $! > /tmp/api-rust.pid

nohup /usr/local/bin/e2b-shim >/tmp/e2b-shim.log 2>&1 &
echo $! > /tmp/e2b-shim.pid

# Wait for /health
for i in $(seq 1 60); do
  curl -fsS http://127.0.0.1:8000/health >/dev/null 2>&1 && break
  sleep 1
done
curl -sS http://127.0.0.1:8000/health
```

### Autostart after reboot

The dev-instance has a writable filesystem layer the platform persists
across `start`/`stop` cycles. A `systemd --user` unit doesn't work
(dev-instance init isn't systemd), but you can drop a bash startup hook
into `~/.bashrc` or `/etc/profile.d/`:

```bash
cat > /etc/profile.d/01-mindbox.sh <<'SH'
# Start mindbox on first interactive login if it isn't already up.
if [ -t 1 ] && ! pgrep -f /usr/local/bin/api-rust >/dev/null; then
    nohup /usr/local/bin/api-rust >/tmp/api-rust.log 2>&1 &
    nohup /usr/local/bin/e2b-shim >/tmp/e2b-shim.log 2>&1 &
fi
SH
chmod +x /etc/profile.d/01-mindbox.sh
```

For a less ssh-coupled approach, you can also patch the platform's init
script (look under `/usr/local/bin/` for the file that runs sshd; many
MLP images expose it). YMMV depending on platform version.

### Stop / restart instance

Programmatically:

```bash
ops/devbox/stop_dev_instance.py  DEV_ID=di-...
ops/devbox/start_dev_instance.py DEV_ID=di-...
```

These are thin wrappers over `StopDevInstance` and `StartDevInstance`.
They don't restart the mindbox process inside; do that via SSH +
autostart hook described above.

### Delete an instance

```bash
ops/devbox/delete_dev_instance.py DEV_ID=di-...
```

Tears down the dev-instance entirely. Reclaims its public ports. Don't
do this to `di-20260519182002-4fj4l` (the canonical mindbox dev-instance).

## Plain VM / ECS

Standard `docker` operations apply. A few mindbox-flavoured recipes:

```bash
# Tail controller logs
docker logs -f mindbox

# Restart cleanly
docker restart mindbox

# Replace with a newer image
docker pull ghcr.io/trotsky1997/mindbox:latest
docker rm -f mindbox
docker run -d --name mindbox \
  -p 8000:8000 -p 8001:8001 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TOOLS_PROCESS_ENABLED=1 \
  -e TOOLS_EXPOSE_PROCESS=1 \
  ghcr.io/trotsky1997/mindbox:latest

# Prune orphaned tools containers from a previous run
docker ps -aq --filter label=inspect-api-hot=1 | xargs -r docker rm -f
```

## Operational dashboards

The controller exposes two introspection endpoints:

```bash
curl http://<host>:8000/stats
# {
#   "configs_total": 3,
#   "hot": ["tools-default"],
#   "warm": []
# }

curl http://<host>:8000/v2/templates
# {
#   "templates":["tools-default","tools-python-dev","tools-node-dev"],
#   "fallback_daemon": null
# }
```

Use these in a uptime probe / health alert: `/health` for liveness,
`/stats` to track how many templates are hot.

## Image / template rebuild

The `release-image` GitHub workflow rebuilds `ghcr.io/trotsky1997/mindbox`
and `ghcr.io/trotsky1997/mindbox/tpl-tools-*` on every push to main.
Triggering it manually:

```bash
gh workflow run release-image --ref main
```

Once it succeeds, on each running deployment:

```bash
docker pull ghcr.io/trotsky1997/mindbox:latest
docker pull ghcr.io/trotsky1997/mindbox/tpl-tools-default:latest
docker pull ghcr.io/trotsky1997/mindbox/tpl-tools-python-dev:latest
docker pull ghcr.io/trotsky1997/mindbox/tpl-tools-node-dev:latest
```

Restart the controller (`docker restart mindbox` on VM, or kill +
re-`nohup` on dev-instance) to pick up the new code.

## Logs to grep when something's off

| Symptom | Where to look | What to look for |
|---|---|---|
| api-rust unreachable | `/tmp/api-rust.log` (dev-instance) or `docker logs mindbox` | `listening on 0.0.0.0:8000`, panics |
| sessions return 404 immediately | api-rust log | `template '…' not found`, `not backed by a tools daemon` |
| tools daemon never goes hot | api-rust log | `tools daemon at … not ready in 30s` |
| process tool refuses | api-rust log | `process_forbidden` lines tell you the forward gate is off |
| Sluggish on Cold→Hot | api-rust log | Look for `warmup start` / `warmup ok` / `warmup … failed` lines |

For broader failure cases see [troubleshooting.md](troubleshooting.md).
