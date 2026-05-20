# Troubleshooting

Catalog of the failure modes we've hit, what the symptom looks like, and
the fix.

## Volcano MLPlatform dev-instance

### Symptom: image entrypoint doesn't run, no listener on `:8000`

```bash
$ curl http://<dev-instance-public-ip>:<api-port>/health
curl: (52) Empty reply from server          # or 502 Bad Gateway
```

**Why.** MLPlatform's "开发机" product replaces the container's
`ENTRYPOINT` with its own init that brings up `sshd` and a WebIDE. The
mindbox entrypoint script never executes.

**Fix.** SSH in and start mindbox by hand:

```bash
ssh -p <ssh-port> root@115.190.235.210
nohup /usr/local/bin/api-rust >/tmp/api-rust.log 2>&1 &
nohup /usr/local/bin/e2b-shim >/tmp/e2b-shim.log 2>&1 &
```

Persistent across reboots: see [operations.md → autostart](operations.md#autostart-after-reboot).

### Symptom: `docker run -v /var/run/docker.sock:/var/run/docker.sock` rejected

```
docker: Error response from daemon: failed to open path
        /ebs/rootfs/var/run/docker.sock: open ...: no such device or address.
```

**Why.** Inside a Volcano dev-instance you are already in a container.
The platform's docker proxy (`/var/run/docker.sock` is a symlink to
`/var/run/user/proxy.sock`) refuses to mount itself into a child
container, because doing so would hand daemon control to a nested
container.

**Fix.** Don't run mindbox as a nested container in the dev-instance.
Either:
- Run `api-rust` / `e2b-shim` as host processes of the dev-instance
  (they `connect()` the existing `/var/run/docker.sock` directly, which
  the proxy allows).
- Or deploy on a normal VM / ECS, where `-v sock:sock` is allowed.

The dev-instance can still call `docker run <image>` to spawn sibling
containers — that's how api-rust's PagedRegistry lazy-spawns tools
templates. The proxy specifically blocks the `-v sock:sock` recipe.

### Symptom: `docker pull` works, but `inspect-tpl-tools-…` image missing

```
template '…' not found: template ... tools image inspect-tpl-tools-… not built
```

**Why.** PagedRegistry expects the local tag `inspect-tpl-tools-<name>:latest`.
The published GHCR images use a different naming (`tpl-<name>:latest`),
and the entrypoint normally retags them via `ensure-templates`. If you
launched the binaries manually, that step ran (it's separate), but if
you bypassed the entrypoint entirely you have to retag yourself.

**Fix.**

```bash
for t in tools-default tools-python-dev tools-node-dev; do
  docker pull ghcr.io/trotsky1997/mindbox/tpl-$t:latest
  docker tag  ghcr.io/trotsky1997/mindbox/tpl-$t:latest inspect-tpl-tools-$t:latest
done
```

## glibc

### Symptom: binary scp'd from your laptop won't run on the dev-instance

```
./api-rust: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.39' not found
```

**Why.** Your laptop has a newer glibc (e.g. 2.39) than the dev-instance
(Debian 12, glibc 2.36).

**Fix.** Don't `cargo build` locally and scp. Pull the image instead and
extract the binary:

```bash
docker pull ghcr.io/trotsky1997/mindbox:latest
docker create --name x ghcr.io/trotsky1997/mindbox:latest
docker cp x:/usr/local/bin/api-rust ./api-rust
docker rm x
scp ./api-rust root@<host>:/usr/local/bin/
```

The published image is built on Debian 12, matching the dev-instance.

## Sticky daemon URL / session not found

### Symptom: `session not found` immediately after a successful create

```
client: POST /v2/sessions {"template":"tools-pair"} → {"session_id":"abc..."}
client: POST /v2/sessions/abc.../tools/bash → "session not found"
```

**Why.** Old behavior (pre `09158fd`) — api-rust stored only the
template name for each session and round-robin'd a fresh daemon URL on
every request. With `containers > 1`, the second request landed on a
different daemon than the one that created the session.

**Fix.** Already fixed upstream. The forward now records the chosen
daemon URL at session-create time and reuses it for the session's
lifetime. If you still see this, you're running an old image; pull the
latest `ghcr.io/trotsky1997/mindbox:latest`.

## Process tool

### Symptom: process route returns `403 process_forbidden`

**Why.** `TOOLS_EXPOSE_PROCESS` is unset on api-rust. The forward gate
hides the process tool from clients by default.

**Fix.**

```bash
docker run -e TOOLS_EXPOSE_PROCESS=1 -e TOOLS_PROCESS_ENABLED=1 ... mindbox
```

Or on the dev-instance shell, export both before `api-rust`.

### Symptom: process route returns `404 Not Found`

Two possibilities:

1. `TOOLS_PROCESS_ENABLED` is unset on the daemon (the daemon-side kill
   switch). Set to 1 on the tools-rust container env.
2. The `process_id` belongs to a session that no longer exists (deleted,
   idle-reaped, daemon shutdown). This is the **expected behavior**;
   `process_id` is strictly session-scoped.

### Symptom: process returns `501 process_unsupported`

You explicitly set `TOOLS_PROCESS_FORCE_UNSUPPORTED=1`, or the daemon
self-flagged unsupported (rare, requires a runtime invariant failure).
Unset the env or fix the underlying issue.

## Networking between mindbox and tools containers

### Symptom: tools daemon never becomes ready (timeout in PagedRegistry)

```
[paged] tools daemon at http://172.18.0.X:8002 not ready in 30s
```

**Why.** mindbox can't reach the tools container's `:8002`. Usually
because the controller is on a different docker network than
`mindbox-tools`.

**Fix.**
- If mindbox is itself a container, ensure it self-attaches: the log
  should show `[paged] self-attached <hostname> to mindbox-tools`.
  Missing means the daemon refused the connect (could be permissions or
  the controller isn't a real docker container; on a dev-instance it
  usually still works because the controller's network already routes
  to the bridge).
- If mindbox is a host process, the `172.18.x.x` subnet must be
  routable. Standard docker installs route this automatically.
- For weird setups, override `TOOLS_CONTAINER_NETWORK_MODE=host` and
  hardcode the daemon URL. Loses isolation; only use as a temporary
  unblock.

## Idle reaper killed my session

### Symptom: session abruptly returns 404 hours into a run

**Why.** Default `TOOLS_SESSION_IDLE_REAP_SEC=3600` evicts sessions
without any tool call for an hour. The reaper kills any owned processes
on the way out.

**Fix.** Either bump the env to a higher value, or send a no-op tool
call (any `ls /` is enough) before the timer expires to refresh
`last_touched`.

## Diagnostic commands

Quick probes you'll want, in order of intrusiveness:

```bash
# 1. Is the api-rust process running and listening?
ss -lntp | grep ':8000'    # or netstat -ltnp

# 2. Does it report its own state?
curl -sS http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/stats
curl -sS http://127.0.0.1:8000/v2/templates

# 3. What sibling containers does the controller see?
docker ps --filter label=inspect-api-hot=1

# 4. Is the user-defined network there with the expected members?
docker network inspect mindbox-tools

# 5. Tail the controller log
tail -f /tmp/api-rust.log    # or wherever you redirected stdout
```

For deeper inspection, every tools-rust daemon also responds to
`/sessions` / `/sessions/:id/...` directly on its container IP, which
you can hit from inside any container also on the `mindbox-tools`
network.
