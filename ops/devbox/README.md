# ops/devbox

Helper scripts for provisioning / lifecycle-managing Volcano MLPlatform
dev-instances that host mindbox. All scripts share env-driven credential
loading and a few sane defaults (image, queue, zone, instance type)
captured from the canonical mindbox dev-instance.

## Prereqs

```bash
pip install volcengine-python-sdk
export VOLC_ACCESS_KEY_ID=...
export VOLC_SECRET_ACCESS_KEY=...
export VOLC_REGION=cn-beijing  # default; override only if AK/SK is for another region
```

The AK/SK come from 火山引擎 → 访问控制 → API 访问密钥.

## Scripts

| Script | Purpose |
|---|---|
| `list_dev_instances.py` | List dev-instances in the current project. Also dumps the first one as full JSON so you can pick fields to copy into a new spec. |
| `create_dev_instance.py [suffix]` | Provision a fresh `mindbox-<suffix>` dev-instance with the standard spec. Defaults to a CPU 32vCPU/128GiB node. |
| `add_ssh_port.py` | Add `SSH连接` (2222) + `WebIDE` (10000) to an existing instance. Required if you forgot to declare SSH at create time. |
| `lifecycle_dev_instance.py` | Stop/start/delete a dev-instance by id. |

## Common workflows

### Bring up a new mindbox dev-instance from scratch

```bash
ops/devbox/create_dev_instance.py
# wait for "state=Running", note the printed rust-api / e2b external ports

# get SSH access on the new instance (using its DEV_ID from above)
DEV_ID=di-2026... ops/devbox/add_ssh_port.py

# SSH in and manually boot mindbox; see docs/operations.md → "Manual start"
```

### Recycle an existing instance

```bash
DEV_ID=di-... ACTION=stop   ops/devbox/lifecycle_dev_instance.py
DEV_ID=di-... ACTION=start  ops/devbox/lifecycle_dev_instance.py
```

Note that `stop`/`start` preserve the instance's identity, network
addresses, attached volumes, and SSH keys. It only restarts the
container. The mindbox process itself doesn't auto-resume — re-SSH in.

### Tear down

```bash
DEV_ID=di-... ACTION=delete ops/devbox/lifecycle_dev_instance.py
```

Release ports + storage. Irreversible.

## Per-instance overrides

`create_dev_instance.py` reads optional env overrides for non-default
deployments:

| Env | Default | Use when |
|---|---|---|
| `MINDBOX_IMAGE_URL` | `ghcr.io/trotsky1997/mindbox:latest` | Trying a private fork or a specific tag |
| `VOLC_QUEUE` | (canonical CPU queue) | Different resource queue |
| `VOLC_INSTANCE_TYPE` | `ml.g3a.8xlarge` | Different node spec |
| `VOLC_ZONE` | `cn-beijing-c` | Different AZ |
| `VOLC_VOLUME_GIB` | `50` | More room |
| `VOLC_VOLUME_TYPE` | `ml.essd.pl0` | Different storage class |
| `VOLC_PROJECT` | `default` | Different MLPlatform project |
| `WAIT_SEC` | `600` | Override the polling timeout |

For the SDK contract behind these calls, see
[`docs/operations.md`](../../docs/operations.md).
