#!/usr/bin/env python3
"""Create a fresh MLPlatform dev-instance running the mindbox image.

Mirrors the canonical mindbox dev-instance (queue, zone, instance type,
volume) but with a unique name. The instance comes up without an SSH
port; run `add_ssh_port.py` afterwards if you need to log in.

Usage:
    VOLC_ACCESS_KEY_ID=... VOLC_SECRET_ACCESS_KEY=... \
    ops/devbox/create_dev_instance.py [name-suffix]
"""
from __future__ import annotations

import os
import sys
import time

from _common import (
    DEFAULT_INSTANCE_TYPE_ID,
    DEFAULT_RESOURCE_QUEUE_ID,
    DEFAULT_VOLUME_GIB,
    DEFAULT_VOLUME_TYPE_ID,
    DEFAULT_ZONE_ID,
    MINDBOX_IMAGE_URL,
    TRUSTED_SSH_KEYS,
    configure_sdk,
)


def main() -> int:
    configure_sdk()
    import volcenginesdkmlplatform20240701 as mlp  # type: ignore[import-not-found]

    suffix = sys.argv[1] if len(sys.argv) > 1 else str(int(time.time()))
    name = f"mindbox-{suffix}"

    api = mlp.MLPLATFORM20240701Api()
    req = mlp.CreateDevInstanceRequest(
        name=name,
        description="auto: mindbox controller dev-instance",
        project_name=os.environ.get("VOLC_PROJECT", "default"),
        image=mlp.ImageForCreateDevInstanceInput(
            type="Public",
            url=os.environ.get("MINDBOX_IMAGE_URL", MINDBOX_IMAGE_URL),
        ),
        resource_queue_id=os.environ.get("VOLC_QUEUE", DEFAULT_RESOURCE_QUEUE_ID),
        resource_claim=mlp.ResourceClaimForCreateDevInstanceInput(
            type="Preset",
            instance_type_id=os.environ.get("VOLC_INSTANCE_TYPE", DEFAULT_INSTANCE_TYPE_ID),
            zone_id=os.environ.get("VOLC_ZONE", DEFAULT_ZONE_ID),
        ),
        node_affinity_spec=mlp.NodeAffinitySpecForCreateDevInstanceInput(
            gpucpu_node_preference="CPURequired",
            strategy_type="Queue",
        ),
        volume=mlp.VolumeForCreateDevInstanceInput(
            size=int(os.environ.get("VOLC_VOLUME_GIB", DEFAULT_VOLUME_GIB)),
            volume_type_id=os.environ.get("VOLC_VOLUME_TYPE", DEFAULT_VOLUME_TYPE_ID),
        ),
        default_folder="/root/code",
        ssh_public_key=TRUSTED_SSH_KEYS,
        ports=[
            mlp.PortForCreateDevInstanceInput(
                name="rust-api",
                type="custom",
                internal_port=8000,
                enable_public_network_access=True,
            ),
            mlp.PortForCreateDevInstanceInput(
                name="e2b",
                type="custom",
                internal_port=8001,
                enable_public_network_access=True,
            ),
        ],
    )

    print(f"--- creating dev-instance {name} ---")
    resp = api.create_dev_instance(req)
    new_id = resp.id
    print(f"created id = {new_id}")

    deadline = time.time() + int(os.environ.get("WAIT_SEC", "600"))
    last = None
    get_req = mlp.GetDevInstanceRequest(id=new_id)
    while time.time() < deadline:
        got = api.get_dev_instance(get_req)
        state = getattr(got.status, "state", None)
        if state != last:
            print(f"[{int(time.time())}] state={state}")
            last = state
        if state == "Running":
            for p in got.ports or []:
                if p.external_ip and p.external_port:
                    print(
                        f"  port {p.name}: {p.eni_ip}:{p.eni_port}  "
                        f"public={p.external_ip}:{p.external_port}  "
                        f"state={getattr(p.status, 'state', None)}"
                    )
            break
        if state == "Failed":
            print("FAILED:", got.status)
            return 1
        time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
