#!/usr/bin/env python3
"""Add system SSH (2222) and WebIDE (10000) ports to an existing dev-instance.

The MLPlatform CreateDevInstance API does not auto-add system ports if
you only declared custom ones. This script patches them in via
UpdateDevInstance.

Usage:
    DEV_ID=di-... VOLC_ACCESS_KEY_ID=... VOLC_SECRET_ACCESS_KEY=... \
    ops/devbox/add_ssh_port.py
"""
from __future__ import annotations

import os
import sys
import time

from _common import TRUSTED_SSH_KEYS, configure_sdk


def main() -> int:
    configure_sdk()
    import volcenginesdkmlplatform20240701 as mlp  # type: ignore[import-not-found]

    dev_id = os.environ.get("DEV_ID")
    if not dev_id:
        sys.stderr.write("DEV_ID env var is required.\n")
        return 2

    api = mlp.MLPLATFORM20240701Api()
    got = api.get_dev_instance(mlp.GetDevInstanceRequest(id=dev_id))

    existing = [
        mlp.PortForUpdateDevInstanceInput(
            name=p.name,
            type=p.type,
            internal_port=p.eni_port,
            enable_public_network_access=p.enable_public_network_access,
        )
        for p in got.ports or []
    ]
    have = {p.name for p in existing}
    if "SSH连接" not in have:
        existing.append(
            mlp.PortForUpdateDevInstanceInput(
                name="SSH连接",
                type="system",
                internal_port=2222,
                enable_public_network_access=True,
            )
        )
    if "WebIDE" not in have:
        existing.append(
            mlp.PortForUpdateDevInstanceInput(
                name="WebIDE",
                type="system",
                internal_port=10000,
                enable_public_network_access=True,
            )
        )

    api.update_dev_instance(
        mlp.UpdateDevInstanceRequest(
            id=dev_id,
            ports=existing,
            ssh_public_key=TRUSTED_SSH_KEYS,
        )
    )
    print(f"update ok; polling {dev_id} for external SSH port...")

    deadline = time.time() + 180
    while time.time() < deadline:
        got = api.get_dev_instance(mlp.GetDevInstanceRequest(id=dev_id))
        ssh = next((p for p in (got.ports or []) if p.name == "SSH连接"), None)
        if ssh and ssh.external_ip and ssh.external_port:
            print(f"ssh -p {ssh.external_port} root@{ssh.external_ip}")
            for p in got.ports or []:
                print(
                    f"  {p.name}: {p.eni_ip}:{p.eni_port}  "
                    f"public={p.external_ip}:{p.external_port}  "
                    f"state={getattr(p.status, 'state', None)}"
                )
            return 0
        time.sleep(3)
    sys.stderr.write("timed out waiting for SSH external port allocation\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
