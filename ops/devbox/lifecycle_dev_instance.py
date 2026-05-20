#!/usr/bin/env python3
"""Stop / start / delete an MLPlatform dev-instance.

Usage:
    DEV_ID=di-... ACTION=stop   ops/devbox/lifecycle_dev_instance.py
    DEV_ID=di-... ACTION=start  ops/devbox/lifecycle_dev_instance.py
    DEV_ID=di-... ACTION=delete ops/devbox/lifecycle_dev_instance.py
"""
from __future__ import annotations

import os
import sys

from _common import configure_sdk


def main() -> int:
    configure_sdk()
    import volcenginesdkmlplatform20240701 as mlp  # type: ignore[import-not-found]

    dev_id = os.environ.get("DEV_ID")
    action = (os.environ.get("ACTION") or "").lower()
    if not dev_id or action not in {"stop", "start", "delete"}:
        sys.stderr.write(
            "DEV_ID and ACTION (stop|start|delete) env vars are required.\n"
        )
        return 2

    api = mlp.MLPLATFORM20240701Api()
    if action == "stop":
        api.stop_dev_instance(mlp.StopDevInstanceRequest(id=dev_id))
    elif action == "start":
        api.start_dev_instance(mlp.StartDevInstanceRequest(id=dev_id))
    elif action == "delete":
        api.delete_dev_instance(mlp.DeleteDevInstanceRequest(id=dev_id))

    print(f"{action} {dev_id}: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
