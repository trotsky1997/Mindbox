#!/usr/bin/env python3
"""List MLPlatform dev-instances in the current project.

Usage:
    VOLC_ACCESS_KEY_ID=... VOLC_SECRET_ACCESS_KEY=... ops/devbox/list_dev_instances.py
"""
from __future__ import annotations

import json
import os

from _common import configure_sdk


def main() -> int:
    configure_sdk()
    import volcenginesdkmlplatform20240701 as mlp  # type: ignore[import-not-found]

    api = mlp.MLPLATFORM20240701Api()
    req = mlp.ListDevInstancesRequest(
        page_number=1,
        page_size=int(os.environ.get("PAGE_SIZE", "20")),
        sort_by="CreateTime",
        sort_order="Descend",
    )
    resp = api.list_dev_instances(req)

    print("--- summary ---")
    print(f"total = {resp.total_count}")
    for item in resp.list or []:
        sid = getattr(item, "id", None)
        name = getattr(item, "name", None)
        phase = getattr(getattr(item, "status", None), "state", None)
        img = getattr(item, "image", None)
        img_url = getattr(img, "url", None) or getattr(img, "id", None) or ""
        print(f"  id={sid}  name={name}  phase={phase}  image={img_url}")

    if (resp.list or []) and os.environ.get("DUMP_FIRST", "1") != "0":
        print()
        print("--- first instance (full) ---")
        item = resp.list[0]
        d = item.to_dict() if hasattr(item, "to_dict") else item
        print(json.dumps(d, default=str, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
