"""Shared helpers for the ops/devbox scripts.

All scripts in this directory share AK/SK loading and region selection
through environment variables:

    VOLC_ACCESS_KEY_ID
    VOLC_SECRET_ACCESS_KEY
    VOLC_REGION             (default: cn-beijing)

The Volcano SDK is the official `volcengine-python-sdk` package
(install with `pip install volcengine-python-sdk`).
"""
from __future__ import annotations

import os
import sys


def configure_sdk() -> None:
    """Wire up volcenginesdkcore.Configuration from env. Call once at startup."""
    try:
        import volcenginesdkcore  # type: ignore[import-not-found]
    except ImportError:
        sys.stderr.write(
            "volcenginesdkcore not installed.\n"
            "    pip install volcengine-python-sdk\n"
        )
        raise SystemExit(2)

    ak = os.environ.get("VOLC_ACCESS_KEY_ID")
    sk = os.environ.get("VOLC_SECRET_ACCESS_KEY")
    if not ak or not sk:
        sys.stderr.write(
            "VOLC_ACCESS_KEY_ID and VOLC_SECRET_ACCESS_KEY must be set in env.\n"
            "Obtain from 火山引擎 → 访问控制 → API 访问密钥.\n"
        )
        raise SystemExit(2)

    cfg = volcenginesdkcore.Configuration()
    cfg.ak = ak
    cfg.sk = sk
    cfg.region = os.environ.get("VOLC_REGION", "cn-beijing")
    volcenginesdkcore.Configuration.set_default(cfg)


# SSH keys trusted on the canonical mindbox dev-instance. Reused for new
# instances we provision so an existing operator's `~/.ssh/id_rsa` keeps
# working.
TRUSTED_SSH_KEYS = (
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQClWFCJdzl4y3pzfsPvNZVUlE3wnXhKByVQ/d0DPFstA3Bq8DmzCTznD53Gb2Y+eYL5IZvpxI+1vvefuCzdNQNsNJSEgSoeDcQaDY62L3lCTwqqqMl3WcPGNSkLirihMXpGhlxfxK084hLwWkvs2YjO78xmPPlsQys8Xr93Bz6GdonJJs9YCtCCbpWVoROxpgBHcPGolOB14ZnAtEApRmg5QigtdaHWWS+eRDcvxt5U+ouK4/5237shkJtRDhMrvpTp8HZL7eH5228u0cfD6rFu5IJWb4ZdwpQ6jG5Vk3bbZybgvJmEnDhyon2k9KKJuwMAW3z00oXHXpjvNaJ/CPlMliMd0jcv0jZb0KrXewTATvJfaK/hGZa41YHatRBroeLOgrLhil6kpX0CrWrCf9WYzHRgPkwuGpSBD9pJ3mjH376LgW8VxfvZr1lmpO2rf096uDrs3Rziu+Gli6wJlBfDM0+ioe9awqHemz16rHdKzzaxCGUbWPO46kfbjiVosm0= trots@Thinkbook\n"
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCxENjmtDsJctYzABctpzLPZVQEFL95PCoMZQ3isRO6mMdLgyJU2MT2xn67kV4o/yLyvVr8kXrS7QF3yMCzjnL+BPyaOUYmalOrPADYlCrb9zpm4u6PL63q6X5NxWqiK+sI84uwTe//RS8CRXe2eGkvgSLxudow1iSJuuo/6D3kP8zWTbv8ME0nqrD9Zi83QaLmcpmfjpkrYe1L2TItalwC9vsuxPLQPLh0f+7LpuNpyLxcNhT2mkixr0OFcJ3aeCLrj+qsgq7v0T+YNOY29vuqnwqaopRvuX8JEwas0w0acwRsa+Fybz04qXH7t/wUQm3LeP5jPqrtCBYseGS/8Eweu7oHaiKv9yYzBRbC6BLKmt6v01mtwhpylxFXDn6+W86YlokRWoqX70sHqR9dtv0Jk0V7g0nxd5jNKNlT5z3RrFK9W/dV9GswN6ttlCb6EWJrBhQO9+H3QVH6v0gxVcG7EIbIPKOADe+j8/xQsEjd4oSnLEHWoUg5YJwX/zXDI6VTSMa/IYgxId9+y7+V2Je1f5JZPCdW0vlj3zArf3KWkY3O7GNs3U6YeqqBpzGvFhyTwP+p7kYyKr/NTKykB05ClS5ZiQ/hbnI8U2hKahcwluvn376wAQ89HBr4uJuMf0P9sj0ZrBLgoTSiYvs9SC8boCFJYwBcj0ElP+odgbtGzw== di.zhang@ustc.edu\n"
)

# Mindbox image and queue defaults captured from the canonical
# di-20260519182002-4fj4l dev-instance. Override per call site if needed.
MINDBOX_IMAGE_URL = "ghcr.io/trotsky1997/mindbox:latest"
DEFAULT_RESOURCE_QUEUE_ID = "q-20260519151516-zh4nt"
DEFAULT_INSTANCE_TYPE_ID = "ml.g3a.8xlarge"
DEFAULT_ZONE_ID = "cn-beijing-c"
DEFAULT_VOLUME_GIB = 50
DEFAULT_VOLUME_TYPE_ID = "ml.essd.pl0"
