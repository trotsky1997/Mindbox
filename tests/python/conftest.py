# pytest config for tests/python/sandbox_helper.py coverage.
#
# Run from repo root:
#   pip install -r tests/python/requirements.txt
#   pytest tests/python -v

import importlib
import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER_SRC = REPO_ROOT / "worker-rust" / "src"
PROTO_DIR = REPO_ROOT / "proto"

# Make `import sandbox_helper` and `import inspect_pb2` resolve to checked-in
# files. No codegen needed — proto/inspect_pb2.py is committed.
for p in (WORKER_SRC, PROTO_DIR):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

# Default to S3 offload OFF so module import doesn't try to construct a boto3
# client. Individual tests opt in by setting the env + reload.
os.environ.setdefault("WORKER_STDOUT_S3_THRESHOLD", "0")


@pytest.fixture
def helper():
    """Fresh `sandbox_helper` module per test that asks for it.

    Most tests don't care about module globals (`_requests_served`, etc.),
    but `_build_resp` populates `Lifecycle.requests_served` from the global,
    so anything asserting on that field wants a clean slate.
    """
    import sandbox_helper as sh
    importlib.reload(sh)
    return sh


@pytest.fixture
def pb():
    import inspect_pb2 as pb
    return pb
