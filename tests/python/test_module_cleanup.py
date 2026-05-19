"""sys.modules diff cleanup — drops modules user code imports per request.

Prewarmed modules (captured in _initial_sys_modules at first _ensure_init)
stay. Anything added later is purged so the next request gets a fresh
re-import (and a fresh module top-level execution).
"""

import sys


def test_drop_skips_when_initial_is_none(helper):
    # Before _ensure_init runs, _initial_sys_modules is None and the
    # cleanup must be a no-op (no exceptions, no state change).
    assert helper._initial_sys_modules is None
    before = set(sys.modules.keys())
    helper._drop_request_modules(None)
    assert set(sys.modules.keys()) == before


def test_ensure_init_snapshots_sys_modules(helper):
    assert helper._initial_sys_modules is None
    helper._ensure_init()
    assert isinstance(helper._initial_sys_modules, frozenset)
    assert "sys" in helper._initial_sys_modules
    # frozenset → immutable; later imports must not silently extend it.
    snapshot = helper._initial_sys_modules
    helper._ensure_init()  # second call is a no-op for this slot
    assert helper._initial_sys_modules is snapshot


def test_drop_removes_newly_added_modules(helper):
    helper._ensure_init()
    initial = helper._initial_sys_modules
    # Pick an unlikely-to-be-prewarmed stdlib module so the assertion is
    # robust against incidental imports in conftest / fixtures.
    fresh = "html.parser"
    sys.modules.pop(fresh, None)
    assert fresh not in initial
    __import__(fresh)
    assert fresh in sys.modules
    helper._drop_request_modules(initial)
    assert fresh not in sys.modules


def test_drop_preserves_initial_modules(helper):
    helper._ensure_init()
    initial = helper._initial_sys_modules
    # Pick something guaranteed to be in the initial snapshot.
    anchor = "sys"
    assert anchor in initial
    # Even if a request "imports" it (it's already there), cleanup must
    # not drop it.
    __import__(anchor)
    helper._drop_request_modules(initial)
    assert anchor in sys.modules
