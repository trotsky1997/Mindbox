"""Snapshot diff logic — pre-exec/post-exec dir scan + text/binary classification.

P2 in the plan: the current `_run_sandbox_pb` does this inline. Extracting it
into a `_diff_dir(initial, run_dir)` helper would let us pytest the contract
directly. Until that refactor lands, this file documents the cases and the
contract — they're enforced today only via the e2e scripts (`e2e_persist.py`,
`e2e_binary.py`).
"""

import pytest


@pytest.mark.skip(reason="P2: needs `_diff_dir` extracted from `_run_sandbox_pb` first")
def test_added_text_file_appears_in_output_files(helper, tmp_path):
    ...


@pytest.mark.skip(reason="P2: same as above")
def test_added_binary_file_appears_in_output_files_b64(helper, tmp_path):
    ...


@pytest.mark.skip(reason="P2: same as above")
def test_deleted_file_appears_in_deleted_files(helper, tmp_path):
    ...


@pytest.mark.skip(reason="P2: same as above")
def test_unchanged_file_is_omitted_from_diff(helper, tmp_path):
    ...
