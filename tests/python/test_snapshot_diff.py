"""Snapshot diff logic — pre-exec/post-exec dir scan + text/binary classification.

Tests the `_diff_dir(initial, run_dir)` helper extracted from `_run_sandbox_pb`.
Contract: returns (changed_text, deleted, changed_binary_b64). A file is in
the diff iff its (mtime_ns, size) differs from `initial.get(rel)`. NUL byte
in the first 4KB → binary (base64); else text (UTF-8, errors='replace').
"""

import base64


def test_added_text_file_appears_in_output_files(helper, tmp_path):
    (tmp_path / "hello.txt").write_text("hi")
    changed_text, deleted, changed_bin = helper._diff_dir({}, tmp_path)
    assert changed_text == {"hello.txt": "hi"}
    assert deleted == []
    assert changed_bin == {}


def test_added_binary_file_appears_in_output_files_b64(helper, tmp_path):
    payload = b"\x89PNG\r\n\x1a\n\x00\x00\x00pretend"
    (tmp_path / "blob.bin").write_bytes(payload)
    changed_text, deleted, changed_bin = helper._diff_dir({}, tmp_path)
    assert changed_text == {}
    assert deleted == []
    assert set(changed_bin) == {"blob.bin"}
    assert base64.b64decode(changed_bin["blob.bin"]) == payload


def test_deleted_file_appears_in_deleted_files(helper, tmp_path):
    initial = {"gone.txt": (1, 5), "kept.txt": (2, 3)}
    (tmp_path / "kept.txt").write_text("abc")
    initial["kept.txt"] = helper._snapshot_dir(tmp_path)["kept.txt"]
    changed_text, deleted, changed_bin = helper._diff_dir(initial, tmp_path)
    assert deleted == ["gone.txt"]
    assert changed_text == {}
    assert changed_bin == {}


def test_unchanged_file_is_omitted_from_diff(helper, tmp_path):
    (tmp_path / "stable.txt").write_text("untouched")
    initial = helper._snapshot_dir(tmp_path)
    changed_text, deleted, changed_bin = helper._diff_dir(initial, tmp_path)
    assert changed_text == {}
    assert deleted == []
    assert changed_bin == {}
