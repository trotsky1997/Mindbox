"""_build_resp: ChildResponse protobuf marshaling."""


def test_basic_fields_roundtrip(helper, pb):
    raw = helper._build_resp("hi", "err", 0, False, None)
    msg = pb.ChildResponse.FromString(raw)
    assert msg.stdout == "hi"
    assert msg.stderr == "err"
    assert msg.exit_code == 0
    assert msg.expire is False


def test_expire_flag_and_reason(helper, pb):
    raw = helper._build_resp("", "", 1, True, "max_reqs")
    msg = pb.ChildResponse.FromString(raw)
    assert msg.expire is True
    assert msg.lifecycle.expire_reason == "max_reqs"


def test_output_files_text_map(helper, pb):
    raw = helper._build_resp(
        "", "", 0, False, None,
        output_files={"a.txt": "alpha", "sub/b.txt": "beta"},
    )
    msg = pb.ChildResponse.FromString(raw)
    assert dict(msg.output_files) == {"a.txt": "alpha", "sub/b.txt": "beta"}
    assert len(msg.output_files_b64) == 0
    assert len(msg.deleted_files) == 0


def test_output_files_b64_map(helper, pb):
    raw = helper._build_resp(
        "", "", 0, False, None,
        output_files_b64={"blob.bin": "AAEC"},
    )
    msg = pb.ChildResponse.FromString(raw)
    assert dict(msg.output_files_b64) == {"blob.bin": "AAEC"}


def test_deleted_files_list(helper, pb):
    raw = helper._build_resp(
        "", "", 0, False, None,
        deleted_files=["gone1.txt", "gone2.txt"],
    )
    msg = pb.ChildResponse.FromString(raw)
    assert list(msg.deleted_files) == ["gone1.txt", "gone2.txt"]


def test_empty_inputs_produce_empty_maps_not_unset(helper, pb):
    raw = helper._build_resp("", "", 0, False, None)
    msg = pb.ChildResponse.FromString(raw)
    # Empty (not None) is the contract — caller should still see iterable maps.
    assert len(msg.output_files) == 0
    assert len(msg.output_files_b64) == 0
    assert len(msg.deleted_files) == 0


def test_lifecycle_fields_present(helper, pb):
    raw = helper._build_resp("", "", 0, False, None)
    msg = pb.ChildResponse.FromString(raw)
    # rss_mb / requests_served / age_sec / total_exec_ms come from the module.
    # Just assert they're set to int values (not negative); exact numbers vary.
    assert isinstance(msg.lifecycle.rss_mb, int)
    assert isinstance(msg.lifecycle.requests_served, int)
    assert msg.lifecycle.requests_served >= 0
    assert msg.lifecycle.age_sec >= 0
