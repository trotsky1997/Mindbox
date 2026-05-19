"""Per-request cwd isolation — fast-path now mkdtemp+chdir each request so
relative writes in request N can't be observed by request N+1 sharing the
same worker child.
"""


def _run(helper, pb, code):
    job = pb.Job()
    job.code = code
    job.timeout = 5
    raw = helper._run_sandbox_pb(job.SerializeToString())
    resp = pb.ChildResponse()
    resp.ParseFromString(raw)
    return resp


def test_relative_write_does_not_leak_to_next_request(helper, pb):
    a = _run(helper, pb, "import pathlib; pathlib.Path('marker.txt').write_text('A')")
    assert a.exit_code == 0, a.stderr
    b = _run(
        helper,
        pb,
        "import pathlib,sys; sys.stdout.write('present' if pathlib.Path('marker.txt').exists() else 'gone')",
    )
    assert b.exit_code == 0, b.stderr
    assert b.stdout == "gone"


def test_cwd_is_a_fresh_tmpdir_per_request(helper, pb):
    a = _run(helper, pb, "import os,sys; sys.stdout.write(os.getcwd())")
    b = _run(helper, pb, "import os,sys; sys.stdout.write(os.getcwd())")
    assert a.exit_code == 0 and b.exit_code == 0
    assert a.stdout != b.stdout, "expected different per-request tmpdirs"
    # Tmpdirs should be cleaned up after request returns (no lingering dir).
    import os
    assert not os.path.exists(a.stdout)
    assert not os.path.exists(b.stdout)


def test_cwd_restored_after_request(helper, pb):
    import os
    before = os.getcwd()
    _run(helper, pb, "import os; assert os.getcwd().startswith('/tmp/run-') or '/run-' in os.getcwd()")
    assert os.getcwd() == before
