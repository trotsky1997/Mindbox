"""_get_rss_mb + _count_fds: /proc introspection with mocked filesystem."""

import io


def test_get_rss_mb_parses_vmrss(helper, monkeypatch):
    fake = io.StringIO("Name:\tx\nVmRSS:\t  4096 kB\nFoo:\t1\n")
    monkeypatch.setattr(helper, "open", lambda p: fake, raising=False)
    # Note: open() is a builtin used in sandbox_helper as `open('/proc/self/status')`.
    # We need to patch the builtin in that module's globals.
    # The simpler & reliable path: patch builtins.open.
    import builtins
    monkeypatch.setattr(builtins, "open", lambda p: io.StringIO(
        "Name:\tx\nVmRSS:\t  4096 kB\n"))
    assert helper._get_rss_mb() == 4   # 4096 kB / 1024


def test_get_rss_mb_missing_vmrss_returns_zero(helper, monkeypatch):
    import builtins
    monkeypatch.setattr(builtins, "open", lambda p: io.StringIO(
        "Name:\tx\nVmPeak:\t1 kB\n"))
    assert helper._get_rss_mb() == 0


def test_get_rss_mb_open_raises_returns_zero(helper, monkeypatch):
    import builtins
    def boom(_p):
        raise FileNotFoundError("no proc")
    monkeypatch.setattr(builtins, "open", boom)
    assert helper._get_rss_mb() == 0


def test_count_fds_happy_path(helper, monkeypatch):
    import os as os_mod
    monkeypatch.setattr(os_mod, "listdir", lambda _p: ["0", "1", "2", "3"])
    assert helper._count_fds() == 4


def test_count_fds_listdir_raises_returns_minus_one(helper, monkeypatch):
    import os as os_mod
    def boom(_p):
        raise PermissionError("denied")
    monkeypatch.setattr(os_mod, "listdir", boom)
    assert helper._count_fds() == -1
