"""_maybe_offload: S3 stdout/stderr offload threshold logic."""


class _MockClient:
    def __init__(self, raise_on_put=False):
        self.raise_on_put = raise_on_put
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_on_put:
            raise RuntimeError("boom")
        return {}


def test_below_threshold_passes_through(helper):
    # Default state: no client, threshold 0 → input echoed.
    out = helper._maybe_offload("small text", "stdout")
    assert out == "small text"


def test_above_threshold_uploads_and_returns_url(helper):
    helper._s3_client = _MockClient()
    helper._S3_THRESHOLD = 10
    helper._S3_BUCKET = "test-bucket"
    helper._S3_PREFIX = "p/"
    text = "x" * 20
    out = helper._maybe_offload(text, "stdout")
    assert out.startswith("s3://test-bucket/p/stdout/")
    assert out.endswith("[offloaded 20 bytes]")
    assert len(helper._s3_client.calls) == 1
    call = helper._s3_client.calls[0]
    assert call["Bucket"] == "test-bucket"
    assert call["Body"] == b"x" * 20


def test_threshold_boundary_inclusive_at_threshold(helper):
    # Contract: `len(text) < threshold` passes through; >= uploads.
    helper._s3_client = _MockClient()
    helper._S3_THRESHOLD = 10
    helper._S3_BUCKET = "b"
    # len == threshold → uploads
    out = helper._maybe_offload("x" * 10, "stderr")
    assert out.startswith("s3://b/")
    assert len(helper._s3_client.calls) == 1
    # len == threshold-1 → passthrough
    helper._s3_client.calls.clear()
    out2 = helper._maybe_offload("y" * 9, "stderr")
    assert out2 == "yyyyyyyyy"
    assert helper._s3_client.calls == []


def test_put_failure_appends_diagnostic(helper):
    helper._s3_client = _MockClient(raise_on_put=True)
    helper._S3_THRESHOLD = 5
    helper._S3_BUCKET = "b"
    out = helper._maybe_offload("xxxxxx", "stderr")
    assert out.startswith("xxxxxx")
    assert "[s3 offload failed: boom]" in out


def test_no_client_means_passthrough_even_above_threshold(helper):
    helper._s3_client = None
    helper._S3_THRESHOLD = 1
    out = helper._maybe_offload("zzz", "stdout")
    assert out == "zzz"
