"""Sandbox helper (Google protobuf + fast-path).

Wire format with worker parent: protobuf inspect.Job → inspect.ChildResponse.
Fast path: skip tmpdir/chdir/env if job.files and job.env are both empty (the
common "print(1)" case).
"""
import io, sys, signal, contextlib, traceback, tempfile, os, shutil, threading, time, gc, uuid
from functools import lru_cache
from pathlib import Path
import inspect_pb2

# ---- compile cache --------------------------------------------------------
# Most training-data-gen workloads run the same code template repeatedly with
# different inputs. lru_cache on `compile()` saves the parse+compile work per
# duplicate code string. Cache lives in the child process; cleared on respawn.

@lru_cache(maxsize=1024)
def _compile_cached(code):
    return compile(code, "<sandbox>", "exec")


# ---- S3 offload for big stdout/stderr -------------------------------------
# When the workload outputs > threshold bytes, push to S3 and replace the
# field with an s3:// URL. boto3 is optional — if unavailable, offload is a
# no-op and big payloads ride the wire.

_S3_THRESHOLD = int(os.environ.get("WORKER_STDOUT_S3_THRESHOLD", "0"))
_S3_BUCKET = os.environ.get("WORKER_STDOUT_S3_BUCKET", "")
_S3_PREFIX = os.environ.get("WORKER_STDOUT_S3_PREFIX", "inspect-out/")
_s3_client = None
if _S3_THRESHOLD > 0 and _S3_BUCKET:
    try:
        import boto3
        _s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("WORKER_STDOUT_S3_ENDPOINT_URL") or None,
            region_name=os.environ.get("WORKER_STDOUT_S3_REGION") or None,
        )
    except Exception as e:
        sys.stderr.write("[helper] S3 offload disabled: {}\n".format(e))


def _maybe_offload(text, kind):
    if _s3_client is None or _S3_THRESHOLD <= 0 or len(text) < _S3_THRESHOLD:
        return text
    try:
        key = "{}{}/{}.txt".format(_S3_PREFIX, kind, uuid.uuid4().hex)
        _s3_client.put_object(Bucket=_S3_BUCKET, Key=key, Body=text.encode("utf-8", errors="replace"))
        return "s3://{}/{}\n[offloaded {} bytes]".format(_S3_BUCKET, key, len(text))
    except Exception as e:
        return text + "\n[s3 offload failed: {}]\n".format(e)


class _Timeout(BaseException):
    pass


def _alarm(_sig, _frm):
    raise _Timeout()


_started_at = None
_requests_served = 0
_total_exec_ns = 0
_total_gc_collected = 0
_initial_thread_count = None
_initial_fds = None

_MAX_REQS = int(os.environ.get('WORKER_REUSE_MAX_REQS', '200'))
_MAX_RSS_MB = int(os.environ.get('WORKER_REUSE_MAX_RSS_MB', '1024'))
_MAX_AGE_SEC = int(os.environ.get('WORKER_MAX_AGE_SECONDS', '600'))
_GC_EVERY_N = int(os.environ.get('WORKER_GC_EVERY_N', '5'))


def _get_rss_mb():
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 0


def _count_fds():
    try:
        return len(os.listdir('/proc/self/fd'))
    except Exception:
        return -1


def _ensure_init():
    global _started_at, _initial_thread_count, _initial_fds
    if _started_at is None:
        _started_at = time.time()
    if _initial_thread_count is None:
        _initial_thread_count = threading.active_count()
    if _initial_fds is None:
        _initial_fds = _count_fds()


def _build_resp(stdout, stderr, exit_code, expire, expire_reason, output_files=None, deleted_files=None, output_files_b64=None):
    stdout = _maybe_offload(stdout, "stdout")
    stderr = _maybe_offload(stderr, "stderr")
    resp = inspect_pb2.ChildResponse()
    resp.stdout = stdout
    resp.stderr = stderr
    resp.exit_code = exit_code
    resp.expire = expire
    if output_files:
        for k, v in output_files.items():
            resp.output_files[k] = v
    if output_files_b64:
        for k, v in output_files_b64.items():
            resp.output_files_b64[k] = v
    if deleted_files:
        resp.deleted_files.extend(deleted_files)
    lc = resp.lifecycle
    lc.rss_mb = _get_rss_mb()
    lc.requests_served = _requests_served
    lc.age_sec = int(time.time() - _started_at) if _started_at else 0
    lc.total_exec_ms = _total_exec_ns // 1_000_000
    lc.expire_reason = expire_reason or ''
    return resp.SerializeToString()


def _run_sandbox_pb(job_bytes):
    global _requests_served, _total_exec_ns, _total_gc_collected
    _ensure_init()

    job = inspect_pb2.Job()
    try:
        job.ParseFromString(bytes(job_bytes))
    except Exception as e:
        return _build_resp('', 'bad job pb: {}'.format(e), 1, True, 'bad_pb')

    code = job.code
    timeout = max(1, min(int(job.timeout) if job.timeout else 10, 60))
    env = dict(job.env)
    files = dict(job.files)
    persist = bool(getattr(job, 'persist_changes', False))

    saved_cwd = os.getcwd() if (files or persist) else None
    saved_env = dict(os.environ) if env else None
    fds_before = _count_fds()

    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = 0
    run_dir = None

    _persist_out = ({}, [], {})
    exec_start = time.monotonic_ns()
    try:
        # Fast path: no files and no persist → skip tmpdir creation entirely.
        if files or persist:
            run_dir = Path(tempfile.mkdtemp(prefix='run-'))
            run_root = run_dir.resolve()
            for rel, content in files.items():
                target = (run_dir / rel).resolve()
                if not (str(target) == str(run_root) or str(target).startswith(str(run_root) + os.sep)):
                    raise ValueError('invalid path: ' + rel)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
            os.chdir(run_dir)
        # snapshot before exec: rel_path -> (mtime, size)
        _initial = {}
        if persist and run_dir is not None:
            for _root, _, _fs in os.walk(run_dir):
                for _f in _fs:
                    _p = os.path.join(_root, _f)
                    try:
                        _st = os.stat(_p)
                        _rel = os.path.relpath(_p, run_dir)
                        _initial[_rel] = (_st.st_mtime_ns, _st.st_size)
                    except Exception:
                        pass
        if env:
            for k, v in env.items():
                os.environ[str(k)] = str(v)
        signal.signal(signal.SIGALRM, _alarm)
        signal.alarm(timeout)
        ns = {'__name__': '__main__'}
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                exec(_compile_cached(code), ns, ns)
            except SystemExit as e:
                exit_code = int(e.code) if isinstance(e.code, int) else (0 if e.code is None else 1)
            except _Timeout:
                exit_code = 124
                print('timeout after {}s'.format(timeout), file=sys.stderr)
            except BaseException:
                exit_code = 1
                traceback.print_exc(file=sys.stderr)
    except Exception as e:
        exit_code = 1
        stderr.write('worker setup error: {}\n'.format(e))
    finally:
        try:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
        except Exception:
            pass
        if saved_cwd is not None:
            try: os.chdir(saved_cwd)
            except Exception: pass
        if saved_env is not None:
            try:
                for k in list(os.environ.keys()):
                    if k not in saved_env:
                        del os.environ[k]
                for k, v in saved_env.items():
                    os.environ[k] = v
            except Exception: pass
        if persist and run_dir is not None:
            try:
                _final = {}
                for _root, _, _fs in os.walk(run_dir):
                    for _f in _fs:
                        _p = os.path.join(_root, _f)
                        try:
                            _st = os.stat(_p)
                            _rel = os.path.relpath(_p, run_dir)
                            _final[_rel] = (_st.st_mtime_ns, _st.st_size)
                        except Exception:
                            pass
                _changed = {}
                _changed_bin = {}
                import base64 as _b64
                for _rel, _meta in _final.items():
                    _prev = _initial.get(_rel)
                    if _prev != _meta:
                        try:
                            with open(os.path.join(run_dir, _rel), 'rb') as _fp:
                                _raw = _fp.read()
                            if b'\x00' in _raw[:4096]:
                                _changed_bin[_rel] = _b64.b64encode(_raw).decode('ascii')
                            else:
                                _changed[_rel] = _raw.decode('utf-8', 'replace')
                        except Exception:
                            pass
                _deleted = [r for r in _initial.keys() if r not in _final]
                _persist_out = (_changed, _deleted, _changed_bin)
            except Exception:
                _persist_out = ({}, [], {})
        if run_dir is not None:
            try: shutil.rmtree(run_dir, ignore_errors=True)
            except Exception: pass

    _total_exec_ns += time.monotonic_ns() - exec_start
    _requests_served += 1
    if _GC_EVERY_N > 0 and _requests_served % _GC_EVERY_N == 0:
        try:
            _total_gc_collected += gc.collect()
        except Exception:
            pass

    poison = None
    extra_threads = threading.active_count() - _initial_thread_count
    if extra_threads > 0:
        poison = 'threads:+{}'.format(extra_threads)
    fds_after = _count_fds()
    if fds_after > 0 and fds_after > _initial_fds + 2:
        poison = 'fds:{}->{}'.format(_initial_fds, fds_after)

    rss_mb = _get_rss_mb()
    age_sec = time.time() - _started_at
    expire_reason = None
    if poison:
        expire_reason = poison
    elif _requests_served >= _MAX_REQS:
        expire_reason = 'max_reqs'
    elif rss_mb > _MAX_RSS_MB:
        expire_reason = 'rss'
    elif age_sec > _MAX_AGE_SEC:
        expire_reason = 'age'
    expire = expire_reason is not None
    stderr_val = stderr.getvalue() + ('\n[poison: {}]\n'.format(poison) if poison else '')
    return _build_resp(stdout.getvalue(), stderr_val, exit_code, expire, expire_reason, output_files=_persist_out[0], deleted_files=_persist_out[1], output_files_b64=_persist_out[2])
