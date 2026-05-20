#!/usr/bin/env python3
"""
Numpy matmul micro-benchmark for the seven-tool runtime.

Measures cold vs warmed tools-rust daemon for a Python numpy workload:

  - simulate the api-rust startup warmup against the warm daemon
  - first session first bash on each daemon
  - 30 sequential numpy matmul 256x256 on each daemon
  - N parallel numpy matmul 256x256 on fresh sessions on each daemon

The cold daemon is the baseline; the warm daemon has the same image but is
pre-touched by warmup commands before the bench starts.

Hits the tools-rust HTTP API directly, so it does not need api-rust running.
Both daemons must already be reachable on the given base URLs.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.request


def matmul_cmd(n: int) -> str:
    return (
        "python -c \"import numpy as np, time; "
        f"t=time.perf_counter(); n={n}; "
        "a=np.random.rand(n,n); b=np.random.rand(n,n); c=a@b; "
        f"print(f'{n}x{n} '+str(round((time.perf_counter()-t)*1000,2))+'ms')\""
    )


def post(base: str, path: str, body: dict, timeout: float = 180.0) -> tuple[float, dict]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        base + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out = r.read()
    return (time.perf_counter() - t) * 1000, json.loads(out or b"{}")


def delete(base: str, path: str) -> None:
    req = urllib.request.Request(base + path, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=30):
            pass
    except urllib.error.HTTPError:
        pass


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int((len(xs) - 1) * p))]


def summary(name: str, xs: list[float]) -> None:
    print(
        f"{name}: n={len(xs)} min={min(xs):.1f}ms p50={pct(xs, .50):.1f}ms "
        f"p95={pct(xs, .95):.1f}ms max={max(xs):.1f}ms mean={statistics.mean(xs):.1f}ms",
        flush=True,
    )


def new_session(base: str) -> str:
    _, resp = post(base, "/sessions", {})
    return resp["session_id"]


def run_bash(base: str, sid: str, cmd: str, timeout: float = 240.0) -> tuple[float, dict]:
    dt, body = post(
        base,
        f"/sessions/{sid}/tools/bash",
        {"command": cmd, "timeout": 120},
        timeout=timeout,
    )
    if body.get("exit_code") != 0:
        raise RuntimeError(body)
    return dt, body


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cold", default="http://127.0.0.1:18002", help="cold daemon base URL")
    parser.add_argument("--warm", default="http://127.0.0.1:18003", help="warm daemon base URL")
    parser.add_argument("--n", type=int, default=256, help="matrix size")
    parser.add_argument(
        "--sequential",
        type=int,
        default=30,
        help="iterations of sequential matmul per daemon",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=32,
        help="degree of parallel matmul (new session per call)",
    )
    args = parser.parse_args()

    cmd = matmul_cmd(args.n)

    prewarm_sid = new_session(args.warm)
    dt1, _ = run_bash(args.warm, prewarm_sid, "python -c 'import numpy; import numpy.linalg'")
    dt2, _ = run_bash(
        args.warm,
        prewarm_sid,
        "python -c \"import numpy as np; a=np.zeros((256,256)); _=a@a\"",
    )
    delete(args.warm, f"/sessions/{prewarm_sid}")
    print(f"[warm-daemon warmup] step1_ms={dt1:.2f} step2_ms={dt2:.2f}", flush=True)

    cold_first_sid = new_session(args.cold)
    dt_cold_first, body_cold = run_bash(args.cold, cold_first_sid, cmd)
    print(
        f"[cold] first_session_first_bash_ms={dt_cold_first:.2f} stdout={body_cold['stdout'].strip()}",
        flush=True,
    )

    warm_first_sid = new_session(args.warm)
    dt_warm_first, body_warm = run_bash(args.warm, warm_first_sid, cmd)
    print(
        f"[warm] first_session_first_bash_ms={dt_warm_first:.2f} stdout={body_warm['stdout'].strip()}",
        flush=True,
    )

    xs_cold = [run_bash(args.cold, cold_first_sid, cmd)[0] for _ in range(args.sequential)]
    summary(f"cold sequential matmul {args.n}", xs_cold)

    xs_warm = [run_bash(args.warm, warm_first_sid, cmd)[0] for _ in range(args.sequential)]
    summary(f"warm sequential matmul {args.n}", xs_warm)

    def parallel(base: str) -> float:
        sid = new_session(base)
        try:
            dt, _ = run_bash(base, sid, cmd, timeout=240.0)
            return dt
        finally:
            delete(base, f"/sessions/{sid}")

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
        xs_cpar = list(ex.map(lambda _: parallel(args.cold), range(args.parallel)))
    wall_cold = (time.perf_counter() - t0) * 1000
    summary(f"cold {args.parallel}x parallel matmul {args.n} per-call", xs_cpar)
    print(
        f"cold {args.parallel}x parallel wall={wall_cold:.1f}ms "
        f"throughput={args.parallel / (wall_cold / 1000):.1f}/s",
        flush=True,
    )

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
        xs_par = list(ex.map(lambda _: parallel(args.warm), range(args.parallel)))
    wall_warm = (time.perf_counter() - t0) * 1000
    summary(f"warm {args.parallel}x parallel matmul {args.n} per-call", xs_par)
    print(
        f"warm {args.parallel}x parallel wall={wall_warm:.1f}ms "
        f"throughput={args.parallel / (wall_warm / 1000):.1f}/s",
        flush=True,
    )

    delete(args.cold, f"/sessions/{cold_first_sid}")
    delete(args.warm, f"/sessions/{warm_first_sid}")


if __name__ == "__main__":
    main()
