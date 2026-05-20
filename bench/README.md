# bench

Local micro-benchmarks for the seven-tool runtime. Not run in CI; they need
Docker and produce noisy numbers depending on host load.

## numpy matmul (cold vs warmed tools-rust)

`run_matmul.sh` spins up two `inspect-tpl-tools-tools-python-dev:latest` containers
on host ports `18002` and `18003`, ensures numpy is installed in each, and then
runs `matmul.py` to compare a cold daemon against a daemon that the bench has
already warmed via the same commands `api-rust` would issue for `[warmup]`.

```bash
# Easiest: start the released mindbox image once with a docker socket. The
# entrypoint pulls / builds every configured template, so the bench's expected
# `inspect-tpl-tools-tools-python-dev:latest` tag is present locally afterwards.
docker run --rm -d --name mindbox-bench-bootstrap \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e MINDBOX_MODE=api \
  ghcr.io/trotsky1997/mindbox:latest
docker logs -f mindbox-bench-bootstrap | grep -m1 templates
docker rm -f mindbox-bench-bootstrap

# Or build the template image directly from the controller image:
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$PWD/templates:/opt/inspect-api/templates:ro" \
  --entrypoint /usr/local/bin/template-build \
  ghcr.io/trotsky1997/mindbox:latest tools-python-dev

# Run the bench
bench/run_matmul.sh

# Bigger matrix / longer parallel sweep
bench/run_matmul.sh -- --n 512 --parallel 64
```

`run_matmul.sh` cleans up the two containers on exit. Pass `--keep` to leave
them running for ad-hoc curl checks.

What the output means:

- `[warm-daemon warmup]` is the work `api-rust` would do on cold start before
  marking the template Hot. Pre-touches numpy + a small matmul.
- `first_session_first_bash_ms` is the latency a user actually sees on their
  first `bash` call against that daemon.
- `sequential matmul N` is steady-state per-call latency in the same session.
- `parallel matmul N per-call` and `wall` measure throughput when many fresh
  sessions hit the daemon concurrently.

Caveats:

- Host OS page cache makes the cold side warmer than a true first-time pull.
  For a faithful cold reading, run on a freshly booted host or drop caches
  between runs (`sync && echo 3 > /proc/sys/vm/drop_caches`, root-only).
- numpy is `pip install`ed at bench setup, not built into the template image
  yet.
- `bash` spawns one Python interpreter per call, so warmup only primes OS page
  cache and `pip`'s on-disk metadata; it does not retain a Python heap across
  calls. Workloads with very heavy imports (e.g. `import torch`) tend to show
  the biggest warmup benefit.
