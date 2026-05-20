#!/usr/bin/env bash
# Launch two tools-rust template containers (cold + warm) and run the
# numpy matmul bench against them. Both containers use the
# inspect-tpl-tools-tools-python-dev:latest image. The warm daemon is
# warmed by the bench script itself, not by api-rust [warmup].
#
# Requirements:
#   - docker available and able to publish ports 18002/18003 on the host
#   - inspect-tpl-tools-tools-python-dev:latest image present locally
#     (build it via `template-build tools-python-dev` from the mindbox image)
#   - python3 with stdlib only on the host
#
# Usage:
#   bench/run_matmul.sh [--keep] [-- <extra args to matmul.py>]
#
# Examples:
#   bench/run_matmul.sh                 # default 256, seq=30, par=32
#   bench/run_matmul.sh -- --n 512      # bigger matrix
#   bench/run_matmul.sh --keep          # leave daemon containers running

set -euo pipefail

IMAGE="${BENCH_IMAGE:-inspect-tpl-tools-tools-python-dev:latest}"
COLD_NAME="${BENCH_COLD:-tools-bench-cold}"
WARM_NAME="${BENCH_WARM:-tools-bench-warm}"
COLD_PORT="${BENCH_COLD_PORT:-18002}"
WARM_PORT="${BENCH_WARM_PORT:-18003}"

KEEP=0
EXTRA_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --keep) KEEP=1; shift ;;
        --) shift; EXTRA_ARGS=("$@"); break ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

cleanup() {
    if [ "$KEEP" -ne 1 ]; then
        docker rm -f "$COLD_NAME" "$WARM_NAME" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

docker rm -f "$COLD_NAME" "$WARM_NAME" >/dev/null 2>&1 || true
docker run -d --name "$COLD_NAME" --rm -p "${COLD_PORT}:8002" "$IMAGE" >/dev/null
docker run -d --name "$WARM_NAME" --rm -p "${WARM_PORT}:8002" "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
    ok=1
    for port in "$COLD_PORT" "$WARM_PORT"; do
        curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1 || ok=0
    done
    [ "$ok" = 1 ] && break
    sleep 0.2
done

# Ensure numpy is available in both containers (extra_pip in the template
# does not include numpy by default; this is bench-only setup).
for name in "$COLD_NAME" "$WARM_NAME"; do
    docker exec "$name" sh -c "command -v python >/dev/null && python -c 'import numpy' 2>/dev/null \
        || pip install --quiet --no-cache-dir --root-user-action ignore numpy" >/dev/null
done

exec python3 "$(dirname "$0")/matmul.py" \
    --cold "http://127.0.0.1:${COLD_PORT}" \
    --warm "http://127.0.0.1:${WARM_PORT}" \
    "${EXTRA_ARGS[@]}"
