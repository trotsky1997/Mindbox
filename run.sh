#!/usr/bin/env bash
set -euo pipefail
cd /opt/inspect-api
# Load .env if present (REGISTRY_*, TOS_*, WORKER_*). Quiet if missing.
if [ -f /opt/inspect-api/.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /opt/inspect-api/.env
  set +a
fi
export INSPECT_API_TEMPLATES_DIR=${INSPECT_API_TEMPLATES_DIR:-/opt/inspect-api/templates}
export INSPECT_API_MAX_TIMEOUT=${INSPECT_API_MAX_TIMEOUT:-60}
export PORT=${PORT:-8000}
export RUST_LOG=${RUST_LOG:-info}
exec /opt/inspect-api/api-rust/target/release/api-rust
