#!/usr/bin/env bash
set -euo pipefail
cd /opt/inspect-api
export INSPECT_API_TEMPLATES_DIR=${INSPECT_API_TEMPLATES_DIR:-/opt/inspect-api/templates}
export INSPECT_API_MAX_TIMEOUT=${INSPECT_API_MAX_TIMEOUT:-60}
export PORT=${PORT:-8000}
export RUST_LOG=${RUST_LOG:-info}
exec /opt/inspect-api/api-rust/target/release/api-rust
