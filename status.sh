#!/usr/bin/env bash
set -euo pipefail
cd /opt/inspect-api
SESSION="${INSPECT_API_TMUX_SESSION:-inspect-api}"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "tmux session '$SESSION' alive"
else
  echo "tmux session '$SESSION' DOWN"
fi
curl -fsS http://127.0.0.1:8000/health 2>/dev/null && echo || echo "(no api-rust /health response)"
curl -fsS http://127.0.0.1:8001/health 2>/dev/null && echo || echo "(no e2b-shim /health response)"
echo "hot containers:"
docker ps --filter label=inspect-api-hot=1 --format "  {{.Names}} ({{.Status}}) image={{.Image}} ports={{.Ports}}"
