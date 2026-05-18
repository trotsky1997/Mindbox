#!/usr/bin/env bash
set -euo pipefail
cd /opt/inspect-api
SESSION="${INSPECT_API_TMUX_SESSION:-inspect-api}"
tmux kill-session -t "$SESSION" 2>/dev/null && echo "killed tmux: $SESSION" || echo "no tmux session: $SESSION"
# Belt-and-suspenders: kill any e2b-shim that escaped the tmux session
pkill -f "e2b-shim/target/release/e2b-shim" 2>/dev/null && echo "killed stray e2b-shim" || true
HOT=$(docker ps -aq --filter label=inspect-api-hot=1)
[ -n "$HOT" ] && docker rm -f $HOT >/dev/null 2>&1 && echo "removed $(echo $HOT | wc -w) hot containers" || echo "no hot containers"
COLD=$(docker ps -aq --filter label=inspect-api=1)
[ -n "$COLD" ] && docker rm -f $COLD >/dev/null 2>&1 && echo "removed $(echo $COLD | wc -w) cold containers" || true
echo stopped
