#!/usr/bin/env bash
set -euo pipefail
cd /opt/inspect-api
SESSION="${INSPECT_API_TMUX_SESSION:-inspect-api}"
REPLICAS="${INSPECT_API_REPLICAS:-1}"
ENABLE_SHIM="${INSPECT_API_ENABLE_SHIM:-1}"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "already running tmux: $SESSION"
  exit 0
fi
: > server.log
: > shim.log

if [ "$REPLICAS" = "1" ]; then
  tmux new-session -d -s "$SESSION" "exec ./run.sh 2>&1 | tee -a /opt/inspect-api/server.log"
  echo "started 1 instance in tmux session: $SESSION"
else
  # Multiple replicas via SO_REUSEPORT. Each in its own pane.
  tmux new-session -d -s "$SESSION" "INSPECT_API_INSTANCE=0 exec ./run.sh 2>&1 | sed 's/^/[0] /' | tee -a /opt/inspect-api/server.log"
  for ((i=1; i<REPLICAS; i++)); do
    tmux split-window -t "$SESSION" "INSPECT_API_INSTANCE=$i exec ./run.sh 2>&1 | sed 's/^/[$i] /' | tee -a /opt/inspect-api/server.log"
    tmux select-layout -t "$SESSION" tiled
  done
  echo "started $REPLICAS instances in tmux session: $SESSION (panes 0..$((REPLICAS-1)))"
fi

if [ "$ENABLE_SHIM" = "1" ] && [ -x ./e2b-shim/target/release/e2b-shim ]; then
  tmux split-window -t "$SESSION" "exec ./e2b-shim/target/release/e2b-shim 2>&1 | sed 's/^/[shim] /' | tee -a /opt/inspect-api/shim.log"
  tmux select-layout -t "$SESSION" tiled
  echo "started e2b-shim in same tmux session (pane added)"
fi

echo "attach: tmux attach -t $SESSION"
