#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

HOST="${PRIMADB_HOST:-127.0.0.1}"
PORT="${PRIMADB_PORT:-4175}"
SCRIPT_CMD="python3 examples/browser-threaded-mesh-notes/serve.py"

if command -v lsof >/dev/null 2>&1; then
  mapfile -t LISTEN_PIDS < <(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)
else
  LISTEN_PIDS=()
fi

if ((${#LISTEN_PIDS[@]} > 0)); then
  for pid in "${LISTEN_PIDS[@]}"; do
    cmdline="$(tr '\0' ' ' </proc/"$pid"/cmdline 2>/dev/null || true)"
    cwd="$(readlink -f /proc/"$pid"/cwd 2>/dev/null || true)"
    if [[ "$cmdline" == *"$SCRIPT_CMD"* && "$cwd" == "$ROOT_DIR" ]]; then
      echo "Threaded mesh demo is already serving $ROOT_DIR at http://$HOST:$PORT/"
      exit 0
    fi
  done

  echo "Port $PORT is already in use by:" >&2
  for pid in "${LISTEN_PIDS[@]}"; do
    ps -fp "$pid" >&2 || true
  done
  echo "Set PRIMADB_PORT to another port if you want a second server." >&2
  exit 1
fi

exec env PRIMADB_HOST="$HOST" PRIMADB_PORT="$PORT" python3 examples/browser-threaded-mesh-notes/serve.py
