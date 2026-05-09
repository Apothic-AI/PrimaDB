#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${PRIMADB_ROOT:-$(cd -- "$SCRIPT_DIR/../../../.." && pwd)}"
EXAMPLE_DIR="${PRIMADB_PYTHON_MESH_PEER_EXAMPLE_DIR:-$SCRIPT_DIR}"
PORT="${PRIMADB_TEST_RELAY_PORT:-9012}"
RELAY_URL="ws://127.0.0.1:${PORT}"
ROOM="offline-retry-${RANDOM}-${RANDOM}"
APP_LOG="$(mktemp)"
RELAY_LOG="$(mktemp)"
APP_PID=""
RELAY_PID=""

cleanup() {
  if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
    kill "$APP_PID" 2>/dev/null || true
  fi
  if [[ -n "$RELAY_PID" ]] && kill -0 "$RELAY_PID" 2>/dev/null; then
    kill "$RELAY_PID" 2>/dev/null || true
  fi
  rm -f "$APP_LOG" "$RELAY_LOG"
}
trap cleanup EXIT

(
  cd "$EXAMPLE_DIR"
  uv run python main.py \
    --relay "$RELAY_URL" \
    --room "$ROOM" \
    --name "py-offline-retry" \
    --duration-ms 10000
) >"$APP_LOG" 2>&1 &
APP_PID=$!

sleep 2

(
  cd "$ROOT"
  cargo run --quiet --example ws_relay_server -- "127.0.0.1:${PORT}"
) >"$RELAY_LOG" 2>&1 &
RELAY_PID=$!

wait "$APP_PID"

grep -q "continuing offline and retrying in background" "$APP_LOG"
grep -q "connected; mesh signaling is active" "$APP_LOG"

echo "python_mesh_offline_retry_confirmed"
