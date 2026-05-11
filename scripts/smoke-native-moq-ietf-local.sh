#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${TMPDIR:-/tmp}/primadb-moq-ietf-local"
MOQ_RS_DIR="$TMP_DIR/moq-rs"
CERT_DIR="$TMP_DIR/certs"
PORT="${PRIMADB_MOQ_IETF_LOCAL_PORT:-4443}"
SCOPE="${PRIMADB_MOQ_IETF_LOCAL_SCOPE:-primadb-local-smoke}"
RELAY_URL="https://127.0.0.1:${PORT}/${SCOPE}"
LOG_FILE="$TMP_DIR/moq-relay-ietf.log"

mkdir -p "$TMP_DIR" "$CERT_DIR"

if [[ ! -d "$MOQ_RS_DIR/.git" ]]; then
  git clone --depth 1 https://github.com/cloudflare/moq-rs.git "$MOQ_RS_DIR"
else
  git -C "$MOQ_RS_DIR" fetch --depth 1 origin main
  git -C "$MOQ_RS_DIR" checkout --quiet origin/main
fi

if [[ ! -f "$CERT_DIR/localhost.crt" || ! -f "$CERT_DIR/localhost.key" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -nodes \
    -days 1 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout "$CERT_DIR/localhost.key" \
    -out "$CERT_DIR/localhost.crt" \
    >/dev/null 2>&1
fi

cargo run \
  --manifest-path "$MOQ_RS_DIR/Cargo.toml" \
  -p moq-relay-ietf \
  -- \
  --bind "127.0.0.1:${PORT}" \
  --tls-cert "$CERT_DIR/localhost.crt" \
  --tls-key "$CERT_DIR/localhost.key" \
  --coordinator-file "$TMP_DIR/coordinator.json" \
  >"$LOG_FILE" 2>&1 &

relay_pid="$!"
cleanup() {
  kill "$relay_pid" >/dev/null 2>&1 || true
  wait "$relay_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in {1..60}; do
  if grep -q "listening on" "$LOG_FILE" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$relay_pid" >/dev/null 2>&1; then
    cat "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 1
done

cd "$ROOT"
PRIMADB_MOQ_TLS_DISABLE_VERIFY=1 \
MOQ_RELAY="$RELAY_URL" \
MOQ_RELAY_DRAFT=draft_14 \
cargo run --features native-moq --example native_moq_live_probe
