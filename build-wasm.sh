#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${PRIMADB_WASM_OUT_DIR:-pkg}"

exec ./scripts/build-wasm.sh --out-dir "$OUT_DIR" "$@"
