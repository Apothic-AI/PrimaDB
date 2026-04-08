#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack is required. Install it with: cargo install wasm-pack" >&2
  exit 1
fi

TOOLCHAIN="${PRIMADB_WASM_THREADS_TOOLCHAIN:-nightly}"

rustup component add rust-src --toolchain "$TOOLCHAIN" >/dev/null
rustup target add wasm32-unknown-unknown --toolchain "$TOOLCHAIN" >/dev/null

RUSTUP_TOOLCHAIN="$TOOLCHAIN" \
RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals \
  -C link-arg=--shared-memory \
  -C link-arg=--max-memory=1073741824 \
  -C link-arg=--import-memory \
  -C link-arg=--export=__wasm_init_tls \
  -C link-arg=--export=__tls_size \
  -C link-arg=--export=__tls_align \
  -C link-arg=--export=__tls_base" \
wasm-pack build \
  --target web \
  --out-dir examples/browser-threaded-query/pkg \
  -- \
  --features wasm-threads \
  -Z build-std=panic_abort,std
