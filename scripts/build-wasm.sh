#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR=""
FEATURES=""
TOOLCHAIN="${PRIMADB_WASM_TOOLCHAIN:-stable}"
TARGET="${PRIMADB_WASM_TARGET:-web}"
THREADS=0
DEV=0
WASM_RUSTFLAGS="${RUSTFLAGS:-}"

append_feature() {
  local feature="$1"
  local existing="${FEATURES:-}"
  for candidate in $existing; do
    if [[ "$candidate" == "$feature" ]]; then
      return
    fi
  done
  FEATURES="${existing:+$existing }$feature"
}

while (($# > 0)); do
  case "$1" in
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --features)
      FEATURES="${2:-}"
      shift 2
      ;;
    --toolchain)
      TOOLCHAIN="${2:-}"
      shift 2
      ;;
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --threads)
      THREADS=1
      shift
      ;;
    --dev)
      DEV=1
      shift
      ;;
    --help|-h)
      cat <<'EOF'
Usage:
  scripts/build-wasm.sh --out-dir <path> [options]

Options:
  --features <list>    Cargo feature list passed to wasm-pack.
  --toolchain <name>   Rust toolchain to use. Defaults to stable, or to
                       PRIMADB_WASM_TOOLCHAIN if set.
  --target <name>      wasm-pack target. Defaults to web.
  --threads            Build the threaded wasm-threads variant.
  --dev                Build in dev mode instead of release mode.

Examples:
  ./scripts/build-wasm.sh --out-dir examples/browser-notes/pkg
  ./scripts/build-wasm.sh --out-dir examples/browser-gun-notes/pkg --features crypto
  ./scripts/build-wasm.sh --out-dir dist/pkg --threads
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  echo "--out-dir is required" >&2
  exit 1
fi

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack is required. Install it with: cargo install wasm-pack" >&2
  exit 1
fi

PACK_ARGS=(build --target "$TARGET" --out-dir "$OUT_DIR")
if ((DEV)); then
  PACK_ARGS+=(--dev)
fi
if ! command -v wasm-opt >/dev/null 2>&1; then
  PACK_ARGS+=(--no-opt)
fi

CARGO_ARGS=()
THREAD_BUILD_STD=0

if ((THREADS)); then
  TOOLCHAIN="${PRIMADB_WASM_THREADS_TOOLCHAIN:-nightly}"
  append_feature "wasm-threads"
  rustup component add rust-src --toolchain "$TOOLCHAIN" >/dev/null
  rustup target add wasm32-unknown-unknown --toolchain "$TOOLCHAIN" >/dev/null
  THREAD_BUILD_STD=1
  WASM_RUSTFLAGS="${WASM_RUSTFLAGS:+$WASM_RUSTFLAGS }-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-arg=--shared-memory -C link-arg=--max-memory=1073741824 -C link-arg=--import-memory -C link-arg=--export=__wasm_init_tls -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align -C link-arg=--export=__tls_base"
else
  rustup target add wasm32-unknown-unknown --toolchain "$TOOLCHAIN" >/dev/null
fi

if [[ -n "$FEATURES" ]]; then
  CARGO_ARGS+=(--features "$FEATURES")
fi
if ((THREAD_BUILD_STD)); then
  CARGO_ARGS+=(-Z build-std=panic_abort,std)
fi

if ((${#CARGO_ARGS[@]} > 0)); then
  RUSTUP_TOOLCHAIN="$TOOLCHAIN" RUSTFLAGS="$WASM_RUSTFLAGS" wasm-pack "${PACK_ARGS[@]}" -- "${CARGO_ARGS[@]}"
else
  RUSTUP_TOOLCHAIN="$TOOLCHAIN" RUSTFLAGS="$WASM_RUSTFLAGS" wasm-pack "${PACK_ARGS[@]}"
fi
