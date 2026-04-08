#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

wasm-pack build --target web --out-dir examples/browser-gun-notes/pkg -- --features crypto
