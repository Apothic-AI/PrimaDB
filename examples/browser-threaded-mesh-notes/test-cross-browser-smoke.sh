#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "examples/browser-threaded-mesh-notes/pkg/primadb.js" ]; then
  ./examples/browser-threaded-mesh-notes/build.sh
fi

node ./examples/browser-threaded-mesh-notes/test-cross-browser-smoke.mjs
