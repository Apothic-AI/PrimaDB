#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "examples/browser-mesh-notes/pkg/primadb.js" ]; then
  ./examples/browser-mesh-notes/build.sh
fi

node ./examples/browser-mesh-notes/test-two-page-smoke.mjs
