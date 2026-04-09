#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "examples/browser-gun-notes/pkg/primadb.js" ]; then
  ./examples/browser-gun-notes/build.sh
fi

node ./examples/browser-gun-notes/test-runtime-smoke.mjs
