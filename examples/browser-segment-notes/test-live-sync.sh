#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "examples/browser-segment-notes/pkg/primadb.js" ]; then
  ./examples/browser-segment-notes/build.sh
fi

node ./examples/browser-segment-notes/test-live-sync.mjs
