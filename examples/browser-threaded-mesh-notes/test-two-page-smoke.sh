#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

./examples/browser-threaded-mesh-notes/build.sh

node ./examples/browser-threaded-mesh-notes/test-two-page-smoke.mjs
