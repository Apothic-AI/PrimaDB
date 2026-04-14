#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

./examples/browser-relay-notes/build.sh

node ./examples/browser-relay-notes/test-watch-smoke.mjs
