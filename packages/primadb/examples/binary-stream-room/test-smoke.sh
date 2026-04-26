#!/usr/bin/env bash
set -euo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EXAMPLES_DIR"

pnpm run build

node ./binary-stream-room/test-smoke.mjs
