#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

./examples/browser-mesh-notes/build.sh

node ./examples/browser-mesh-notes/test-browser-native-smoke.mjs
