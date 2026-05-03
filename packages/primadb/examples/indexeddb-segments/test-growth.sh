#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
node ./indexeddb-segments/test-growth.mjs
