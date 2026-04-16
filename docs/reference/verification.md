---
title: Verification Matrix
sidebar_position: 4
---

PrimaDB is not relying on a single smoke check. The repo currently exercises:

- Rust tests
- WASM compile checks
- browser relay smokes
- browser mesh smokes
- threaded browser mesh smokes
- native relay smokes
- native mesh smokes
- Node package smokes
- Python package smokes
- cross-target all-build mesh and storage end-to-end tests

## Core Verification Commands

```bash
cd /home/bitnom/Code/gunport/primadb
cargo test --features "crypto native-websocket native-webrtc"
cargo check --target wasm32-unknown-unknown --features crypto
bash examples/test-native-relay-smoke.sh
bash examples/test-native-mesh-smoke.sh
bash examples/browser-relay-notes/test-watch-smoke.sh
bash examples/test-all-targets-mesh-e2e.sh
```

## Package Verification

Browser package:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
npm run typecheck
npm run smoke
```

Node package:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run smoke
```

Python package:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
uv run python scripts/smoke_core.py
uv run python scripts/smoke_hooks.py
uv run python scripts/smoke_relay.py
uv run python scripts/smoke_mesh.py
```
