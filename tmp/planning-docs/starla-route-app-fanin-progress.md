# Starla Route Application and Fan-In Progress

## Completed

- Reviewed the current public router schema in `src/router.rs`.
- Confirmed `RoutePayload` and `RouteBatchItem` do not yet have a first-class application/custom
  payload variant.
- Reviewed the current remote-interest types in `src/sync.rs`.
- Confirmed `RemoteWatchMessage` does not carry source peer or partial-failure metadata.
- Reviewed native relay/MoQ policy resolution in `src/native_sync.rs`.
- Confirmed current relay `RemoteInterestPolicy` resolution selects a single peer before sending a
  pull or watch.
- Reviewed native WebRTC mesh policy resolution in `src/native_mesh.rs`.
- Confirmed current mesh `RemoteInterestPolicy` resolution selects a single open mesh peer before
  sending a watch.
- Reviewed WASM policy resolution in `src/wasm.rs`.
- Confirmed browser WebSocket sync follows the same single-peer ambient policy behavior.
- Reviewed route-mode MoQ session typings and implementation in `packages/primadb-node/moq.d.ts`
  and `packages/primadb-node/moq.js`.
- Confirmed JS MoQ route sessions expose low-level route handlers but not typed application route
  subscriptions.
- Drafted the Starla route application and fan-in sprint plan.
- Clarified that the sprint should preserve existing advanced route-level APIs while keeping the
  new Starla-facing APIs free of raw transport-handle requirements.
- Added shared `ApplicationRouteMessage`, `ApplicationRouteEvent`, `ApplicationRouteFilter`, and
  `ApplicationRouteSubscription` types in `src/app_route.rs`.
- Added `RoutePayload::Application`, `RouteBatchItem::Application`, and
  `Router::wrap_application(...)`.
- Added a bounded shared `ApplicationRouteBus` with filtered subscriptions and deterministic
  `recv`/`try_recv`/`drain`/`close` behavior.
- Wired native WebSocket relay and `NativeMoqSync` application send/publish/subscribe APIs through
  the shared native sync state.
- Wired native WebRTC mesh application send/publish/subscribe APIs for direct WebRTC routes and
  relay/signaling fallback where applicable.
- Added `RemotePeerFailure`, `RemotePeerRecords`, `RemoteRecordConflict`,
  `RemoteRecordsFanIn`, `RemoteFanInWatchEvent`, and `RemoteFanInWatch`.
- Added deterministic fan-in merge behavior with source-tagged peer results, partial failures,
  conflicts, and per-peer cursors encoded into the merged cursor.
- Added relay/MoQ fan-in `records_fan_in(...)` and `watch_records_fan_in(...)`.
- Added WebRTC mesh fan-in `records_fan_in(...)` and `watch_records_fan_in(...)` using the
  existing mesh watch path for record results.
- Wired browser/WASM WebSocket sync and WebRTC mesh exports for application routes and record
  fan-in.
- Wired Node N-API bindings for WebSocket and WebRTC mesh application routes and record fan-in.
- Wired Python PyO3 bindings for WebSocket and WebRTC mesh application routes and record fan-in.
- Added typed application payload queues/subscriptions to browser `packages/primadb/moq.ts`.
- Added typed application payload queues/subscriptions to Node `packages/primadb-node/moq.js` and
  declarations in `moq.d.ts`.
- Added typed application payload helpers to the Python deterministic MoQ loopback helper.
- Updated package declarations/stubs for Node and Python public APIs.
- Added tests for application route serde/batch conversion, application bus filtering, in-memory
  application route delivery, and deterministic record fan-in merge/conflict behavior.

## Validation

- `cargo check --features "native-websocket native-webrtc native-moq" --lib`
- `cargo check --target wasm32-unknown-unknown --lib`
- `cargo test --lib --features "native-websocket native-webrtc native-moq"`
- `cargo check --manifest-path packages/primadb-node/Cargo.toml`
- `cargo check --manifest-path packages/primadb-python/Cargo.toml`
- `pnpm --dir packages/primadb run typecheck`
- `node --check packages/primadb-node/moq.js`
- `python -m py_compile packages/primadb-python/python/primadb/moq.py packages/primadb-python/python/primadb/__init__.py`
- Rebuilt browser artifacts with `pnpm --dir packages/primadb run build`.
- Rebuilt Node native artifact with `pnpm --dir packages/primadb-node run build`.
- Rebuilt Python native extension with `uv run maturin develop --release`.
- Rebuilt browser example artifacts with `pnpm --dir packages/primadb/examples run build`.
- Verified browser package smoke with `pnpm --dir packages/primadb run smoke`.
- Verified MoQ-backed browser mesh signaling with `pnpm --dir packages/primadb run smoke:moq-mesh-signaling`.
- Verified Node native package smokes:
  `node ./scripts/smoke-core.mjs`,
  `node ./scripts/smoke-hooks.mjs`,
  `node ./scripts/smoke-relay-server.mjs`,
  `node ./scripts/smoke-relay.mjs`,
  `node ./scripts/smoke-mesh.mjs`, and
  `node ./examples/moq-sync/index.mjs`.
- Verified Python package smokes:
  `uv run python scripts/smoke_core.py`,
  `uv run python scripts/smoke_hooks.py`,
  `uv run python scripts/smoke_relay_server.py`,
  `uv run python scripts/smoke_relay.py`,
  `uv run python scripts/smoke_relay_offline.py`,
  `uv run python scripts/smoke_mesh.py`, and
  `uv run python examples/moq_sync/main.py`.
- Verified Python wheel packaging with `uv run python scripts/pack_check.py`.
- Verified browser examples with `pnpm --dir packages/primadb/examples run smoke`.
- Verified native Rust relay and WebRTC mesh smokes with
  `bash examples/test-native-relay-smoke.sh` and
  `bash examples/test-native-mesh-smoke.sh`.
- Verified local native MoQ IETF route smoke with `bash scripts/smoke-native-moq-ietf-local.sh`.
- Verified live Node MoQ probe with `node ./scripts/smoke-moq-live.mjs` from
  `packages/primadb-node`: Cloudflare draft-14 passed; Cloudflare draft-07 returned
  `E_SESSION_CLOSED`.
- Verified live native MoQ probes:
  `cargo run --features native-moq --example native_moq_live_probe` passed draft-14 and draft-07,
  and `cargo run --features "native-websocket native-moq" --example native_gateway_moq_ws_live_probe`
  passed draft-14 and draft-07.
- Verified live browser MoQ route/signaling probe with
  `node ./moq-sync/test-live-route.mjs` from `packages/primadb/examples`: Cloudflare draft-14
  passed browser/browser route exchange, browser/Node route exchange, and browser WebRTC via MoQ
  signaling; Cloudflare draft-07 failed in JS/browser paths with WebTransport/session close
  errors.
- Regenerated generated API docs with `pnpm --dir website run generate:api`.
- Updated authored docs for application routes, record fan-in, and MoQ/WebTransport fallback
  semantics in the root README, routing/mesh concept docs, relay/full-node/mesh guide, MoQ guide,
  and package MoQ example README.
- Verified docs site generation with `pnpm --dir website run build`.

## Remaining

- JS/browser Cloudflare draft-07 MoQ remains unsupported by the current `@moq/lite`/WebTransport
  stack. Native Rust draft-07 works through the Cloudflare `moq-rs` draft-07 backend.
- Python MoQ remains a deterministic route-mode loopback helper; no Python Cloudflare live MoQ
  client has been added.
