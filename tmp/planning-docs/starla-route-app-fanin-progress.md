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

## Remaining

- Live relay/MoQ/WebRTC interop smoke coverage for the new APIs is still not run in this sprint.
- Browser generated `dist/` artifacts are not regenerated until the package build is run.
- Python and Node native binary artifacts are not rebuilt until their package build/release steps
  run.
