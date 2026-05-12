# Dead-Code Warning Cleanup Plan

## Goal

Remove PrimaDB dead-code warnings that appear in Starla consumer builds without hiding unrelated
warnings globally or removing public APIs.

## Evidence Inputs

- PrimaDB repo status and recent commits.
- Starla consumer repo status and recent commits.
- Direct symbol search for all warned identifiers before edits.
- Starla reproductions:
  - `cargo test --features primadb-storage mesh --lib`
  - `cargo check --features primadb-native-moq --example native_mesh_live_smoke`
  - `cargo check --features primadb-native-webrtc --example native_mesh_live_smoke`
- PrimaDB reproductions:
  - `cargo check --no-default-features --features crypto --lib`
  - `cargo check --features "native-websocket native-webrtc native-moq" --lib`
  - `cargo test --lib --features "native-websocket native-webrtc native-moq"`

## Cleanup Rules

- Prefer cfg cleanup when code is intentionally compiled only for transport or wasm paths.
- Remove code only when evidence shows it is stale and not part of a public or planned surface.
- Use targeted `allow(dead_code)` only for intentionally dormant cross-feature plumbing that cannot
  be expressed cleanly with cfg.
- Preserve public data types and SDK/package API surfaces.

## Classification

- `ApplicationRouteBus`, `ApplicationRouteSubscriber`, and queue capacity:
  transport/test-only internals for application route delivery. Use cfg rather than removal.
- Storage transaction and external storage hook helpers:
  wasm browser storage/test internals. Use wasm/test cfg rather than removal.
- Node-fetch scheduler id and registration helpers:
  transport/wasm/test internals. Use transport/wasm/test cfg.
- Session presence/auth helper functions:
  transport/wasm/test internals, with crypto-specific bodies where required. Use transport/wasm/test
  cfg.
- Network hook serving helpers:
  relay/mesh/wasm/test internals. Use transport-specific cfg.
- `allow_room_join`:
  mesh/wasm/test only, not needed by native MoQ/WebSocket relay-only builds. Use tighter mesh/wasm
  cfg.
- Remote watch/fan-in constructors:
  native relay/mesh/test constructors. Use native-websocket/test cfg.

## Validation

Run the reproduction commands above, plus wasm compile checks because the patch touches wasm-gated
helpers.
