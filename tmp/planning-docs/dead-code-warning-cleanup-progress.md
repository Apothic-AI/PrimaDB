# Dead-Code Warning Cleanup Progress

## Completed

- Checked PrimaDB and Starla git status and recent commits before editing.
- Searched all warned PrimaDB symbols with `rg` before editing.
- Reproduced the broad warning set with `cargo check --no-default-features --features crypto --lib`.
- Reproduced the narrowed Starla native MoQ/WebRTC warning set before editing.
- Confirmed the broad warning set consisted of transport/wasm/test plumbing compiled into
  crypto-only builds.
- Confirmed the remaining native feature warnings were browser storage helpers plus
  `allow_room_join` in MoQ-only builds.
- Added precise cfg gates instead of global `allow(dead_code)`:
  - application route bus internals compile only for wasm, native relay-backed transports, or tests
  - browser storage transaction helpers compile only for wasm or tests
  - node-fetch scheduler registration helpers compile only for wasm, native relay-backed
    transports, or tests
  - session auth helpers compile only for wasm, native relay-backed transports, or tests
  - network hook serving helpers compile only for wasm, native relay-backed transports, or tests
  - `allow_room_join` compiles only for wasm, native WebRTC mesh, or tests
  - remote watch/fan-in constructors compile only for native relay-backed transports or tests
- Kept public route, auth, watch, fan-in, and package API types intact.
- Did not remove any public APIs.

## Validation

- `cargo fmt`
- `cargo check --no-default-features --features crypto --lib`
- `cargo check --no-default-features --lib`
- `cargo check --features native-moq --lib`
- `cargo check --features "native-websocket native-webrtc native-moq" --lib`
- `cargo test --lib --features "native-websocket native-webrtc native-moq"`
- `cargo check --target wasm32-unknown-unknown --features crypto --lib`
- `cargo check --target wasm32-unknown-unknown --no-default-features --lib`
- From `experiments/starla-slim`: `cargo test --features primadb-storage mesh --lib`
- From `experiments/starla-slim`: `cargo check --features primadb-native-moq --example native_mesh_live_smoke`
- From `experiments/starla-slim`: `cargo check --features primadb-native-webrtc --example native_mesh_live_smoke`

## Residual

- Starla still reports its own local dead-code warnings in `execute.rs`, `mesh.rs`, and
  `runtime.rs`; PrimaDB warnings are no longer present in the reproduced Starla checks.
