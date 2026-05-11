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

## In Progress

- Awaiting implementation approval or follow-up scope changes.

## Remaining

- Add shared application route payload/event/filter/subscription types.
- Wire application route send/receive APIs through native relay, native MoQ, native WebRTC, WASM,
  JS MoQ route sessions, Node, and Python where applicable.
- Add source-aware multi-peer records fan-in result types.
- Implement relay, MoQ, WebRTC, WASM, Node, and Python fan-in APIs where applicable.
- Add deterministic and transport-specific tests.
- Regenerate docs and API references.
