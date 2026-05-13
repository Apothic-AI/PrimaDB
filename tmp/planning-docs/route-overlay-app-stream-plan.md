# Route Overlay And Application Stream Sprint Plan

## Goal

Add PrimaDB networking primitives that let higher-level runtimes keep their own mesh domain APIs while PrimaDB owns multi-underlay route sending, route delivery diagnostics, event pumping, verified sender context, and reliable application streams over `RouteEnvelope`.

## Scope

- Work in PrimaDB only.
- Keep existing low-level `RouteEnvelope`, WebSocket, MoQ, WebRTC, in-memory, and SDK helper APIs available.
- Add higher-level route overlay/session APIs instead of replacing existing transport-specific APIs.
- Do not modify the Starla subrepo during this sprint.

## Tranches

1. Shared route context and delivery types.
   - Add source/underlay/auth/provenance context to application route events.
   - Add route overlay policy and send report types.
   - Preserve existing event fields for current callers.

2. Route overlay session core.
   - Add a shared `RouteOverlaySession`.
   - Register multiple route underlays.
   - Send one route through policy-ordered underlays.
   - Support first-success and fan-out modes.
   - Provide deterministic route/event pumping and duplicate suppression.

3. Reliable application stream core.
   - Add protocol-neutral stream frame/message types carried as application routes.
   - Support ordered chunks, ack/nack, close, error, receive reassembly, and diagnostics.
   - Expose stream send helpers on the overlay session.

4. Native and WASM route hooks.
   - Add public route-envelope send methods where transport handles already exist.
   - Keep existing `send_application` and `subscribe_applications` APIs intact.

5. SDK parity.
   - Update Rust exports.
   - Update browser/Node MoQ route-session helpers and declarations.
   - Update native Node/Python declarations where events and route envelopes are exposed.

6. Tests and docs.
   - Add core overlay tests for fallback, fan-out duplicate suppression, partial failure diagnostics, and app stream reassembly.
   - Update README, routing docs, API docs, and package docs.
   - Run focused Rust checks/tests and package type checks where feasible.

## Acceptance Criteria

- A caller can register multiple underlays and call one high-level app-route send method.
- Send reports include attempted underlays, delivered underlays/peers, failures, fallback reason, and duplicate-suppression counts.
- Event consumers can use `recv`/`try_recv`/`drain` and receive route context.
- App streams are encoded as `RoutePayload::Application` and reassembled deterministically.
- Existing application route, fan-in, WebSocket, MoQ, WebRTC, and in-memory tests continue passing.

## Implementation Status

Completed in this sprint:

- Shared Rust overlay/session core with sync and async underlay send support.
- Native WebSocket, native MoQ, and native WebRTC route-overlay underlay adapters.
- Event pumping from raw route sessions and existing application-route subscriptions.
- Verified sender context on app route events.
- Route-level application streams with ordered chunk reassembly.
- Browser/Node MoQ route overlay helpers.
- Node/Python/browser declaration updates where exposed.
- README, routing, mesh, guide, and generated API doc text updates.
