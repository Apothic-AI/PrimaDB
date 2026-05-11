# Transport Unification Plan

## Problem

PrimaDB currently has a rich router overlay (`RouteEnvelope`) for WebSocket relay and WebRTC mesh,
plus a package-local MoQ helper that sends raw sync envelopes over MoQ tracks. That leaves gaps:

- MoQ peers are not first-class PrimaDB router peers.
- WebSocket relay, WebRTC direct links, and MoQ/WebTransport do not share one transport contract.
- A relay/full node cannot currently bridge MoQ clients and WebRTC/WebSocket peers at the route
  layer.
- Fallback behavior is split across transport-specific helpers instead of one network model.

## Direction

Make `RouteEnvelope` the single PrimaDB overlay protocol. Treat WebSocket, MoQ/WebTransport, MoQ
WebSocket fallback, WebRTC data channels, and browser BroadcastChannel as underlay transports that
carry the same route envelopes.

Transport roles:

- Relay underlay: client-server route exchange through WebSocket or MoQ/WebTransport.
- Direct underlay: peer-to-peer route exchange through WebRTC data channels.
- Local/test underlay: in-memory or BroadcastChannel route exchange for deterministic tests and
  same-browser demos.

MoQ should be implemented as a relay underlay, not as a separate sync protocol. A MoQ-connected
client can exchange route traffic through a generic MoQ relay/service such as Cloudflare MoQ without
requiring that relay to be a PrimaDB full node. A PrimaDB-aware full node or gateway is still needed
when route traffic must be bridged between MoQ clients and WebSocket/WebRTC peers, unless every peer
participates in the same route-mode MoQ namespace directly. A MoQ-only client is not directly
WebRTC-meshed unless it also supports WebRTC.

## Available Environment

Local `.env` provides concrete Cloudflare inputs for live validation:

- MoQ relay hosts for draft-07 and draft-14 Cloudflare Media over QUIC endpoints.
- Cloudflare STUN and TURN hostnames for WebRTC ICE validation.
- Cloudflare TURN and SFU API credentials for provisioning or testing authenticated relay/media
  paths.

Do not commit `.env` or derived secrets. Planning and docs should reference variable names and
public hostnames only.

## Proposed Architecture

1. Introduce a transport-neutral route session abstraction.
   - Each session sends and receives `RouteEnvelope`.
   - Each session reports transport kind, peer/session id, capabilities, connection state, and
     backpressure/close state.
   - Dedupe stays at the router layer via route id/content hash/seen metadata.

2. Refactor relay and mesh runtimes around a shared route coordinator.
   - The coordinator owns `Router`, pending remote requests, incoming watch state, peer presence,
     peer recommendations, and route forwarding decisions.
   - WebSocket relay clients, MoQ relay clients, and WebRTC direct links become adapters.
   - Forward route envelopes before applying DB operations so cross-transport bridging does not
     rely on re-adding remote ops to `pending_ops`.

3. Add a PrimaDB MoQ relay profile.
   - Use MoQ/WebTransport as a route-envelope underlay.
   - Define stable path/track naming for upstream, peer downlink, room broadcast, and optionally
     chunked route objects.
   - Support WebSocket fallback inside the MoQ adapter where the MoQ stack provides it.
   - Keep the current package-local sync-envelope helper only as a legacy/simple example until the
     route-mode MoQ adapter replaces it.

4. Turn the native full node into a multi-transport route relay/gateway.
   - One full node can listen on WebSocket and MoQ/WebTransport simultaneously.
   - It forwards route envelopes across all connected sessions.
   - If it has a local DB, it also participates as a normal peer/authority.
   - Session auth, hooks, redaction, pull/watch serving, and strict-scope authority behavior apply
     consistently regardless of ingress transport.
   - This complements, but does not replace, generic MoQ relay services; generic MoQ relays can
     fan out route envelopes, while PrimaDB-aware gateways understand and bridge the overlay.

5. Generalize public network config.
   - Replace transport-specific relay URLs with a tagged relay endpoint config:
     `websocket`, `moq`, and later additional relay underlays.
   - Let `connectMesh(...)` use any relay endpoint for discovery/signaling, not only WebSocket.
   - Keep direct WebRTC settings separate from relay endpoint settings.

6. Define fallback semantics explicitly.
   - Underlay fallback: MoQ over WebTransport can fall back to MoQ over WebSocket while preserving
     one logical route session.
   - Overlay fallback: if WebRTC direct links are unavailable, route traffic remains relay-routed.
   - Cross-transport bridge: relay/full node forwards route envelopes between underlays; DB-level
     remote operation application is not used as a gossip bridge.

## Implementation Tranches

1. Write transport contract tests.
   - In-memory route sessions.
   - Broadcast, targeted peer, duplicate suppression, TTL, content hash, and peer presence.

2. Extract shared route coordinator.
   - Move duplicated WebSocket/WebRTC routing logic behind a common coordinator.
   - Preserve current WebSocket and WebRTC behavior.

3. Add route-mode MoQ spike against Cloudflare MoQ.
   - Evaluate `moq-native`/`moq-lite`/`web-transport` compatibility with existing `@moq/lite`.
   - Prove one browser client and one native client can exchange `RouteEnvelope` through
     `MOQ_DRAFT07_RELAY` or `MOQ_DRAFT14_RELAY`.
   - Identify whether PrimaDB should target one MoQ draft first or support both during transition.
   - Decide dependency and feature flags after the interop spike, not before.

4. Add native MoQ relay/full-node listener and Cloudflare-compatible client path.
   - Serve MoQ route sessions.
   - Bridge MoQ and WebSocket clients through one route coordinator.
   - Add session auth and hook coverage.
   - Verify route-mode MoQ over a generic public MoQ relay/service, not only a self-hosted PrimaDB
     full node.

5. Add MoQ relay clients to browser, Node, Python, and Rust.
   - Expose the same high-level relay/remote-watch API surface as WebSocket.
   - Keep MoQ transport details in endpoint config.

6. Use MoQ relay for WebRTC signaling.
   - Let `connectMesh(...)` signal through a MoQ relay endpoint.
   - Verify WebRTC-capable MoQ clients can discover and form direct WebRTC links.
   - Verify MoQ-only clients remain relay-routed but reachable.
   - Use `CLOUDFLARE_STUN` / `CLOUDFLARE_TURN` to validate practical ICE behavior.

7. Retire or reclassify the package-local MoQ sync helper.
   - Either deprecate it in favor of route-mode MoQ or keep it clearly documented as a low-level
     sync-envelope demo that does not participate in the PrimaDB router overlay.

## Verification Matrix

- WebSocket relay: two peers sync, pull, watch, and strict-scope submit.
- MoQ relay: two peers sync, pull, watch, and strict-scope submit.
- Mixed relay: WebSocket peer and MoQ peer discover each other and exchange route traffic.
- MoQ fallback: WebTransport unavailable, MoQ WebSocket fallback still passes route traffic.
- WebRTC via WebSocket signaling: existing mesh behavior remains intact.
- WebRTC via MoQ signaling: WebRTC-capable peers form direct data channels through MoQ relay
  signaling.
- MoQ-only plus WebRTC peer: traffic works through a PrimaDB-aware gateway or shared route-mode MoQ
  relay namespace, but no direct WebRTC link is expected.
- Multi-homed peer: same peer connected via multiple underlays does not loop or duplicate route
  delivery.
- Browser, Node, Python, and Rust examples cover both single-transport and mixed-transport cases.

## Risks

- MoQ JS/Rust library wire compatibility needs proof before picking dependencies.
- WebTransport deployment requires TLS/cert handling and secure browser contexts.
- MoQ path/track naming must support authorization, backpressure, chunking, and targeted delivery
  without becoming a second router protocol.
- Multi-transport peers need stable peer identity and route dedupe to avoid loops.
- The existing native/browser routing code is duplicated enough that the coordinator extraction
  should happen before adding a third transport.

## Residual Gap Completion Plan

### Gap 1: Live Cloudflare MoQ Interop

Goal: prove the route-mode MoQ profile works against public Cloudflare MoQ endpoints and not only
in local/deterministic harnesses.

1. Add opt-in live smoke scripts.
   - Gate every live test behind explicit environment variables.
   - Use `MOQ_DRAFT14_RELAY` first, with a fallback matrix for `MOQ_DRAFT07_RELAY`.
   - Generate unique route paths per run, for example
     `primadb/live/moq/<timestamp>-<random>`.
   - Do not log secrets or commit `.env`.

2. Validate same-stack route exchange.
   - Browser/browser through `@moq/lite`.
   - Node/Node through `primadb-node/moq`.
   - Native/native through `NativeMoqRouteClient`.
   - For each, assert presence exchange, `RoutePayload::Sync`, duplicate suppression, and close
     behavior.

3. Validate cross-stack route exchange.
   - Browser to Node.
   - Browser to native.
   - Node to native.
   - Verify that all stacks agree on `primadb.route.v1`, route target encoding, path/track names,
     and `sync_frame` payload JSON.

4. Validate gateway bridge behavior.
   - Start a native relay server with a MoQ uplink.
   - Connect one peer over WebSocket and one peer over MoQ.
   - Assert presence, sync, pull, watch, and strict-scope transaction behavior across the gateway.
   - Add multi-homed loop/dedupe checks by connecting one logical peer through both WebSocket and
     MoQ.

5. Decide draft support policy from evidence.
   - Prefer draft-14 if both JS and Rust stacks pass.
   - If draft-07 fails or requires incompatible framing, keep it as best-effort/documented rather
     than hiding compatibility risk.
   - Record observed Cloudflare endpoint behavior in the progress doc.

### Gap 2: Browser `connectMesh(...)` MoQ Signaling

Goal: give browser WebRTC mesh the same MoQ signaling capability now available to native WebRTC
mesh.

1. Add an external signaling adapter to the WASM mesh runtime.
   - Introduce a browser-only mesh signaling variant that does not own WebSocket or
     BroadcastChannel.
   - Expose methods on `WebRtcMesh` for injected incoming route/signaling messages.
   - Route outgoing `RoutePayload::Signal` through a JS callback supplied at construction time.
   - Keep the existing WebRTC data-channel routing unchanged.

2. Build a TypeScript MoQ signaling wrapper.
   - Add `connectMeshViaMoq(db, options)` in `packages/primadb/moq.ts` or a dedicated mesh module.
   - Internally create a route-mode `PrimadbMoqSession`.
   - Construct the WASM mesh with external signaling.
   - Forward MoQ `onRoute(...)` events into the WASM mesh.
   - Forward WASM mesh outgoing signaling routes into the MoQ session.

3. Align browser config types.
   - Add `RelayEndpointConfig`/`MoqRelayClientConfig` to browser package types.
   - Let `connectMesh(...)` accept a MoQ relay endpoint at the JS package layer.
   - Preserve existing WebSocket and BroadcastChannel paths.

4. Verify WebRTC behavior.
   - Local deterministic test: use the in-process MoQ transport pair to signal two browser peers.
   - Browser smoke test: two pages/tabs form WebRTC data channels via MoQ signaling.
   - Live Cloudflare test: route-mode MoQ signaling plus `CLOUDFLARE_STUN`/`CLOUDFLARE_TURN`.
   - Assert MoQ-only peers remain relay-routed and are not expected to form WebRTC links.

5. Document browser support.
   - Update browser examples and docs to show WebSocket signaling, BroadcastChannel signaling, and
     MoQ signaling as equivalent signaling underlays.
   - Clearly call out secure-context/WebTransport requirements and WebSocket fallback behavior.
