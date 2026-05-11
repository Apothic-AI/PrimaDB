# Starla Route Application and Fan-In Sprint Plan

## Sprint Goal

Expose RouteEnvelope-level primitives that let Starla move trusted mesh-channel,
trust/share/profile/vault proposal, and future memory-coordination traffic through
PrimaDB without raw transport handles, while adding true multi-peer remote record fan-in.

## Implementation Status

Implemented in the current sprint branch. Remaining work is limited to live transport smoke
validation and generated package artifacts, both tracked in the companion progress document.

## Current Evidence

- `RouteEnvelope`, `RouteTarget`, and `RoutePayload` are public, but `RoutePayload` has no
  first-class application/custom payload variant.
- `PrimadbMoqSession` exposes low-level route creation and `onRoute(...)`, but there is no typed
  application route queue/subscription API.
- Native relay, native MoQ, native mesh, and WASM WebSocket mesh paths all handle `RoutePayload`
  internally, but unknown application payloads are currently dropped or ignored by the DB sync
  handlers.
- `RemoteInterestPolicy` supports `any`, `peer`, and `peers`, but current relay/mesh policy
  resolution selects one concrete peer before sending a pull/watch.
- `RemoteWatchMessage` only carries `initial` and `result`; it does not identify the source peer,
  transport, watch id, or partial-failure diagnostics.
- The earlier ambient remote-interest plan explicitly deferred multi-peer fan-in.

## Architecture Direction

Keep `RouteEnvelope` as the only public network protocol. Add higher-level application and fan-in
APIs on top of RouteEnvelope and existing pull/watch messages rather than exposing transport
internals.

This sprint should not remove existing advanced public APIs such as `RouteEnvelope`,
`NativeMoqRouteClient`, route-mode MoQ `sendRoute(...)`/`onRoute(...)`, or test relay/session
helpers. The Starla-facing additions should simply avoid requiring raw transport handles or socket
internals for application delivery and remote fan-in.

Transport roles remain unchanged:

- WebSocket: relay RouteEnvelope underlay.
- MoQ/WebTransport: relay RouteEnvelope underlay.
- WebRTC: direct peer RouteEnvelope underlay.
- BroadcastChannel/in-memory: local/test RouteEnvelope underlay.

New application and fan-in primitives should share as much Rust code as possible between native and
WASM. SDK bindings should be thin shape/convenience layers over the shared core.

## Public Application Route Design

Add a first-class payload variant:

```rust
pub enum RoutePayload {
    Application { message: ApplicationRouteMessage },
    // existing variants...
}

pub enum RouteBatchItem {
    Application { message: ApplicationRouteMessage },
    // existing variants...
}
```

Application message shape:

```rust
pub struct ApplicationRouteMessage {
    pub namespace: String,
    pub protocol: String,
    pub topic: Option<String>,
    pub body: serde_json::Value,
    pub metadata: BTreeMap<String, serde_json::Value>,
}
```

Received event shape:

```rust
pub struct ApplicationRouteEvent {
    pub route_id: String,
    pub from: String,
    pub channel: String,
    pub target: RouteTarget,
    pub issued_at_millis: u64,
    pub received_at_millis: u64,
    pub transport: RouteTransportKind,
    pub verified_identity: Option<VerifiedIdentity>,
    pub message: ApplicationRouteMessage,
}
```

Application filtering:

```rust
pub struct ApplicationRouteFilter {
    pub namespace: Option<String>,
    pub protocol: Option<String>,
    pub topic: Option<String>,
}
```

The `RouteEnvelope` already has `route_id`, `from`, `channel`, `target`, `ttl`, `hops`,
`reply_to`, `content_hash`, and `seen_by`; do not duplicate those inside the application message
unless a caller explicitly includes app-level metadata.

## Application Route APIs

Rust native and shared core:

- `publish_application(message, target) -> Result<RouteEnvelope>`
- `send_application(namespace, protocol, topic, body, metadata, target) -> Result<RouteEnvelope>`
- `subscribe_applications(filter) -> ApplicationRouteSubscription`
- `next_application()`, `try_next_application()`, and `drain_applications()` where the owning type
  already uses deterministic pull-style APIs.
- `ApplicationRouteSubscription::close()`

WASM/browser and Node TypeScript:

- `publishApplication(message, target?)`
- `sendApplication(namespace, protocol, topic, body, metadata?, target?)`
- `subscribeApplications(filter?)`
- subscription `next()`, `tryNext()`, `drain()`, and `close()`
- optional callback helper `onApplicationRoute(filterOrHandler, handler?)`

Python:

- `publish_application(message, target=None)`
- `send_application(namespace, protocol, topic, body, metadata=None, target=None)`
- `subscribe_applications(filter=None)`
- subscription `next()`, `try_next()`, `drain()`, and `close()`

Do not make the new application APIs depend on raw WebSocket, MoQ, WebTransport, WebRTC
data-channel, relay-client, or gateway internals. Keep existing advanced route-level APIs available
unless a separate cleanup explicitly decides otherwise.

## Application Route Implementation Plan

1. Add shared route types.
   - Add `ApplicationRouteMessage`, `ApplicationRouteEvent`, `ApplicationRouteFilter`, and
     `ApplicationRouteSubscription`.
   - Add `RoutePayload::Application` and `RouteBatchItem::Application`.
   - Add `Router::wrap_application(...)`.
   - Add serde tests for stable JSON names, content hashes, batch conversion, and topic/broadcast/
     peer targets.

2. Add a shared application event queue.
   - Create a small reusable `ApplicationRouteBus`/queue module.
   - Apply namespace/protocol/topic filters at subscription delivery time.
   - Keep queues bounded by existing network limits or a new `max_application_route_queue` limit.
   - Include close/cancel behavior and deterministic drain support.

3. Wire native relay and native MoQ.
   - Native WebSocket and `NativeMoqSync` share `NativeWebSocketSyncState`; add one application bus
     there.
   - In `handle_route_payload`, pass `RoutePayload::Application` through route auth/trust checks
     before enqueueing.
   - Add public send/publish/subscribe methods to `NativeWebSocketSync` and `NativeMoqSync`.
   - Ensure application payloads can also be packed inside batch routes.

4. Wire native WebRTC mesh.
   - Add the application bus to `NativeWebRtcMeshState`.
   - In mesh route handling, enqueue application messages after router acceptance and auth/trust
     checks.
   - Add public send/publish/subscribe methods to `NativeWebRtcMesh`.

5. Wire WASM/browser route sessions.
   - Add WASM exports for application message/filter/event/subscription shapes.
   - Add app-route send/subscribe methods to browser WebSocket sync and WebRTC mesh.
   - Add app-route support to `connectMeshViaMoq(...)` without exposing the underlying MoQ session
     as the primary Starla integration surface.

6. Wire JS MoQ route sessions.
   - Add typed `application` payload support to `packages/primadb/moq.ts` and
     `packages/primadb-node/moq.js`.
   - Keep `onRoute(...)` for powerful low-level users, but add `sendApplication(...)`,
     `publishApplication(...)`, `subscribeApplications(...)`, `nextApplication(...)`,
     `tryNextApplication(...)`, and `drainApplications(...)`.
   - Route-mode MoQ remains a generic RouteEnvelope underlay, so generic MoQ relays can fan out app
     payloads when all peers share the same route-mode MoQ profile.

7. Wire Python deterministic MoQ loopback.
   - Add app-route helpers to the Python MoQ loopback helper for deterministic tests.
   - Do not add a new Python live MoQ client in this sprint.

## Remote Records Fan-In Design

Add explicit fan-in APIs rather than silently changing the existing single-peer ambient methods in
the first tranche. Existing `records(scan, policy)` and `watchRecords(scan, policy)` may later be
made aliases if Starla and other callers prefer that behavior.

Policy semantics:

- `target: "peer"` means exactly one peer.
- `target: "peers"` means all listed peers that satisfy auth/capability checks.
- `target: "any"` means all currently reachable policy-matching peers, not only the highest-ranked
  peer, for fan-in APIs.
- `requireCapability` continues to filter by advertised capability.

Fan-in result shape:

```rust
pub struct RemotePeerFailure {
    pub peer_id: String,
    pub transport: RouteTransportKind,
    pub message: String,
}

pub struct RemotePeerRecords {
    pub peer_id: String,
    pub transport: RouteTransportKind,
    pub result: RecordScanResult,
}

pub struct RemoteRecordsFanIn {
    pub request_id: String,
    pub records: Vec<RemotePeerRecords>,
    pub failures: Vec<RemotePeerFailure>,
    pub merged: RecordScanResult,
    pub conflicts: Vec<RemoteRecordConflict>,
}
```

Watch fan-in event shape:

```rust
pub enum RemoteFanInWatchEvent {
    Update {
        peer_id: String,
        transport: RouteTransportKind,
        initial: bool,
        sequence: u64,
        result: RemoteResult,
    },
    Failure {
        peer_id: String,
        transport: RouteTransportKind,
        message: String,
        terminal: bool,
    },
    Closed,
}
```

Dedupe strategy:

- Preserve per-peer results in full.
- Build `merged` deterministically by stable record key ordering.
- When the same record key appears from multiple peers with byte-identical serialized content,
  keep one merged record and record all sources.
- When the same record key appears with different serialized content, keep a deterministic winner
  and surface a `RemoteRecordConflict` with all source peers and hashes.
- Include enough source metadata for Starla to ignore `merged` and implement its own reconciliation
  if needed.

Pagination strategy:

- Preserve each peer's `next_cursor`.
- Use an opaque fan-in cursor containing per-peer cursors if merged pagination is needed.
- Do not collapse peer cursors into a single plain cursor.

## Fan-In APIs

Rust native and shared core:

- `records_fan_in(scan, policy) -> Result<RemoteRecordsFanIn>`
- `watch_records_fan_in(scan, policy) -> Result<RemoteFanInWatch>`
- `RemoteFanInWatch::recv()`, `try_recv()`, `recv_blocking()` on native, and `close()`
- helper `resolve_remote_interest_peers(policy, capability, request) -> Result<Vec<PeerPresence>>`

WASM/browser and Node TypeScript:

- `recordsFanIn(scan, policy?)`
- `watchRecordsFanIn(scan, policy?)`
- watch `next()`, `tryNext()`, `drain()`, and `close()`

Python:

- `records_fan_in(scan, policy=None)`
- `watch_records_fan_in(scan, policy=None)`

Optional compatibility follow-up:

- Add `policy.mode: "single" | "fan_in"` if we decide `records(...)` and `watchRecords(...)`
  should select behavior by policy rather than separate method names.

## Fan-In Implementation Plan

1. Extract peer resolution.
   - Replace single-peer-only internal helpers with reusable peer-list resolution helpers.
   - Keep existing single-peer ambient methods by taking the first resolved peer.
   - Share policy validation between native relay, native mesh, and WASM.

2. Add source-aware remote response structs.
   - Keep current `RemoteResult` and `RemoteWatchMessage` for existing APIs.
   - Add fan-in-specific result/event types with source peer, transport, request/watch id, and
     partial failure fields.
   - Convert existing response accumulation logic into reusable per-peer accumulators.

3. Implement native relay fan-in pulls.
   - Send one pull request per selected peer.
   - Aggregate successes and failures without failing the whole call unless no peers were selected
     or all selected peers fail before producing diagnostics.
   - Surface partial failures in `RemoteRecordsFanIn.failures`.

4. Implement native relay fan-in watches.
   - Start one watch per selected peer.
   - Forward updates as source-aware fan-in events.
   - Emit per-peer failures without closing the whole fan-in watch unless every child watch is
     terminal or the caller closes it.
   - Cancel every child watch on close.

5. Implement native WebRTC mesh fan-in.
   - Reuse the same fan-in accumulator and watcher code where possible.
   - Resolve only open mesh data-channel peers.
   - Preserve direct P2P source metadata as `RouteTransportKind::WebRtc`.

6. Implement WASM/browser fan-in.
   - Mirror native fan-in semantics for WebSocket sync and browser WebRTC mesh.
   - Avoid Rust `std::thread`/blocking paths; use async channels and JS-friendly polling.

7. Expose Node and Python bindings.
   - Add napi and pyo3 conversions for fan-in result, conflict, failure, and watch event shapes.
   - Update `.d.ts`, `.pyi`, package READMEs, and generated API docs.

## Transport Parity Matrix

| Surface | Application Routes | Records Fan-In | Watch Fan-In | Notes |
| --- | --- | --- | --- | --- |
| `NativeWebSocketSync` | Required | Required | Required | Shared relay state path. |
| `NativeMoqSync` | Required | Required | Required | Uses same state as native WebSocket plus MoQ route client. |
| `NativeWebRtcMesh` | Required | Required | Required | Direct P2P source transport metadata. |
| WASM/browser WebSocket sync | Required | Required | Required | JS polling and callbacks. |
| WASM/browser WebRTC mesh | Required | Required | Required | Includes `connectMesh(...)` and external signaling. |
| browser/Node `PrimadbMoqSession` | Required | Not directly required | Not directly required | Route-only session; DB fan-in belongs to DB sync/mesh handles. |
| `connectMeshViaMoq(...)` | Required | Through returned mesh handle | Through returned mesh handle | MoQ is signaling/relay underlay, WebRTC remains direct when available. |
| Python MoQ loopback | Required for tests | Not required | Not required | Deterministic route-mode helper only. |

## Test Plan

1. Route schema and router tests.
   - `RoutePayload::Application` serde round trip.
   - batch conversion preserves application payloads.
   - content hash and duplicate suppression work for application payloads.
   - peer/topic/broadcast targets deliver through `InMemoryRouteHub`.

2. Application route transport tests.
   - in-memory route hub sends and receives application events with source metadata.
   - native WebSocket local relay sends application payloads both directions.
   - native MoQ deterministic/local route harness sends application payloads.
   - JS MoQ loopback sends application payloads.
   - native WebRTC deterministic harness sends application payloads if available; otherwise add a
     fake data-channel route-session harness before live WebRTC coverage.
   - browser `connectMeshViaMoq(...)` smoke forwards application messages through the route overlay.

3. Fan-in pull tests.
   - three reachable peers, two return record scan results, one returns an error.
   - `records_fan_in` returns both successful source-tagged results and one partial failure.
   - deterministic dedupe handles identical records and conflict records.
   - `target: "peer"`, `target: "peers"`, `target: "any"`, and `requireCapability` are covered.

4. Fan-in watch tests.
   - watch emits initial updates from multiple peers.
   - updates include source peer, transport, sequence, and initial flag.
   - one peer failure is surfaced without closing the whole watch.
   - close cancels all child watches and prevents further events.

5. Binding and docs checks.
   - Rust native feature checks for WebSocket, WebRTC, MoQ, and combined features.
   - WASM/browser build and TypeScript typecheck.
   - Node declaration typecheck and focused napi tests.
   - Python stub syntax check and focused pyo3 tests.
   - API docs regenerated.

## Acceptance Criteria

- Starla can publish and consume application RouteEnvelope payloads without raw transport handles.
- Application route events include source peer metadata and, where available, verified identity.
- Application route APIs exist for native WebSocket, native MoQ, native WebRTC, WASM/browser sync,
  browser WebRTC mesh, browser/Node MoQ route sessions, `connectMeshViaMoq(...)`, and Python
  deterministic MoQ loopback.
- Multi-peer `records_fan_in` returns source-tagged peer results, deterministic merged output,
  conflicts, and partial failures.
- Multi-peer `watch_records_fan_in` emits source-tagged updates and partial failures and closes
  cleanly.
- Existing single-peer ambient APIs continue passing tests.
- Existing sync/pull/watch/WebRTC/MoQ transport tests continue passing.
- Docs and generated API surfaces clearly distinguish RouteEnvelope application routes from raw
  transport handles.

## Risks and Decisions

- Backpressure: application queues must be bounded; dropping policy should be explicit in docs and
  tests.
- Auth identity: verified identity exists on native relay/mesh paths but may not be uniformly
  available in JS-only MoQ sessions. Events should use `verifiedIdentity: null` when not available.
- Fan-in pagination: per-peer cursors are required; a single cursor string would be misleading.
- Watch lifecycle: fan-in watch close must cancel all child watches even across mixed transports.
- API naming: keep explicit `FanIn` names first to avoid surprising users of current ambient
  single-peer methods.
- Generic MoQ relays do not enforce PrimaDB hooks/auth; those checks happen in PrimaDB-aware
  clients/gateways. Application route APIs should not imply relay-side authorization unless a
  PrimaDB gateway is in the path.
