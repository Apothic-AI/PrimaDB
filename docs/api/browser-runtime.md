---
title: Browser Runtime API
sidebar_position: 3
---

This page covers the browser-facing `wasm_bindgen` runtime exported by the core crate. These are the classes and functions re-exported through the browser TypeScript package.

> Generated from `src/wasm.rs`.

## Top-level functions

### `parallelEnabled`

```ts
function parallelEnabled(): boolean;
```

### `parallelThreadCount`

```ts
function parallelThreadCount(): number;
```

### `generateSeaPair`

```ts
function generateSeaPair(): any;
```

### `derivePasswordKey`

```ts
function derivePasswordKey(password: string, options: any): any;
```

### `seaPairFromPrivateKeys`

```ts
function seaPairFromPrivateKeys(secret_key_base64: string, encryption_secret_key_base64: string): any;
```

### `seaSign`

```ts
function seaSign(pair: any, payload: any): any;
```

### `seaVerify`

```ts
function seaVerify(public_key_base64: string, signed: any): any;
```

### `seaSecret`

```ts
function seaSecret(pair: any, other_epub_base64: string): string;
```

### `seaEncrypt`

```ts
function seaEncrypt(key_base64: string, payload: any): any;
```

### `seaDecrypt`

```ts
function seaDecrypt(key_base64: string, payload: any): any;
```

## Runtime classes

### `Primadb`

```ts
class Primadb {
  constructor(replica_id: string | null);
  replicaId(): string;
  chain(root: string): Chain;
  scope(root: string): Scope;
  transaction(steps: any): any;
  attachNodeScript(path: any, script: any): void;
  removeNodeScript(path: any, script_id: string): void;
  nodeScripts(path: any): any;
  executeNodeScripts(path: any, options: any): any;
  snapshot(): any;
  snapshotForRoot(root: string | null): any;
  exportSnapshotJson(): string;
  importSnapshotJson(payload: string): void;
  mergeSnapshotJson(payload: string): void;
  pendingOperations(): any;
  pendingEnvelope(): any;
  exportPendingOperationsJson(): string;
  drainPendingOperations(): any;
  drainPendingEnvelope(): any;
  drainPendingOperationsJson(): string;
  drainPendingEnvelopeJson(): string;
  applyOperations(operations: any): number;
  applyEnvelope(envelope: any): number;
  applyOperationsJson(payload: string): number;
  useBrowserStorage(key: string): boolean;
  openDurableStorage(config: any): Promise<any>;
  putRecord(key: string, value: any): void;
  putRecordBytes(key: string, bytes: Uint8Array): void;
  putRecordBlob(key: string, bytes: Uint8Array, media_type: string | null): any;
  getRecord(key: string): any;
  scanRecords(scan: any): any;
  watchRecords(scan: any, callback: Function): RecordWatchSubscription;
  createVectorCollection(name: string, config: any): void;
  putVector(collection: string, id: string, vector: any, metadata: any): void;
  deleteVector(collection: string, id: string): void;
  getVector(collection: string, id: string): any;
  searchVectors(collection: string, query: any, spec: any): any;
  watchVectorSearch(collection: string, query: any, spec: any, callback: Function): VectorWatchSubscription;
  saveVectorCacheOpfs(directory: string, namespace: string, collection: string): Promise<any>;
  loadVectorCacheOpfs(directory: string, namespace: string, collection: string): Promise<boolean>;
  applyRecordBatch(batch: any): any;
  deleteRecord(key: string): void;
  syncStorage(): any;
  storageRecoveryReport(): any;
  closeDurableStorage(): void;
  registerUser(alias: string, public_key_base64: string, roots: any): void;
  authenticateLocalUser(alias: string, secret_key_base64: string, roots: any): void;
  requireSignedSync(required: boolean): void;
  setSnapshotEncryptionKey(key_base64: string): void;
  setTransportEncryptionKey(key_base64: string): void;
  createWriteCertificate(certificants: any, write_policy: any, expires_at_millis: number | null, write_block: any): string;
  setNetworkHooks(hooks: any): void;
  clearNetworkHooks(): void;
  saveIndexedDb(database_name: string, store_name: string, key: string): Promise<void>;
  loadIndexedDb(database_name: string, store_name: string, key: string): Promise<boolean>;
  enableIndexedDbPersistence(database_name: string, store_name: string, key: string, load_existing: boolean | null): Promise<IndexedDbPersistence>;
  saveIndexedDbSegments(database_name: string, store_name: string, namespace: string): Promise<void>;
  loadIndexedDbSegments(database_name: string, store_name: string, namespace: string): Promise<boolean>;
  enableIndexedDbSegmentPersistence(database_name: string, store_name: string, namespace: string, load_existing: boolean | null): Promise<IndexedDbSegmentPersistence>;
  saveOpfsSegments(directory: string, namespace: string): Promise<void>;
  loadOpfsSegments(directory: string, namespace: string): Promise<boolean>;
  enableOpfsSegmentPersistence(directory: string, namespace: string, load_existing: boolean | null): Promise<OpfsSegmentPersistence>;
  openBlobStorage(config: any): any;
  enableIndexedDbBlobStorage(database_name: string, store_name: string, namespace: string): IndexedDbBlobStorage;
  connectWebSocket(url: string, retry_interval_ms: number | null): WebSocketSync;
  connectRelay(config: any): WebSocketSync;
  connectWebRtcMesh(room: string, retry_interval_ms: number | null, options: any | null): WebRtcMesh;
  connectWebRtcMeshViaRelay(url: string, room: string, retry_interval_ms: number | null, options: any | null): WebRtcMesh;
  connectMesh(config: any): WebRtcMesh;
  connectMeshWithExternalSignaling(config: any, send_route: Function): WebRtcMesh;
}
```

### `Chain`

```ts
class Chain {
  field(key: string): Chain;
  path(): string;
  put(value: any): void;
  putBytes(bytes: Uint8Array): void;
  putSigned(value: any, certificate: string | null): void;
  once(): any;
  onceBytes(): any;
  set(value: any): string;
  setSigned(value: any, certificate: string | null): string;
  remove(value: any): string;
  unset(): void;
  map(): any;
  query(spec: any): any;
  scan(spec: any): any;
  traverse(spec: any): any;
  firstQuery(spec: any): any;
  on(callback: Function): Subscription;
  watchTraverse(spec: any, callback: Function): TraversalSubscription;
  putBlob(data: Uint8Array, media_type: string | null): Promise<any>;
  blobRef(): any;
  getBlob(): Promise<any>;
}
```

### `Subscription`

```ts
class Subscription {
  cancel(): void;
}
```

### `TraversalSubscription`

```ts
class TraversalSubscription {
  cancel(): void;
}
```

### `RecordWatchSubscription`

```ts
class RecordWatchSubscription {
  cancel(): void;
}
```

### `VectorWatchSubscription`

```ts
class VectorWatchSubscription {
  cancel(): void;
}
```

### `RemoteWatch`

```ts
class RemoteWatch {
  next(): Promise<any>;
  tryNext(): any;
  cancel(): void;
}
```

### `ApplicationRouteSubscription`

```ts
class ApplicationRouteSubscription {
  next(): Promise<any>;
  tryNext(): any;
  drain(): any;
  close(): void;
}
```

### `RemoteFanInWatch`

```ts
class RemoteFanInWatch {
  next(): Promise<any>;
  tryNext(): any;
  drain(): any;
  close(): void;
}
```

### `Scope`

```ts
class Scope {
  root(): string;
  configure(policy: any): void;
  policy(): any;
  proposals(): any;
  transaction(steps: any, options: any): any;
}
```

### `IndexedDbPersistence`

```ts
class IndexedDbPersistence {
  flush(): Promise<void>;
  close(): void;
}
```

### `IndexedDbSegmentPersistence`

```ts
class IndexedDbSegmentPersistence {
  flush(): Promise<void>;
  stats(): any;
  estimateStorage(): Promise<any>;
  close(): void;
}
```

### `OpfsSegmentPersistence`

```ts
class OpfsSegmentPersistence {
  flush(): Promise<void>;
  stats(): any;
  estimateStorage(): Promise<any>;
  close(): void;
}
```

### `IndexedDbBlobStorage`

```ts
class IndexedDbBlobStorage {
  put(data: Uint8Array, media_type: string | null): Promise<any>;
  get(blob_id: string): Promise<any>;
  hasBlob(blob_id: string): Promise<boolean>;
}
```

### `WebSocketSync`

```ts
class WebSocketSync {
  readyState(): number;
  url(): string;
  pendingCount(): number;
  inflightCount(): number;
  recommendedPeers(): any;
  publishApplication(message: any, target: any | null): any;
  sendApplication(namespace: string, protocol: string, topic: string | null, body: any, metadata: any | null, target: any | null): any;
  subscribeApplications(filter: any | null): ApplicationRouteSubscription;
  get(path: any, policy: any | null): Promise<any>;
  query(path: any, spec: any, policy: any | null): Promise<any>;
  lex(path: any, spec: any, policy: any | null): Promise<any>;
  records(scan: any, policy: any | null): Promise<any>;
  recordsFanIn(scan: any, policy: any | null): Promise<any>;
  vectorSearch(collection: string, query: any, spec: any, policy: any | null): Promise<any>;
  node(id: string, policy: any | null): Promise<any>;
  snapshot(root: string | null, policy: any | null): Promise<any>;
  watchGet(path: any, policy: any | null): RemoteWatch;
  watchMap(path: any, policy: any | null): RemoteWatch;
  watchQuery(path: any, spec: any, policy: any | null): RemoteWatch;
  watchLex(path: any, spec: any, policy: any | null): RemoteWatch;
  watchRecords(scan: any, policy: any | null): RemoteWatch;
  watchRecordsFanIn(scan: any, policy: any | null): RemoteFanInWatch;
  watchVectorSearch(collection: string, query: any, spec: any, policy: any | null): RemoteWatch;
  watchNode(id: string, policy: any | null): RemoteWatch;
  watchSnapshot(root: string | null, policy: any | null): RemoteWatch;
  watchRemoteGet(peer_id: string, path: any): RemoteWatch;
  watchRemoteMap(peer_id: string, path: any): RemoteWatch;
  watchRemoteQuery(peer_id: string, path: any, spec: any): RemoteWatch;
  watchRemoteLex(peer_id: string, path: any, spec: any): RemoteWatch;
  watchRemoteRecords(peer_id: string, scan: any): RemoteWatch;
  watchRemoteVectorSearch(peer_id: string, collection: string, query: any, spec: any): RemoteWatch;
  watchRemoteNode(peer_id: string, id: string): RemoteWatch;
  watchRemoteSnapshot(peer_id: string, root: string | null): RemoteWatch;
  remoteGet(peer_id: string, path: any): Promise<any>;
  remoteQuery(peer_id: string, path: any, spec: any): Promise<any>;
  remoteLex(peer_id: string, path: any, spec: any): Promise<any>;
  remoteRecords(peer_id: string, scan: any): Promise<any>;
  remoteVectorSearch(peer_id: string, collection: string, query: any, spec: any): Promise<any>;
  remoteNode(peer_id: string, id: string): Promise<any>;
  remoteSnapshot(peer_id: string, root: string | null): Promise<any>;
  remoteTransaction(peer_id: string, scope: string, steps: any, options: any): Promise<any>;
  flushPending(): number;
  retryInflight(): number;
  close(): void;
}
```

### `WebRtcMesh`

```ts
class WebRtcMesh {
  peerId(): string;
  signalingMode(): string;
  relayUrl(): string | null;
  signalingReadyState(): number | null;
  acceptSignalingRoute(route: any): void;
  announceSignalingPresence(): void;
  peerCount(): number;
  openPeerCount(): number;
  inflightCount(): number;
  publishApplication(message: any, target: any | null): any;
  sendApplication(namespace: string, protocol: string, topic: string | null, body: any, metadata: any | null, target: any | null): any;
  subscribeApplications(filter: any | null): ApplicationRouteSubscription;
  recordsFanIn(scan: any, policy: any | null): Promise<any>;
  watchGet(path: any, policy: any | null): RemoteWatch;
  watchMap(path: any, policy: any | null): RemoteWatch;
  watchQuery(path: any, spec: any, policy: any | null): RemoteWatch;
  watchLex(path: any, spec: any, policy: any | null): RemoteWatch;
  watchRecords(scan: any, policy: any | null): RemoteWatch;
  watchRecordsFanIn(scan: any, policy: any | null): RemoteFanInWatch;
  watchVectorSearch(collection: string, query: any, spec: any, policy: any | null): RemoteWatch;
  watchNode(id: string, policy: any | null): RemoteWatch;
  watchSnapshot(root: string | null, policy: any | null): RemoteWatch;
  watchRemoteGet(peer_id: string, path: any): RemoteWatch;
  watchRemoteMap(peer_id: string, path: any): RemoteWatch;
  watchRemoteQuery(peer_id: string, path: any, spec: any): RemoteWatch;
  watchRemoteLex(peer_id: string, path: any, spec: any): RemoteWatch;
  watchRemoteRecords(peer_id: string, scan: any): RemoteWatch;
  watchRemoteVectorSearch(peer_id: string, collection: string, query: any, spec: any): RemoteWatch;
  watchRemoteNode(peer_id: string, id: string): RemoteWatch;
  watchRemoteSnapshot(peer_id: string, root: string | null): RemoteWatch;
  flushPending(): number;
  retryInflight(): number;
  close(): void;
}
```

## Browser segment persistence

`enableIndexedDbSegmentPersistence(...)` and `enableOpfsSegmentPersistence(...)` both perform an initial full flush, then auto-persist later data changes as incremental segment transactions.

Segment persistence stores current graph state and storage transaction bookkeeping. It intentionally omits the transport pending-op queue, so high-churn opaque values are not duplicated into durable metadata on every save.

Use OPFS segments for large or high-churn browser-local datasets when `navigator.storage.getDirectory()` is available. IndexedDB segments remain the compatibility path.

`stats()` reports queued/coalesced events, successful and failed writes, full replacements, incremental transactions, entries written/deleted, estimated bytes written, and the last write error. `estimateStorage()` reports logical namespace size; OPFS also includes origin quota/usage when the browser exposes it.

## Keyed records

`putRecord(...)`, `putRecordBytes(...)`, `putRecordBlob(...)`, `getRecord(...)`, `scanRecords(...)`, `watchRecords(...)`, `applyRecordBatch(...)`, and `deleteRecord(...)` expose graph-native ordered records in the browser runtime.

`records(...)`, `remoteRecords(...)`, `watchRecords(...)`, and `watchRemoteRecords(...)` use the same record-scan request shape as local record watches, so relay and mesh transports do not define separate record semantics.

Records persist through IndexedDB/OPFS segment persistence and use the same graph transaction, watch, sync, and blob paths as normal graph writes.

## Remote interest selection

Relay `WebSocketSync` exposes peer-agnostic pulls such as `get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)`. Relay and mesh handles expose peer-agnostic watches such as `watchQuery(...)` and `watchRecords(...)`.

The default policy selects any connected/recommended peer. Pass a `RemoteInterestPolicy` object only when a caller needs to pin or constrain selection; explicit `remote*` / `watchRemote*` methods remain available for direct peer targeting.

## Application routes

Application route APIs carry caller-defined messages inside `RoutePayload::Application` / `{ kind: "application" }` while preserving the surrounding `RouteEnvelope` metadata.

Use `publishApplication(...)` / `publish_application(...)` when the caller has already assembled an application message, or `sendApplication(...)` / `send_application(...)` for the namespace/protocol/topic/body convenience shape.

`subscribeApplications(...)` / `subscribe_applications(...)` returns a filtered subscription with deterministic `next`/`tryNext`/`drain`/`close` behavior. Received events include route id, source peer, channel, target, receive time, transport kind where available, and the application message.

These APIs are RouteEnvelope-level. They do not expose raw WebSocket, WebRTC, WebTransport, or MoQ socket handles.

## Record fan-in

`recordsFanIn(...)` / `records_fan_in(...)` sends a record scan to every currently reachable peer that matches the supplied `RemoteInterestPolicy` instead of selecting one ambient peer.

`watchRecordsFanIn(...)` / `watch_records_fan_in(...)` keeps child watches open across all matching peers and emits source-tagged updates plus partial failures. Closing the returned watch cancels all child watches.

Fan-in results include per-peer records, a deterministic merged result, conflict metadata, and partial failure diagnostics. Per-peer source metadata is preserved so callers can apply their own trust or dedupe policy above the built-in deterministic merge.

## Strict consistency and transactions

PrimaDB is eventual/local-first by default. Strict consistency APIs are opt-in and scoped to a graph root.

- `db.transaction(...)` applies a step array atomically on the local replica.
- `db.scope(root).configure(...)` stores a scope policy for that root.
- `scope.transaction(...)` runs a step array inside the scope and prefixes relative step paths with the scope root.
- `consistency: "local_transactional"` marks the scope as a transaction boundary without adding network coordination.
- `consistency: "coordinated"` requires the configured authority for canonical writes.
- Non-authority peers use `offlineWrites: "reject"` to fail immediately or `offlineWrites: "queue_provisional"` to store a durable local proposal that normal reads and watches do not treat as committed graph state.
- Relay sync clients expose `remoteTransaction(...)` to submit a coordinated transaction to an authority peer.

The current coordinated implementation is a single-authority path. Quorum policies and strict authority read modes are represented in the policy model but are not full consensus or distributed multi-scope transactions yet.

## Traversal semantics

`Chain.traverse(...)` returns the current local traversal result immediately. When connected relay or mesh transports are active, missing linked nodes are scheduled for bounded background fetch.

`Chain.watchTraverse(...)` is the preferred API for peer-assisted traversal because it emits updated traversal results as fetched nodes merge into the local graph.

`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation, not a blocking network completion count.
