---
title: Browser TypeScript Package API
sidebar_position: 2
---

This page covers the public `primadb` browser package entrypoint, hook helpers, and MoQ helpers. The re-exported runtime classes and transport bindings are documented on the browser runtime API page.

> This page is generated from the current package source declarations.

## `packages/primadb/index.ts`

Primary `primadb` entrypoint.

### Direct exports

#### `PrimadbInitInput`

Kind: type alias

```ts
export type PrimadbInitInput = Parameters<typeof initWasm>[0];
```

#### `PrimadbInitOutput`

Kind: type alias

```ts
export type PrimadbInitOutput = Awaited<ReturnType<typeof initWasm>>;
```

#### `initPrimadb`

Kind: function

```ts
export declare function initPrimadb(input?: PrimadbInitInput): Promise<PrimadbInitOutput>;
```

#### `createPrimadb`

Kind: function

```ts
export declare function createPrimadb(replicaId?: string | null, input?: PrimadbInitInput): Promise<Primadb>;
```

### Re-exports

```ts
export * from "./vendor/default/primadb.js";
```

```ts
export * from "./hooks.js";
```

```ts
export * from "./types.js";
```

```ts
export { initWasm };
```

```ts
export default initPrimadb;
```

## `packages/primadb/moq.ts`

Experimental `primadb/moq` helper entrypoint.

### Direct exports

#### `PrimadbLike`

Kind: interface

```ts
export interface PrimadbLike {
    replicaId(): string;
    pendingEnvelope(): unknown;
    drainPendingEnvelope(): unknown;
    drainPendingEnvelopeJson?(): string;
    drainPendingOperationsJson?(): string;
    applyEnvelope(envelope: unknown): number;
    applyOperationsJson?(payload: string): number;
}
```

#### `PrimadbMoqSyncPayload`

Kind: interface

```ts
export interface PrimadbMoqSyncPayload {
    type: "primadb.sync.v1";
    from: string;
    sentAt: number;
    envelope?: unknown;
    envelopeJson?: string;
}
```

#### `PrimadbRouteTarget`

Kind: type alias

```ts
export type PrimadbRouteTarget = {
    kind: "broadcast";
} | {
    kind: "peer";
    value: string;
} | {
    kind: "topic";
    value: string;
};
```

#### `PrimadbRouteTransportKind`

Kind: type alias

```ts
export type PrimadbRouteTransportKind = "web_socket" | "moq" | "web_rtc" | "broadcast_channel" | "in_memory";
```

#### `PrimadbRoutePayload`

Kind: type alias

```ts
export type PrimadbRoutePayload = {
    kind: "application";
    message: PrimadbApplicationRouteMessage;
} | {
    kind: "sync";
    encoding: string;
    payload: unknown;
} | {
    kind: "presence";
    peer: PrimadbPeerPresence;
} | {
    kind: "peer_exchange";
    peers: PrimadbPeerRecommendation[];
} | {
    kind: "batch";
    items: PrimadbRouteBatchItem[];
} | {
    kind: string;
    [key: string]: unknown;
};
```

#### `PrimadbRouteBatchItem`

Kind: type alias

```ts
export type PrimadbRouteBatchItem = {
    kind: "application";
    message: PrimadbApplicationRouteMessage;
} | {
    kind: "sync";
    encoding: string;
    payload: unknown;
} | {
    kind: "presence";
    peer: PrimadbPeerPresence;
} | {
    kind: "peer_exchange";
    peers: PrimadbPeerRecommendation[];
} | {
    kind: string;
    [key: string]: unknown;
};
```

#### `PrimadbPeerPresence`

Kind: interface

```ts
export interface PrimadbPeerPresence {
    peer_id: string;
    replica_id: string;
    transport: string;
    identity?: unknown;
    capabilities?: string[];
    topics?: string[];
    metadata?: Record<string, string>;
}
```

#### `PrimadbPeerRecommendation`

Kind: interface

```ts
export interface PrimadbPeerRecommendation {
    peer: PrimadbPeerPresence;
    relay_urls?: string[];
    score?: number;
    discovered_at_millis?: number;
}
```

#### `PrimadbRouteEnvelope`

Kind: interface

```ts
export interface PrimadbRouteEnvelope {
    route_id: string;
    from: string;
    channel: string;
    target: PrimadbRouteTarget;
    ttl: number;
    hops: number;
    issued_at_millis: number;
    reply_to?: string | null;
    content_hash?: string | null;
    seen_by: string[];
    payload: PrimadbRoutePayload;
}
```

#### `PrimadbMoqRoutePayload`

Kind: interface

```ts
export interface PrimadbMoqRoutePayload {
    type: "primadb.route.v1";
    from: string;
    sentAt: number;
    route: PrimadbRouteEnvelope;
}
```

#### `PrimadbRouteHandler`

Kind: type alias

```ts
export type PrimadbRouteHandler = (route: PrimadbRouteEnvelope) => void;
```

#### `PrimadbApplicationRouteMessage`

Kind: interface

```ts
export interface PrimadbApplicationRouteMessage {
    namespace: string;
    protocol: string;
    topic?: string | null;
    body: unknown;
    metadata?: Record<string, unknown>;
}
```

#### `PrimadbApplicationRouteAuthStatus`

Kind: type alias

```ts
export type PrimadbApplicationRouteAuthStatus = "unknown" | "not_required" | "unauthenticated" | "authenticated" | "required_but_missing";
```

#### `PrimadbApplicationRouteContext`

Kind: interface

```ts
export interface PrimadbApplicationRouteContext {
    sourcePeerId: string;
    transport: PrimadbRouteTransportKind;
    underlayId?: string | null;
    direct: boolean;
    relayRouted: boolean;
    gatewayRouted: boolean;
    gatewayPeerId?: string | null;
    authStatus: PrimadbApplicationRouteAuthStatus;
    provenance: string[];
}
```

#### `PrimadbApplicationRouteEvent`

Kind: interface

```ts
export interface PrimadbApplicationRouteEvent {
    routeId: string;
    from: string;
    channel: string;
    target: PrimadbRouteTarget;
    issuedAtMillis: number;
    receivedAtMillis: number;
    transport: PrimadbRouteTransportKind;
    verifiedIdentity: null;
    context: PrimadbApplicationRouteContext;
    message: PrimadbApplicationRouteMessage;
}
```

#### `PrimadbApplicationRouteFilter`

Kind: interface

```ts
export interface PrimadbApplicationRouteFilter {
    namespace?: string | null;
    protocol?: string | null;
    topic?: string | null;
}
```

#### `PrimadbRouteOverlaySendMode`

Kind: type alias

```ts
export type PrimadbRouteOverlaySendMode = "first_success" | "fan_out";
```

#### `PrimadbRouteOverlayPolicy`

Kind: interface

```ts
export interface PrimadbRouteOverlayPolicy {
    preferredTransports?: PrimadbRouteTransportKind[];
    sendMode?: PrimadbRouteOverlaySendMode;
    directFirst?: boolean;
    allowDirect?: boolean;
    allowRelay?: boolean;
    requireDirect?: boolean;
}
```

#### `PrimadbRouteOverlayUnderlayInfo`

Kind: interface

```ts
export interface PrimadbRouteOverlayUnderlayInfo {
    id: string;
    transport: PrimadbRouteTransportKind;
    direct?: boolean;
    relayRouted?: boolean;
    connected?: boolean;
    priority?: number;
    metadata?: Record<string, string>;
}
```

#### `PrimadbRouteOverlayUnderlay`

Kind: interface

```ts
export interface PrimadbRouteOverlayUnderlay {
    info(): PrimadbRouteOverlayUnderlayInfo;
    sendRoute(route: PrimadbRouteEnvelope): number | Promise<number>;
    drainRoutes?(): PrimadbRouteEnvelope[];
    close?(): void;
}
```

#### `PrimadbRouteOverlayDeliveryAttempt`

Kind: interface

```ts
export interface PrimadbRouteOverlayDeliveryAttempt {
    underlay: PrimadbRouteOverlayUnderlayInfo;
    attemptedAtMillis: number;
    success: boolean;
    message?: string | null;
}
```

#### `PrimadbRouteOverlaySendReport`

Kind: interface

```ts
export interface PrimadbRouteOverlaySendReport {
    route: PrimadbRouteEnvelope;
    attempts: PrimadbRouteOverlayDeliveryAttempt[];
    deliveredUnderlayIds: string[];
    failedUnderlayIds: string[];
    deliveredPeerIds: string[];
    fallbackReason?: string | null;
    duplicateSuppressed: number;
}
```

#### `PrimadbRouteOverlayPumpReport`

Kind: interface

```ts
export interface PrimadbRouteOverlayPumpReport {
    receivedRoutes: number;
    deliveredApplicationRoutes: number;
    deliveredStreamEvents: number;
    duplicateSuppressed: number;
    underlayIds: string[];
}
```

#### `PrimadbApplicationStreamFrameKind`

Kind: type alias

```ts
export type PrimadbApplicationStreamFrameKind = "open" | "data" | "ack" | "nack" | "close" | "error";
```

#### `PrimadbApplicationStreamFrame`

Kind: interface

```ts
export interface PrimadbApplicationStreamFrame {
    streamId: string;
    sequence: number;
    kind: PrimadbApplicationStreamFrameKind;
    namespace: string;
    protocol: string;
    topic?: string | null;
    chunk?: string | null;
    finalChunk?: boolean;
    ackSequence?: number | null;
    error?: string | null;
    metadata?: Record<string, unknown>;
}
```

#### `PrimadbApplicationStreamEvent`

Kind: interface

```ts
export interface PrimadbApplicationStreamEvent {
    streamId: string;
    from: string;
    transport: PrimadbRouteTransportKind;
    namespace: string;
    protocol: string;
    topic?: string | null;
    body: unknown;
    metadata: Record<string, unknown>;
}
```

#### `PrimadbApplicationStreamSendOptions`

Kind: interface

```ts
export interface PrimadbApplicationStreamSendOptions {
    namespace: string;
    protocol: string;
    topic?: string | null;
    body: unknown;
    metadata?: Record<string, unknown>;
    target?: PrimadbRouteTarget;
    maxChunkChars?: number;
}
```

#### `PrimadbApplicationStreamSendReport`

Kind: interface

```ts
export interface PrimadbApplicationStreamSendReport {
    streamId: string;
    frameReports: PrimadbRouteOverlaySendReport[];
}
```

#### `PrimadbMoqSessionOptions`

Kind: interface

```ts
export interface PrimadbMoqSessionOptions {
    path: string;
    track?: string;
    channel?: string;
    peerId?: string;
    target?: PrimadbRouteTarget;
    intervalMs?: number;
    retryIntervalMs?: number;
    publish?: boolean;
    subscribe?: string[];
    closeConnection?: boolean;
}
```

#### `ConnectPrimadbMoqOptions`

Kind: interface

```ts
export interface ConnectPrimadbMoqOptions extends PrimadbMoqSessionOptions {
    url: string | URL;
    websocketUrl?: string | URL;
    websocket?: boolean;
    webtransport?: WebTransportOptions;
    transport?: WebTransport;
}
```

#### `PrimadbExternalMesh`

Kind: interface

```ts
export interface PrimadbExternalMesh {
    peerId(): string;
    signalingMode(): string;
    relayUrl(): string | undefined;
    signalingReadyState(): number | undefined;
    peerCount(): number;
    openPeerCount(): number;
    acceptSignalingRoute(route: PrimadbRouteEnvelope): void;
    announceSignalingPresence(): void;
    close(): void;
}
```

#### `PrimadbMeshLike`

Kind: interface

```ts
export interface PrimadbMeshLike extends PrimadbLike {
    connectMeshWithExternalSignaling(config: PrimadbExternalMeshConfig, sendRoute: (route: PrimadbRouteEnvelope) => unknown): PrimadbExternalMesh;
}
```

#### `PrimadbMoqRelayEndpointConfig`

Kind: interface

```ts
export interface PrimadbMoqRelayEndpointConfig {
    kind: "moq";
    url: string;
    path: string;
    track?: string;
    channel?: string;
    subscribe?: string[];
    draft?: "draft_07" | "draft_14" | "draft_latest";
    retryIntervalMs?: number;
    tlsDisableVerify?: boolean;
    sessionAuth?: unknown;
}
```

#### `PrimadbExternalMeshConfig`

Kind: interface

```ts
export interface PrimadbExternalMeshConfig {
    room: string;
    signaling?: "relay" | "broadcast_channel";
    relayEndpoint?: PrimadbMoqRelayEndpointConfig;
    retryIntervalMs?: number;
    iceServers?: unknown[];
    sessionAuth?: unknown;
    [key: string]: unknown;
}
```

#### `ConnectPrimadbMeshViaMoqOptions`

Kind: interface

```ts
export interface ConnectPrimadbMeshViaMoqOptions extends ConnectPrimadbMoqOptions {
    room: string;
    retryIntervalMs?: number;
    iceServers?: unknown[];
    sessionAuth?: unknown;
    draft?: "draft_07" | "draft_14" | "draft_latest";
    meshConfig?: Record<string, unknown>;
}
```

#### `ConnectPrimadbMeshViaMoqSessionOptions`

Kind: interface

```ts
export interface ConnectPrimadbMeshViaMoqSessionOptions {
    room: string;
    url?: string | URL;
    path?: string;
    track?: string;
    channel?: string;
    subscribe?: string[];
    retryIntervalMs?: number;
    intervalMs?: number;
    iceServers?: unknown[];
    sessionAuth?: unknown;
    draft?: "draft_07" | "draft_14" | "draft_latest";
    meshConfig?: Record<string, unknown>;
    closeMoqSession?: boolean;
}
```

#### `PrimadbMoqMesh`

Kind: interface

```ts
export interface PrimadbMoqMesh {
    mesh: PrimadbExternalMesh;
    moq: PrimadbMoqSession;
    close(): void;
}
```

#### `PrimadbMoqLoopbackOptions`

Kind: interface

```ts
export interface PrimadbMoqLoopbackOptions {
    publisherDb: PrimadbLike;
    subscriberDb: PrimadbLike;
    path: string;
    track?: string;
    channel?: string;
    intervalMs?: number;
    url?: string | URL;
    protocol?: string;
}
```

#### `PrimadbMoqLoopback`

Kind: interface

```ts
export interface PrimadbMoqLoopback {
    publisher: PrimadbMoqSession;
    subscriber: PrimadbMoqSession;
    flush(): Promise<number>;
    close(): void;
}
```

#### `moqRuntimeSupport`

Kind: function

```ts
export declare function moqRuntimeSupport(): {
  webTransport: boolean;
  webSocket: boolean;
  websocketFallback: boolean;
};
```

#### `connectPrimadbMoq`

Kind: function

```ts
export declare function connectPrimadbMoq(db: PrimadbLike, options: ConnectPrimadbMoqOptions): Promise<PrimadbMoqSession>;
```

#### `connectMeshViaMoq`

Kind: function

```ts
export declare function connectMeshViaMoq(db: PrimadbMeshLike, options: ConnectPrimadbMeshViaMoqOptions): Promise<PrimadbMoqMesh>;
```

#### `connectMeshViaMoqSession`

Kind: function

```ts
export declare function connectMeshViaMoqSession(db: PrimadbMeshLike, moq: PrimadbMoqSession, options: ConnectPrimadbMeshViaMoqSessionOptions): PrimadbMoqMesh;
```

#### `PRIMADB_APPLICATION_STREAM_NAMESPACE`

Kind: variable

```ts
export const PRIMADB_APPLICATION_STREAM_NAMESPACE = "primadb.applicationStream";
```

#### `PRIMADB_APPLICATION_STREAM_PROTOCOL_V1`

Kind: variable

```ts
export const PRIMADB_APPLICATION_STREAM_PROTOCOL_V1 = "primadb.applicationStream.v1";
```

#### `PrimadbApplicationRouteSubscription`

Kind: class

```ts
export declare class PrimadbApplicationRouteSubscription {
    readonly filter: PrimadbApplicationRouteFilter;
    constructor(filter: PrimadbApplicationRouteFilter, onClose: () => void);
    next(): Promise<PrimadbApplicationRouteEvent | null>;
    tryNext(): PrimadbApplicationRouteEvent | null;
    drain(): PrimadbApplicationRouteEvent[];
    close(): void;
    enqueue(event: PrimadbApplicationRouteEvent): void;
}
```

#### `PrimadbRouteOverlaySession`

Kind: class

```ts
export declare class PrimadbRouteOverlaySession {
    readonly peerId: string;
    readonly channel: string;
    readonly ttl: number;
    constructor(options: {
    peerId: string;
    channel?: string;
    ttl?: number;
    policy?: PrimadbRouteOverlayPolicy;
  });
    policy(): Required<PrimadbRouteOverlayPolicy>;
    setPolicy(policy: PrimadbRouteOverlayPolicy): void;
    addUnderlay(underlay: PrimadbRouteOverlayUnderlay): void;
    removeUnderlay(id: string): PrimadbRouteOverlayUnderlayInfo | null;
    underlays(): PrimadbRouteOverlayUnderlayInfo[];
    createRoute(payload: PrimadbRoutePayload, target?: PrimadbRouteTarget, replyTo?: string | null): PrimadbRouteEnvelope;
    publishApplication(message: PrimadbApplicationRouteMessage, target?: PrimadbRouteTarget): Promise<PrimadbRouteOverlaySendReport>;
    sendApplication(namespace: string, protocol: string, topic: string | null | undefined, body: unknown, metadata?: Record<string, unknown>, target?: PrimadbRouteTarget): Promise<PrimadbRouteOverlaySendReport>;
    sendRoute(route: PrimadbRouteEnvelope): Promise<PrimadbRouteOverlaySendReport>;
    subscribeApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteSubscription;
    nextApplication(filter?: PrimadbApplicationRouteFilter): Promise<PrimadbApplicationRouteEvent | null>;
    tryNextApplication(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent | null;
    drainApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent[];
    pump(): PrimadbRouteOverlayPumpReport;
    sendApplicationStream(options: PrimadbApplicationStreamSendOptions): Promise<PrimadbApplicationStreamSendReport>;
    drainStreamEvents(): PrimadbApplicationStreamEvent[];
    close(): void;
}
```

#### `primadbMoqOverlayUnderlay`

Kind: function

```ts
export declare function primadbMoqOverlayUnderlay(id: string, session: PrimadbMoqSession, options?: { priority?: number; maxQueue?: number; metadata?: Record<string, string> }): PrimadbRouteOverlayUnderlay;
```

#### `PrimadbMoqSession`

Kind: class

```ts
export declare class PrimadbMoqSession {
    readonly db: PrimadbLike;
    readonly connection: MoqConnection;
    readonly path: string;
    readonly track: string;
    readonly channel: string;
    readonly peerId: string;
    readonly intervalMs: number;
    readonly retryIntervalMs: number;
    constructor(db: PrimadbLike, connection: MoqConnection, options: PrimadbMoqSessionOptions);
    publish(): MoqBroadcast;
    subscribe(path?: string): string;
    startAutoFlush(): void;
    onRoute(handler: PrimadbRouteHandler): () => void;
    publishApplication(message: PrimadbApplicationRouteMessage, target?: PrimadbRouteTarget): number;
    sendApplication(namespace: string, protocol: string, topic: string | null | undefined, body: unknown, metadata?: Record<string, unknown>, target?: PrimadbRouteTarget): number;
    subscribeApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteSubscription;
    nextApplication(filter?: PrimadbApplicationRouteFilter): Promise<PrimadbApplicationRouteEvent | null>;
    tryNextApplication(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent | null;
    drainApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent[];
    addAcceptedPeerId(peerId: string): () => void;
    knownPeers(): PrimadbPeerPresence[];
    recommendedPeers(): PrimadbPeerRecommendation[];
    createRoute(payload: PrimadbRoutePayload, target?: PrimadbRouteTarget, replyTo?: string | null): PrimadbRouteEnvelope;
    sendRoute(route: PrimadbRouteEnvelope): number;
    announcePresence(): number;
    flushPending(): Promise<number>;
    close(): void;
}
```

#### `createPrimadbMoqLoopback`

Kind: function

```ts
export declare function createPrimadbMoqLoopback(options: PrimadbMoqLoopbackOptions): Promise<PrimadbMoqLoopback>;
```

## `packages/primadb/types.ts`

Shared browser storage, blob, and keyed-record TypeScript helper types.

### Direct exports

#### `JsonPrimitive`

Kind: type alias

```ts
export type JsonPrimitive = string | number | boolean | null;
```

#### `JsonValue`

Kind: type alias

```ts
export type JsonValue = JsonPrimitive | JsonValue[] | {
    [key: string]: JsonValue;
};
```

#### `SegmentDurability`

Kind: type alias

```ts
export type SegmentDurability = "full" | "data" | "relaxed";
```

#### `SegmentLockMode`

Kind: type alias

```ts
export type SegmentLockMode = {
    kind: "exclusive";
} | {
    kind: "wait";
    timeoutMillis: number;
} | {
    kind: "disabled";
};
```

#### `DurableStorageConfig`

Kind: type alias

```ts
export type DurableStorageConfig = {
    kind: "indexed_db_snapshots";
    databaseName: string;
    storeName: string;
    key: string;
    loadExisting?: boolean;
    autoPersist?: boolean;
} | {
    kind: "indexed_db_segments";
    databaseName: string;
    storeName: string;
    namespace: string;
    loadExisting?: boolean;
    autoPersist?: boolean;
} | {
    kind: "opfs_segments";
    directory: string;
    namespace: string;
    loadExisting?: boolean;
    autoPersist?: boolean;
};
```

#### `DurableStorageBinding`

Kind: interface

```ts
export interface DurableStorageBinding {
    backend: string;
    incremental: boolean;
    loadedExisting: boolean;
    autoPersist: boolean;
    durability?: SegmentDurability;
    lockMode?: SegmentLockMode;
}
```

#### `BlobStorageConfig`

Kind: type alias

```ts
export type BlobStorageConfig = {
    kind: "memory";
} | {
    kind: "indexed_db";
    databaseName: string;
    storeName: string;
    namespace: string;
};
```

#### `BlobStorageBinding`

Kind: interface

```ts
export interface BlobStorageBinding {
    backend: string;
    contentAddressed: boolean;
    durability?: SegmentDurability;
}
```

#### `BlobRef`

Kind: interface

```ts
export interface BlobRef {
    id: string;
    bytes: number;
    mediaType?: string | null;
}
```

#### `RecordValue`

Kind: type alias

```ts
export type RecordValue = {
    kind: "json";
    value: JsonValue;
} | {
    kind: "bytes";
    value: string;
} | {
    kind: "blob";
    value: BlobRef;
};
```

#### `RecordEntry`

Kind: interface

```ts
export interface RecordEntry {
    key: string;
    value: RecordValue;
}
```

#### `RecordScan`

Kind: interface

```ts
export interface RecordScan {
    prefix?: string | null;
    startAt?: string | null;
    startAfter?: string | null;
    endAt?: string | null;
    endBefore?: string | null;
    reverse?: boolean;
    limit?: number | null;
    cursor?: string | null;
}
```

#### `RecordScanResult`

Kind: interface

```ts
export interface RecordScanResult {
    entries: RecordEntry[];
    nextCursor?: string | null;
}
```

#### `RemoteInterestTarget`

Kind: type alias

```ts
export type RemoteInterestTarget = "any" | "peer" | "peers";
```

#### `RemoteInterestPolicy`

Kind: interface

```ts
export interface RemoteInterestPolicy {
    target?: RemoteInterestTarget;
    peerId?: string | null;
    peers?: string[];
    requireCapability?: boolean;
}
```

#### `RecordMutation`

Kind: type alias

```ts
export type RecordMutation = {
    kind: "put";
    key: string;
    value: RecordValue;
} | {
    kind: "delete";
    key: string;
} | {
    kind: "delete_range";
    scan: RecordScan;
};
```

#### `RecordPrecondition`

Kind: type alias

```ts
export type RecordPrecondition = {
    kind: "exists";
    key: string;
} | {
    kind: "absent";
    key: string;
} | {
    kind: "value";
    key: string;
    value: RecordValue;
};
```

#### `RecordBatch`

Kind: interface

```ts
export interface RecordBatch {
    preconditions?: RecordPrecondition[];
    mutations?: RecordMutation[];
}
```

#### `RecordBatchReport`

Kind: interface

```ts
export interface RecordBatchReport {
    preconditions: number;
    puts: number;
    deletes: number;
    rangeDeletes: number;
    operationCount: number;
}
```

#### `VectorMetric`

Kind: type alias

```ts
export type VectorMetric = "cosine" | "l2" | "dot";
```

#### `VectorBackendKind`

Kind: type alias

```ts
export type VectorBackendKind = "exact" | "edgevec";
```

#### `VectorManagerState`

Kind: type alias

```ts
export type VectorManagerState = "ready" | "catching_up" | "rebuilding" | "stale" | "failed";
```

#### `VectorStalePolicy`

Kind: type alias

```ts
export type VectorStalePolicy = "fallback_exact" | "allow_stale" | "error";
```

#### `VectorHnswConfig`

Kind: interface

```ts
export interface VectorHnswConfig {
    m?: number | null;
    efConstruction?: number | null;
    efSearch?: number | null;
    tombstoneRebuildRatio?: number | null;
}
```

#### `VectorChunkingConfig`

Kind: interface

```ts
export interface VectorChunkingConfig {
    chunkBytes: number;
}
```

#### `VectorCollectionConfig`

Kind: interface

```ts
export interface VectorCollectionConfig {
    dim: number;
    metric?: VectorMetric;
    backend?: VectorBackendKind | null;
    hnsw?: VectorHnswConfig | null;
    chunking?: VectorChunkingConfig;
}
```

#### `VectorEntry`

Kind: interface

```ts
export interface VectorEntry {
    id: string;
    vector: number[];
    metadata?: JsonValue | null;
    writeId: string;
    checksum: string;
}
```

#### `VectorMetadataFilter`

Kind: interface

```ts
export interface VectorMetadataFilter {
    eq?: Record<string, JsonValue>;
    prefix?: Record<string, string>;
    exists?: string[];
}
```

#### `VectorFilter`

Kind: interface

```ts
export interface VectorFilter {
    idPrefix?: string | null;
    ids?: string[];
    metadata?: VectorMetadataFilter | null;
}
```

#### `VectorSearchSpec`

Kind: interface

```ts
export interface VectorSearchSpec {
    limit: number;
    ef?: number | null;
    filter?: VectorFilter | null;
    includeVector?: boolean;
    includeMetadata?: boolean;
    exact?: boolean;
    stalePolicy?: VectorStalePolicy;
}
```

#### `VectorMatch`

Kind: interface

```ts
export interface VectorMatch {
    id: string;
    distance: number;
    metadata?: JsonValue | null;
    vector?: number[] | null;
}
```

#### `VectorSearchResult`

Kind: interface

```ts
export interface VectorSearchResult {
    matches: VectorMatch[];
    exact: boolean;
    backend: VectorBackendKind;
    state: VectorManagerState;
    stale: boolean;
    approximateReason?: string | null;
}
```

#### `StorageSyncReport`

Kind: interface

```ts
export interface StorageSyncReport {
    backend: string;
    durability: string;
    synced: boolean;
}
```

#### `StorageRecoveryReport`

Kind: interface

```ts
export interface StorageRecoveryReport {
    appliedTransactions: number;
    skippedTransactions: number;
    removedPendingFiles: number;
    removedTempFiles: number;
    quarantinedFiles: number;
}
```

## `packages/primadb/hooks.ts`

Package-level hook helper types and registration utilities.

### Direct exports

#### `PresenceIdentity`

Kind: interface

```ts
export interface PresenceIdentity {
    publicKey: string;
    alias?: string | null;
    keyScheme?: string;
    sessionId: string;
    claims?: Record<string, string>;
    issuedAtMillis?: number;
    expiresAtMillis?: number | null;
}
```

#### `IdentityTrust`

Kind: type alias

```ts
export type IdentityTrust = "verified" | "trusted_public_key" | "trusted_alias";
```

#### `VerifiedIdentity`

Kind: interface

```ts
export interface VerifiedIdentity {
    publicKey: string;
    alias?: string | null;
    peerId: string;
    replicaId: string;
    transport: string;
    sessionId: string;
    claims?: Record<string, string>;
    issuedAtMillis: number;
    expiresAtMillis?: number | null;
    trust: IdentityTrust;
}
```

#### `ConnectHookContext`

Kind: interface

```ts
export interface ConnectHookContext {
    peer: {
        peerId: string;
        replicaId: string;
        transport: string;
        identity?: PresenceIdentity | null;
        capabilities?: string[];
        topics?: string[];
        metadata?: Record<string, string>;
    };
    transport: "relay" | "mesh";
    relayUrl?: string | null;
    verifiedIdentity?: VerifiedIdentity | null;
}
```

#### `RoomHookContext`

Kind: interface

```ts
export interface RoomHookContext {
    peerId: string;
    room: string;
    transport: "relay" | "mesh";
    peer?: ConnectHookContext["peer"] | null;
    verifiedIdentity?: VerifiedIdentity | null;
}
```

#### `PullRequestKind`

Kind: type alias

```ts
export type PullRequestKind = {
    kind: "get";
    path: {
        anchor: string;
        segments?: string[];
    };
} | {
    kind: "map";
    path: {
        anchor: string;
        segments?: string[];
    };
} | {
    kind: "query";
    path: {
        anchor: string;
        segments?: string[];
    };
    spec: Record<string, unknown>;
} | {
    kind: "lex";
    path: {
        anchor: string;
        segments?: string[];
    };
    spec: Record<string, unknown>;
} | {
    kind: "records";
    scan: RecordScan;
} | {
    kind: "vector_search";
    collection: string;
    query: number[];
    spec: VectorSearchSpec;
} | {
    kind: "node";
    id: string;
} | {
    kind: "snapshot";
    root?: string | null;
};
```

#### `RemoteResult`

Kind: type alias

```ts
export type RemoteResult = {
    kind: "get";
    value: unknown | null;
} | {
    kind: "map";
    entries: unknown[];
} | {
    kind: "query";
    entries: unknown[];
} | {
    kind: "lex";
    entries: unknown[];
} | {
    kind: "records";
    result: RecordScanResult;
} | {
    kind: "vector_search";
    result: VectorSearchResult;
} | {
    kind: "node";
    node: unknown | null;
} | {
    kind: "snapshot";
    snapshot: unknown;
};
```

#### `ServeRequestContext`

Kind: interface

```ts
export interface ServeRequestContext {
    peerId: string;
    transport: "relay" | "mesh";
    requestId?: string | null;
    watchId?: string | null;
    request: PullRequestKind;
    verifiedIdentity?: VerifiedIdentity | null;
}
```

#### `ServeResultContext`

Kind: interface

```ts
export interface ServeResultContext {
    peerId: string;
    transport: "relay" | "mesh";
    requestId?: string | null;
    watchId?: string | null;
    request: PullRequestKind;
    initial: boolean;
    verifiedIdentity?: VerifiedIdentity | null;
}
```

#### `VoidHookDecision`

Kind: type alias

```ts
export type VoidHookDecision = boolean | string | {
    allow?: boolean;
    message?: string;
} | null | undefined;
```

#### `RequestHookDecision`

Kind: type alias

```ts
export type RequestHookDecision = VoidHookDecision | PullRequestKind | {
    allow?: boolean;
    message?: string;
    request?: PullRequestKind;
};
```

#### `ResultHookDecision`

Kind: type alias

```ts
export type ResultHookDecision = VoidHookDecision | RemoteResult | {
    allow?: boolean;
    message?: string;
    result?: RemoteResult;
};
```

#### `NetworkHooks`

Kind: interface

```ts
export interface NetworkHooks {
    onConnect?(context: ConnectHookContext): VoidHookDecision;
    onJoinRoom?(context: RoomHookContext): VoidHookDecision;
    onPull?(context: ServeRequestContext): RequestHookDecision;
    onWatch?(context: ServeRequestContext): RequestHookDecision;
    onServeResult?(context: ServeResultContext, result: RemoteResult): ResultHookDecision;
}
```

#### `setNetworkHooks`

Kind: function

```ts
export declare function setNetworkHooks(db: Primadb, hooks: NetworkHooks | null | undefined): void;
```

#### `clearNetworkHooks`

Kind: function

```ts
export declare function clearNetworkHooks(db: Primadb): void;
```

## Traversal semantics

Traversal methods are exported from the generated WASM runtime types.

`traverse(...)` is local-first and bounded. Connected relay or mesh transports schedule missing linked nodes for background fetch, and traversal watches receive updates as those nodes arrive.

## MoQ and WebTransport fallback

`connectPrimadbMoq(...)` uses the JS MoQ stack. In browsers that means `@moq/lite` over WebTransport when available; in Node it uses the configured WebTransport implementation or the package's Node provider.

`@moq/lite`'s WebSocket option is a MoQ transport fallback for compatible MoQ endpoints. It is not the same thing as falling back to PrimaDB's WebSocket relay protocol.

`connectMeshViaMoq(...)` uses MoQ as the WebRTC signaling underlay. Once WebRTC data channels open, mesh data moves over WebRTC. If the MoQ session itself cannot connect, callers should explicitly choose a separate fallback such as normal `connectMesh(...)` with WebSocket relay signaling or local BroadcastChannel signaling.

Current interop evidence: browser/Node JS MoQ passes Cloudflare draft-14 in this workspace; JS draft-07 still fails with WebTransport/session close errors. Native Rust draft-07 uses a separate Cloudflare `moq-rs` backend and passes independently.

## Related pages

- [Browser runtime API](browser-runtime)
- [Threaded browser package API](browser-threads)
- [Gun runtime API](gun-runtime-api)
