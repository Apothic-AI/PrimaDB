---
title: Node Package API
sidebar_position: 6
---

This page covers the `primadb-node` native package surface. It is generated directly from the shipped TypeScript declaration files.

> This page is generated from the current package source declarations.

## `packages/primadb-node/index.d.ts`

Published Node package declarations.

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

#### `IdentityKeyPair`

Kind: interface

```ts
export interface IdentityKeyPair {
    publicKey: string;
    secretKey: string;
}
```

#### `PasswordKeyDerivationParams`

Kind: interface

```ts
export interface PasswordKeyDerivationParams {
    memoryCostKiB?: number;
    timeCost?: number;
    parallelism?: number;
}
```

#### `PasswordKeyDerivationOptions`

Kind: interface

```ts
export interface PasswordKeyDerivationOptions extends PasswordKeyDerivationParams {
    saltBase64?: string | null;
}
```

#### `PasswordDerivedKey`

Kind: interface

```ts
export interface PasswordDerivedKey {
    algorithm: "argon2id-v1.3";
    keyBase64: string;
    saltBase64: string;
    params: Required<PasswordKeyDerivationParams>;
}
```

#### `UserGrant`

Kind: interface

```ts
export interface UserGrant {
    root: string;
    read?: boolean;
    write?: boolean;
}
```

#### `generateIdentity`

Kind: function

```ts
export declare function generateIdentity(): IdentityKeyPair;
```

#### `derivePasswordKey`

Kind: function

```ts
export declare function derivePasswordKey(password: string, options?: PasswordKeyDerivationOptions | null): PasswordDerivedKey;
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

#### `SessionAuthConfig`

Kind: interface

```ts
export interface SessionAuthConfig {
    requireAuthenticatedPeers?: boolean;
    trustedPublicKeys?: string[];
    trustedAliases?: string[];
    challengeTimeoutMs?: number;
    sessionTtlMs?: number;
    allowUnauthenticatedPresence?: boolean;
}
```

#### `RelayClientConfig`

Kind: interface

```ts
export interface RelayClientConfig {
    url: string;
    retryIntervalMs?: number;
    sessionAuth?: SessionAuthConfig;
}
```

#### `MoqDraft`

Kind: type alias

```ts
export type MoqDraft = "draft07" | "draft14" | "draft_latest";
```

#### `MoqRelayClientConfig`

Kind: interface

```ts
export interface MoqRelayClientConfig {
    url: string;
    path: string;
    track?: string;
    channel?: string;
    subscribe?: string[];
    draft?: MoqDraft;
    retryIntervalMs?: number;
    tlsDisableVerify?: boolean;
    sessionAuth?: SessionAuthConfig;
}
```

#### `RelayEndpointConfig`

Kind: type alias

```ts
export type RelayEndpointConfig = {
    kind: "web_socket";
    url: string;
    retryIntervalMs?: number;
    sessionAuth?: SessionAuthConfig;
} | ({
    kind: "moq";
} & MoqRelayClientConfig);
```

#### `RelayServerConfig`

Kind: interface

```ts
export interface RelayServerConfig {
    bind: string;
    moq?: MoqRelayClientConfig | null;
}
```

#### `MeshSignalingMode`

Kind: type alias

```ts
export type MeshSignalingMode = "relay" | "broadcast_channel";
```

#### `IceServerConfig`

Kind: interface

```ts
export interface IceServerConfig {
    urls: string | string[];
    username?: string | null;
    credential?: string | null;
}
```

#### `MeshConfig`

Kind: interface

```ts
export interface MeshConfig {
    room: string;
    signaling?: MeshSignalingMode;
    relayUrl?: string | null;
    relayEndpoint?: RelayEndpointConfig | null;
    retryIntervalMs?: number;
    iceServers?: IceServerConfig[];
    sessionAuth?: SessionAuthConfig;
}
```

#### `DurableStorageConfig`

Kind: type alias

```ts
export type DurableStorageConfig = {
    kind: "snapshot_file";
    path: string;
} | {
    kind: "segment_files";
    directory: string;
    journalRetention?: number;
    durability?: SegmentDurability;
    lockMode?: SegmentLockMode;
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
    kind: "files";
    directory: string;
    durability?: SegmentDurability;
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

#### `QueryOrder`

Kind: interface

```ts
export interface QueryOrder {
    path: string;
    direction?: "asc" | "desc";
}
```

#### `QueryFilter`

Kind: type alias

```ts
export type QueryFilter = {
    kind: "eq";
    path: string;
    value: JsonValue;
} | {
    kind: "ne";
    path: string;
    value: JsonValue;
} | {
    kind: "gt";
    path: string;
    value: JsonValue;
} | {
    kind: "gte";
    path: string;
    value: JsonValue;
} | {
    kind: "lt";
    path: string;
    value: JsonValue;
} | {
    kind: "lte";
    path: string;
    value: JsonValue;
} | {
    kind: "prefix";
    path: string;
    value: string;
} | {
    kind: "contains";
    path: string;
    value: string;
} | {
    kind: "exists";
    path: string;
};
```

#### `QuerySpec`

Kind: interface

```ts
export interface QuerySpec {
    filters?: QueryFilter[];
    order?: QueryOrder | null;
    limit?: number | null;
    offset?: number;
}
```

#### `LexSpec`

Kind: interface

```ts
export interface LexSpec {
    prefix?: string | null;
    startAt?: string | null;
    startAfter?: string | null;
    endAt?: string | null;
    endBefore?: string | null;
    reverse?: boolean;
    limit?: number | null;
    depth?: number;
    followLinks?: boolean;
}
```

#### `TraversalDirection`

Kind: type alias

```ts
export type TraversalDirection = "outbound" | "inbound" | "both";
```

#### `TraversalStrategy`

Kind: type alias

```ts
export type TraversalStrategy = "bfs" | "dfs";
```

#### `TraversalEdgeKind`

Kind: type alias

```ts
export type TraversalEdgeKind = "link" | "set_member";
```

#### `TraversalEdge`

Kind: interface

```ts
export interface TraversalEdge {
    source: string;
    field: string;
    target: string;
    kind: TraversalEdgeKind;
}
```

#### `TraversalSpec`

Kind: interface

```ts
export interface TraversalSpec {
    direction?: TraversalDirection;
    strategy?: TraversalStrategy;
    maxDepth?: number;
    limit?: number | null;
    edgeFields?: string[] | null;
    followLinks?: boolean;
    followSets?: boolean;
    includeStart?: boolean;
    includeValues?: boolean;
    filters?: QueryFilter[];
    fetchMissing?: boolean;
    maxFetches?: number;
}
```

#### `TraversalEntry`

Kind: interface

```ts
export interface TraversalEntry {
    nodeId: string;
    depth: number;
    path: string[];
    via?: TraversalEdge | null;
    value?: JsonValue | null;
}
```

#### `TraversalResult`

Kind: interface

```ts
export interface TraversalResult {
    entries: TraversalEntry[];
    complete: boolean;
    timedOut: boolean;
    depthLimitReached: boolean;
    resultLimitReached: boolean;
    fetched: number;
    missing: string[];
    denied: string[];
}
```

#### `RemotePath`

Kind: interface

```ts
export interface RemotePath {
    anchor: string;
    segments?: string[];
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

#### `ScopeConsistency`

Kind: type alias

```ts
export type ScopeConsistency = "eventual" | "local_transactional" | "coordinated";
```

#### `ScopeOfflineWrites`

Kind: type alias

```ts
export type ScopeOfflineWrites = "reject" | "queue_provisional";
```

#### `ScopeIsolation`

Kind: type alias

```ts
export type ScopeIsolation = "serializable";
```

#### `ScopeReadMode`

Kind: type alias

```ts
export type ScopeReadMode = "cached" | "authority" | "quorum";
```

#### `ScopeAuthority`

Kind: type alias

```ts
export type ScopeAuthority = {
    kind: "peer";
    peerId: string;
} | {
    kind: "full_node";
    peerId: string;
} | {
    kind: "quorum";
    peers: string[];
    threshold: number;
};
```

#### `ScopePolicy`

Kind: interface

```ts
export interface ScopePolicy {
    consistency?: ScopeConsistency;
    authority?: ScopeAuthority | null;
    isolation?: ScopeIsolation;
    readMode?: ScopeReadMode;
    offlineWrites?: ScopeOfflineWrites;
}
```

#### `TransactionOptions`

Kind: interface

```ts
export interface TransactionOptions {
    offline?: ScopeOfflineWrites | null;
}
```

#### `TransactionStep`

Kind: type alias

```ts
export type TransactionStep = {
    kind: "put";
    path: RemotePath;
    value: JsonValue;
} | {
    kind: "unset";
    path: RemotePath;
} | {
    kind: "set";
    path: RemotePath;
    value: JsonValue;
} | {
    kind: "remove";
    path: RemotePath;
    value: JsonValue;
} | {
    kind: "assert_exists";
    path: RemotePath;
} | {
    kind: "assert_absent";
    path: RemotePath;
} | {
    kind: "assert_value";
    path: RemotePath;
    value: JsonValue;
} | {
    kind: "assert_revision";
    path: RemotePath;
    revision?: JsonValue | null;
} | {
    kind: "increment";
    path: RemotePath;
    by: number;
};
```

#### `TransactionReport`

Kind: interface

```ts
export interface TransactionReport {
    status: "committed" | "provisional";
    operationCount: number;
    memberIds?: string[];
    proposalId?: string | null;
}
```

#### `ProvisionalTransaction`

Kind: interface

```ts
export interface ProvisionalTransaction {
    id: string;
    scope: string;
    createdAtMillis: number;
    steps: TransactionStep[];
    options?: TransactionOptions;
}
```

#### `ScriptRuntime`

Kind: type alias

```ts
export type ScriptRuntime = "rhai";
```

#### `ScriptPathGrant`

Kind: interface

```ts
export interface ScriptPathGrant {
    root: string;
    segments?: string[];
    recursive?: boolean;
}
```

#### `ScriptCapabilities`

Kind: interface

```ts
export interface ScriptCapabilities {
    read?: ScriptPathGrant[];
    query?: ScriptPathGrant[];
    traverse?: ScriptPathGrant[];
    write?: ScriptPathGrant[];
    transaction?: ScriptPathGrant[];
}
```

#### `ScriptLimits`

Kind: interface

```ts
export interface ScriptLimits {
    maxOperations?: number;
    maxCallLevels?: number;
    maxVariables?: number;
    maxFunctions?: number;
    maxModules?: number;
    maxExpressionDepth?: number;
    maxStringBytes?: number;
    maxArraySize?: number;
    maxMapSize?: number;
}
```

#### `NodeScript`

Kind: interface

```ts
export interface NodeScript {
    id: string;
    runtime?: ScriptRuntime;
    entry?: string;
    source: string;
    sourceHash?: string | null;
    author?: string | null;
    signature?: string | null;
    capabilities?: ScriptCapabilities;
    metadata?: JsonValue;
}
```

#### `ScriptExecutionOptions`

Kind: interface

```ts
export interface ScriptExecutionOptions {
    args?: JsonValue;
    capabilities?: ScriptCapabilities;
    applyWrites?: boolean;
    limits?: ScriptLimits;
}
```

#### `ScriptExecutionResult`

Kind: interface

```ts
export interface ScriptExecutionResult {
    scriptId: string;
    runtime: ScriptRuntime;
    sourceHash: string;
    value: JsonValue;
    steps: TransactionStep[];
    report?: TransactionReport | null;
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
    spec: QuerySpec;
} | {
    kind: "lex";
    path: {
        anchor: string;
        segments?: string[];
    };
    spec: LexSpec;
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
} | {
    kind: "transaction";
    scope: string;
    steps: TransactionStep[];
    options?: TransactionOptions;
};
```

#### `RemoteResult`

Kind: type alias

```ts
export type RemoteResult = {
    kind: "get";
    value: JsonValue | null;
} | {
    kind: "map";
    entries: JsonValue[];
} | {
    kind: "query";
    entries: JsonValue[];
} | {
    kind: "lex";
    entries: JsonValue[];
} | {
    kind: "records";
    result: RecordScanResult;
} | {
    kind: "vector_search";
    result: VectorSearchResult;
} | {
    kind: "node";
    node: JsonValue | null;
} | {
    kind: "snapshot";
    snapshot: JsonValue;
} | {
    kind: "transaction";
    report: TransactionReport;
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

#### `SubscriptionMessage`

Kind: interface

```ts
export interface SubscriptionMessage {
    done: boolean;
    value?: JsonValue | null;
}
```

#### `RemoteWatchMessage`

Kind: interface

```ts
export interface RemoteWatchMessage {
    done: boolean;
    initial?: boolean;
    kind?: "get" | "map" | "query" | "lex" | "records" | "node" | "snapshot" | "transaction" | null;
    value?: JsonValue | null;
    error?: string | null;
}
```

#### `RouteTarget`

Kind: type alias

```ts
export type RouteTarget = {
    kind: "broadcast";
} | {
    kind: "peer";
    value: string;
} | {
    kind: "topic";
    value: string;
};
```

#### `RouteTransportKind`

Kind: type alias

```ts
export type RouteTransportKind = "web_socket" | "moq" | "web_rtc" | "broadcast_channel" | "in_memory";
```

#### `ApplicationRouteMessage`

Kind: interface

```ts
export interface ApplicationRouteMessage {
    namespace: string;
    protocol: string;
    topic?: string | null;
    body: JsonValue;
    metadata?: Record<string, JsonValue>;
}
```

#### `ApplicationRouteEvent`

Kind: interface

```ts
export interface ApplicationRouteEvent {
    routeId: string;
    from: string;
    channel: string;
    target: RouteTarget;
    issuedAtMillis: number;
    receivedAtMillis: number;
    transport: RouteTransportKind;
    verifiedIdentity?: VerifiedIdentity | null;
    message: ApplicationRouteMessage;
}
```

#### `ApplicationRouteFilter`

Kind: interface

```ts
export interface ApplicationRouteFilter {
    namespace?: string | null;
    protocol?: string | null;
    topic?: string | null;
}
```

#### `RemotePeerFailure`

Kind: interface

```ts
export interface RemotePeerFailure {
    peerId: string;
    transport: RouteTransportKind;
    message: string;
}
```

#### `RemotePeerRecords`

Kind: interface

```ts
export interface RemotePeerRecords {
    peerId: string;
    transport: RouteTransportKind;
    result: RecordScanResult;
}
```

#### `RemoteRecordConflictSource`

Kind: interface

```ts
export interface RemoteRecordConflictSource {
    peerId: string;
    transport: RouteTransportKind;
    contentHash: string;
}
```

#### `RemoteRecordConflict`

Kind: interface

```ts
export interface RemoteRecordConflict {
    key: string;
    winnerPeerId: string;
    winnerHash: string;
    sources: RemoteRecordConflictSource[];
}
```

#### `RemoteRecordsFanIn`

Kind: interface

```ts
export interface RemoteRecordsFanIn {
    requestId: string;
    records: RemotePeerRecords[];
    failures: RemotePeerFailure[];
    merged: RecordScanResult;
    conflicts: RemoteRecordConflict[];
}
```

#### `RemoteFanInWatchEvent`

Kind: type alias

```ts
export type RemoteFanInWatchEvent = {
    kind: "update";
    peerId: string;
    transport: RouteTransportKind;
    initial: boolean;
    sequence: number;
    result: RemoteResult;
} | {
    kind: "failure";
    peerId: string;
    transport: RouteTransportKind;
    message: string;
    terminal: boolean;
} | {
    kind: "closed";
};
```

#### `Primadb`

Kind: class

```ts
export declare class Primadb {
    constructor(replicaId?: string | null);
    replicaId(): string;
    chain(root: string): Chain;
    scope(root: string): Scope;
    transaction(steps: TransactionStep[]): TransactionReport;
    snapshot(): JsonValue;
    snapshotForRoot(root?: string | null): JsonValue;
    nodeState(id: string): JsonValue | null;
    applyNodeState(node: JsonValue): boolean;
    exportSnapshotJson(): string;
    importSnapshotJson(payload: string): void;
    mergeSnapshotJson(payload: string): void;
    pendingOperations(): JsonValue;
    pendingEnvelope(): JsonValue;
    exportPendingOperationsJson(): string;
    drainPendingOperations(): JsonValue;
    drainPendingEnvelope(): JsonValue;
    drainPendingEnvelopeJson(): string;
    applyOperations(operations: JsonValue): number;
    applyEnvelope(envelope: JsonValue): number;
    applyOperationsJson(payload: string): number;
    openDurableStorage(config: DurableStorageConfig): DurableStorageBinding;
    openBlobStorage(config: BlobStorageConfig): BlobStorageBinding;
    closeDurableStorage(): void;
    syncStorage(): StorageSyncReport;
    storageRecoveryReport(): StorageRecoveryReport | null;
    putRecord(key: string, value: JsonValue): void;
    putRecordBytes(key: string, value: Uint8Array): void;
    putRecordBlob(key: string, value: Uint8Array, mediaType?: string | null): BlobRef;
    getRecord(key: string): RecordEntry | null;
    scanRecords(scan: RecordScan): RecordScanResult;
    watchRecords(scan: RecordScan): RecordWatchSubscription;
    createVectorCollection(name: string, config: VectorCollectionConfig): void;
    putVector(collection: string, id: string, vector: number[], metadata?: JsonValue | null): void;
    deleteVector(collection: string, id: string): void;
    getVector(collection: string, id: string): VectorEntry | null;
    searchVectors(collection: string, query: number[], spec: VectorSearchSpec): VectorSearchResult;
    watchVectorSearch(collection: string, query: number[], spec: VectorSearchSpec): VectorWatchSubscription;
    applyRecordBatch(batch: RecordBatch): RecordBatchReport;
    deleteRecord(key: string): void;
    attachNodeScript(path: RemotePath, script: NodeScript): void;
    removeNodeScript(path: RemotePath, scriptId: string): void;
    nodeScripts(path: RemotePath): NodeScript[];
    executeNodeScripts(path: RemotePath, options?: ScriptExecutionOptions | null): ScriptExecutionResult[];
    registerUser(alias: string, publicKey: string, grants: UserGrant[]): void;
    authenticateLocalUser(alias: string, secretKey: string, grants: UserGrant[]): void;
    setRequireSignedSync(required: boolean): void;
    setSnapshotEncryptionKey(keyBase64: string): void;
    setTransportEncryptionKey(keyBase64: string): void;
    connectRelay(config: RelayClientConfig): Promise<WebSocketSync>;
    connectMesh(config: MeshConfig): Promise<WebRtcMesh>;
    setNetworkHooks(hooks: NetworkHooks): void;
    clearNetworkHooks(): void;
}
```

#### `Scope`

Kind: class

```ts
export declare class Scope {
    root(): string;
    configure(policy: ScopePolicy): void;
    policy(): ScopePolicy | null;
    proposals(): ProvisionalTransaction[];
    transaction(steps: TransactionStep[], options?: TransactionOptions | null): TransactionReport;
}
```

#### `Chain`

Kind: class

```ts
export declare class Chain {
    field(key: string): Chain;
    path(): string;
    put(value: JsonValue): void;
    putSigned(value: JsonValue, certificate?: string | null): void;
    once(): JsonValue | null;
    unset(): void;
    set(value: JsonValue): string;
    setSigned(value: JsonValue, certificate?: string | null): string;
    remove(value: JsonValue): string;
    putBytes(value: Uint8Array): void;
    onceBytes(): Uint8Array | null;
    putBlob(value: Uint8Array, mediaType?: string | null): JsonValue;
    blobRef(): JsonValue | null;
    getBlob(): Uint8Array | null;
    map(): JsonValue;
    query(spec: QuerySpec): JsonValue;
    firstQuery(spec: QuerySpec): JsonValue | null;
    scan(spec: LexSpec): JsonValue;
    traverse(spec: TraversalSpec): TraversalResult;
    subscribe(): Subscription;
    watchTraverse(spec: TraversalSpec): TraversalSubscription;
}
```

#### `Subscription`

Kind: class

```ts
export declare class Subscription {
    next(): Promise<SubscriptionMessage>;
    tryNext(): SubscriptionMessage;
    close(): void;
}
```

#### `TraversalSubscription`

Kind: class

```ts
export declare class TraversalSubscription {
    next(): Promise<{ done: boolean; value?: TraversalResult | null }>;
    tryNext(): { done: boolean; value?: TraversalResult | null };
    close(): void;
}
```

#### `RecordWatchSubscription`

Kind: class

```ts
export declare class RecordWatchSubscription {
    next(): Promise<{ done: boolean; value?: RecordScanResult | null }>;
    tryNext(): { done: boolean; value?: RecordScanResult | null };
    close(): void;
}
```

#### `VectorWatchSubscription`

Kind: class

```ts
export declare class VectorWatchSubscription {
    next(): Promise<{ done: boolean; value?: VectorSearchResult | null }>;
    tryNext(): { done: boolean; value?: VectorSearchResult | null };
    close(): void;
}
```

#### `RelayServer`

Kind: class

```ts
export declare class RelayServer {
    static listen(config: RelayServerConfig): Promise<RelayServer>;
    bindAddr(): string;
    url(): string;
    clientCount(): number;
    peerCount(): number;
    close(): Promise<void>;
}
```

#### `RemoteWatch`

Kind: class

```ts
export declare class RemoteWatch {
    next(): Promise<RemoteWatchMessage>;
    tryNext(): RemoteWatchMessage;
    close(): void;
}
```

#### `ApplicationRouteSubscription`

Kind: class

```ts
export declare class ApplicationRouteSubscription {
    next(): Promise<ApplicationRouteEvent | null>;
    tryNext(): ApplicationRouteEvent | null;
    drain(): ApplicationRouteEvent[];
    close(): void;
}
```

#### `RemoteFanInWatch`

Kind: class

```ts
export declare class RemoteFanInWatch {
    next(): Promise<RemoteFanInWatchEvent | null>;
    tryNext(): RemoteFanInWatchEvent | null;
    drain(): RemoteFanInWatchEvent[];
    close(): void;
}
```

#### `WebSocketSync`

Kind: class

```ts
export declare class WebSocketSync {
    isConnected(): boolean;
    pendingCount(): number;
    inflightCount(): number;
    knownPeerCount(): number;
    recommendedPeers(): JsonValue;
    publishApplication(message: ApplicationRouteMessage, target?: RouteTarget | null): JsonValue;
    sendApplication(namespace: string, protocol: string, topic: string | null | undefined, body: JsonValue, metadata?: Record<string, JsonValue> | null, target?: RouteTarget | null): JsonValue;
    subscribeApplications(filter?: ApplicationRouteFilter | null): ApplicationRouteSubscription;
    get(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<JsonValue | null>;
    query(path: RemotePath, spec: QuerySpec, policy?: RemoteInterestPolicy | null): Promise<JsonValue>;
    lex(path: RemotePath, spec: LexSpec, policy?: RemoteInterestPolicy | null): Promise<JsonValue>;
    records(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RecordScanResult>;
    vectorSearch(collection: string, query: number[], spec: VectorSearchSpec, policy?: RemoteInterestPolicy | null): Promise<VectorSearchResult>;
    node(id: string, policy?: RemoteInterestPolicy | null): Promise<JsonValue | null>;
    snapshot(root?: string | null, policy?: RemoteInterestPolicy | null): Promise<JsonValue>;
    recordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteRecordsFanIn>;
    remoteGet(peerId: string, path: RemotePath): Promise<JsonValue | null>;
    remoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<JsonValue>;
    remoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<JsonValue>;
    remoteRecords(peerId: string, scan: RecordScan): Promise<RecordScanResult>;
    remoteVectorSearch(peerId: string, collection: string, query: number[], spec: VectorSearchSpec): Promise<VectorSearchResult>;
    remoteNode(peerId: string, id: string): Promise<JsonValue | null>;
    remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
    remoteTransaction(peerId: string, scope: string, steps: TransactionStep[], options?: TransactionOptions | null): Promise<TransactionReport>;
    watchGet(path: RemotePath, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchMap(path: RemotePath, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchQuery(path: RemotePath, spec: QuerySpec, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchLex(path: RemotePath, spec: LexSpec, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchRecords(scan: RecordScan, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchRecordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): RemoteFanInWatch;
    watchVectorSearch(collection: string, query: number[], spec: VectorSearchSpec, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchNode(id: string, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchSnapshot(root?: string | null, policy?: RemoteInterestPolicy | null): RemoteWatch;
    watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
    watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
    watchRemoteRecords(peerId: string, scan: RecordScan): RemoteWatch;
    watchRemoteVectorSearch(peerId: string, collection: string, query: number[], spec: VectorSearchSpec): RemoteWatch;
    watchRemoteNode(peerId: string, id: string): RemoteWatch;
    watchRemoteSnapshot(peerId: string, root?: string | null): RemoteWatch;
    flushPending(): Promise<number>;
    retryInflight(): Promise<number>;
    close(): void;
}
```

#### `WebRtcMesh`

Kind: class

```ts
export declare class WebRtcMesh {
    peerId(): string;
    signalingMode(): string;
    relayUrl(): string | undefined;
    relayConnected(): boolean;
    peerCount(): Promise<number>;
    openPeerCount(): Promise<number>;
    inflightCount(): Promise<number>;
    recommendedPeers(): Promise<JsonValue>;
    publishApplication(message: ApplicationRouteMessage, target?: RouteTarget | null): Promise<JsonValue>;
    sendApplication(namespace: string, protocol: string, topic: string | null | undefined, body: JsonValue, metadata?: Record<string, JsonValue> | null, target?: RouteTarget | null): Promise<JsonValue>;
    subscribeApplications(filter?: ApplicationRouteFilter | null): ApplicationRouteSubscription;
    recordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteRecordsFanIn>;
    watchGet(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchMap(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchQuery(path: RemotePath, spec: QuerySpec, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchLex(path: RemotePath, spec: LexSpec, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchRecords(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchRecordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteFanInWatch>;
    watchVectorSearch(collection: string, query: number[], spec: VectorSearchSpec, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchNode(id: string, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchSnapshot(root?: string | null, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
    watchRemoteGet(peerId: string, path: RemotePath): Promise<RemoteWatch>;
    watchRemoteMap(peerId: string, path: RemotePath): Promise<RemoteWatch>;
    watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<RemoteWatch>;
    watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<RemoteWatch>;
    watchRemoteRecords(peerId: string, scan: RecordScan): Promise<RemoteWatch>;
    watchRemoteVectorSearch(peerId: string, collection: string, query: number[], spec: VectorSearchSpec): Promise<RemoteWatch>;
    watchRemoteNode(peerId: string, id: string): Promise<RemoteWatch>;
    watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
    flushPending(): Promise<number>;
    retryInflight(): Promise<number>;
    close(): Promise<void>;
}
```

## `packages/primadb-node/moq.d.ts`

Experimental `primadb-node/moq` helper declarations.

### Direct exports

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
    transport: "moq";
    verifiedIdentity: null;
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
    webtransport?: unknown;
    transport?: unknown;
    nodeWebTransport?: boolean;
    nodeWebTransportOptions?: unknown;
    tlsDisableVerify?: boolean;
}
```

#### `PrimadbMoqLoopbackOptions`

Kind: interface

```ts
export interface PrimadbMoqLoopbackOptions {
    publisherDb: Primadb;
    subscriberDb: Primadb;
    path: string;
    track?: string;
    channel?: string;
    intervalMs?: number;
    url?: string | URL;
    protocol?: string;
}
```

#### `moqRuntimeSupport`

Kind: function

```ts
export declare function moqRuntimeSupport(): {
  webTransport: boolean;
  nodeWebTransportProvider: boolean;
  webSocket: boolean;
  websocketFallback: boolean;
};
```

#### `connectPrimadbMoq`

Kind: function

```ts
export declare function connectPrimadbMoq(db: Primadb, options: ConnectPrimadbMoqOptions): Promise<PrimadbMoqSession>;
```

#### `PrimadbApplicationRouteSubscription`

Kind: class

```ts
export declare class PrimadbApplicationRouteSubscription {
    readonly filter: PrimadbApplicationRouteFilter;
    next(): Promise<PrimadbApplicationRouteEvent | null>;
    tryNext(): PrimadbApplicationRouteEvent | null;
    drain(): PrimadbApplicationRouteEvent[];
    close(): void;
}
```

#### `PrimadbMoqSession`

Kind: class

```ts
export declare class PrimadbMoqSession {
    readonly db: Primadb;
    readonly connection: unknown;
    readonly path: string;
    readonly track: string;
    readonly channel: string;
    readonly peerId: string;
    readonly intervalMs: number;
    readonly retryIntervalMs: number;
    publish(): unknown;
    subscribe(path?: string): unknown;
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

#### `createPrimadbMoqLoopback`

Kind: function

```ts
export declare function createPrimadbMoqLoopback(options: PrimadbMoqLoopbackOptions): Promise<PrimadbMoqLoopback>;
```

## Remote interest selection

`WebSocketSync.get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)` select a connected/recommended peer automatically. Relay and mesh watches are available through `watchGet(...)`, `watchQuery(...)`, `watchRecords(...)`, and the other `watch*` helpers.

Pass `RemoteInterestPolicy` only when needed, for example `{ target: "peer", peerId: "native:ledger", requireCapability: true }`. The explicit `remote*` and `watchRemote*` methods still target a concrete peer id.

## Application routes

Application route APIs carry caller-defined messages inside `RoutePayload::Application` / `{ kind: "application" }` while preserving the surrounding `RouteEnvelope` metadata.

Use `publishApplication(...)` / `publish_application(...)` when the caller has already assembled an application message, or `sendApplication(...)` / `send_application(...)` for the namespace/protocol/topic/body convenience shape.

`subscribeApplications(...)` / `subscribe_applications(...)` returns a filtered subscription with deterministic `next`/`tryNext`/`drain`/`close` behavior. Received events include route id, source peer, channel, target, receive time, transport kind where available, and the application message.

These APIs are RouteEnvelope-level. They do not expose raw WebSocket, WebRTC, WebTransport, or MoQ socket handles.

## Record fan-in

`recordsFanIn(...)` / `records_fan_in(...)` sends a record scan to every currently reachable peer that matches the supplied `RemoteInterestPolicy` instead of selecting one ambient peer.

`watchRecordsFanIn(...)` / `watch_records_fan_in(...)` keeps child watches open across all matching peers and emits source-tagged updates plus partial failures. Closing the returned watch cancels all child watches.

Fan-in results include per-peer records, a deterministic merged result, conflict metadata, and partial failure diagnostics. Per-peer source metadata is preserved so callers can apply their own trust or dedupe policy above the built-in deterministic merge.

## MoQ and WebTransport fallback

`connectPrimadbMoq(...)` uses the JS MoQ stack. In browsers that means `@moq/lite` over WebTransport when available; in Node it uses the configured WebTransport implementation or the package's Node provider.

`@moq/lite`'s WebSocket option is a MoQ transport fallback for compatible MoQ endpoints. It is not the same thing as falling back to PrimaDB's WebSocket relay protocol.

`connectMeshViaMoq(...)` uses MoQ as the WebRTC signaling underlay. Once WebRTC data channels open, mesh data moves over WebRTC. If the MoQ session itself cannot connect, callers should explicitly choose a separate fallback such as normal `connectMesh(...)` with WebSocket relay signaling or local BroadcastChannel signaling.

Current interop evidence: browser/Node JS MoQ passes Cloudflare draft-14 in this workspace; JS draft-07 still fails with WebTransport/session close errors. Native Rust draft-07 uses a separate Cloudflare `moq-rs` backend and passes independently.

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

`Chain.traverse(...)` returns the current local traversal result immediately. With an active relay or mesh connection, missing linked nodes are scheduled for bounded background fetch.

`Chain.watchTraverse(...)` receives updated traversal results as fetched nodes merge into the local graph.

`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation.
