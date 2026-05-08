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

#### `RelayServerConfig`

Kind: interface

```ts
export interface RelayServerConfig {
    bind: string;
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
    kind?: "get" | "map" | "query" | "lex" | "node" | "snapshot" | "transaction" | null;
    value?: JsonValue | null;
    error?: string | null;
}
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

#### `WebSocketSync`

Kind: class

```ts
export declare class WebSocketSync {
    isConnected(): boolean;
    pendingCount(): number;
    inflightCount(): number;
    knownPeerCount(): number;
    recommendedPeers(): JsonValue;
    remoteGet(peerId: string, path: RemotePath): Promise<JsonValue | null>;
    remoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<JsonValue>;
    remoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<JsonValue>;
    remoteNode(peerId: string, id: string): Promise<JsonValue | null>;
    remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
    remoteTransaction(peerId: string, scope: string, steps: TransactionStep[], options?: TransactionOptions | null): Promise<TransactionReport>;
    watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
    watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
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
    watchRemoteGet(peerId: string, path: RemotePath): Promise<RemoteWatch>;
    watchRemoteMap(peerId: string, path: RemotePath): Promise<RemoteWatch>;
    watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<RemoteWatch>;
    watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<RemoteWatch>;
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

#### `PrimadbMoqSessionOptions`

Kind: interface

```ts
export interface PrimadbMoqSessionOptions {
    path: string;
    track?: string;
    intervalMs?: number;
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
  webSocket: boolean;
  websocketFallback: boolean;
};
```

#### `connectPrimadbMoq`

Kind: function

```ts
export declare function connectPrimadbMoq(db: Primadb, options: ConnectPrimadbMoqOptions): Promise<PrimadbMoqSession>;
```

#### `PrimadbMoqSession`

Kind: class

```ts
export declare class PrimadbMoqSession {
    readonly db: Primadb;
    readonly connection: unknown;
    readonly path: string;
    readonly track: string;
    readonly intervalMs: number;
    publish(): unknown;
    subscribe(path?: string): unknown;
    startAutoFlush(): void;
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
