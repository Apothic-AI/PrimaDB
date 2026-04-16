---
title: Node Package API
sidebar_position: 6
---

This page covers the `primadb-node` native package surface. It is generated directly from the shipped TypeScript declaration file.

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

#### `ConnectHookContext`

Kind: interface

```ts
export interface ConnectHookContext {
    peer: {
        peerId: string;
        replicaId: string;
        transport: string;
        capabilities?: string[];
        topics?: string[];
        metadata?: Record<string, string>;
    };
    transport: "relay" | "mesh";
    relayUrl?: string | null;
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
}
```

#### `RelayClientConfig`

Kind: interface

```ts
export interface RelayClientConfig {
    url: string;
    retryIntervalMs?: number;
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

#### `RemotePath`

Kind: interface

```ts
export interface RemotePath {
    anchor: string;
    segments?: string[];
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
    kind: "snapshot";
    root?: string | null;
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
    kind: "snapshot";
    snapshot: JsonValue;
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
    kind?: "get" | "map" | "query" | "lex" | "snapshot" | null;
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
    snapshot(): JsonValue;
    snapshotForRoot(root?: string | null): JsonValue;
    exportSnapshotJson(): string;
    importSnapshotJson(payload: string): void;
    mergeSnapshotJson(payload: string): void;
    pendingOperations(): JsonValue;
    pendingEnvelope(): JsonValue;
    exportPendingOperationsJson(): string;
    drainPendingOperations(): JsonValue;
    drainPendingEnvelope(): JsonValue;
    applyOperations(operations: JsonValue): number;
    applyEnvelope(envelope: JsonValue): number;
    applyOperationsJson(payload: string): number;
    openDurableStorage(config: DurableStorageConfig): DurableStorageBinding;
    connectRelay(config: RelayClientConfig): Promise<WebSocketSync>;
    connectMesh(config: MeshConfig): Promise<WebRtcMesh>;
    setNetworkHooks(hooks: NetworkHooks): void;
    clearNetworkHooks(): void;
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
    map(): JsonValue;
    query(spec: QuerySpec): JsonValue;
    firstQuery(spec: QuerySpec): JsonValue | null;
    scan(spec: LexSpec): JsonValue;
    subscribe(): Subscription;
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
    remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
    watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
    watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
    watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
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
    watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
    flushPending(): Promise<number>;
    retryInflight(): Promise<number>;
    close(): Promise<void>;
}
```
