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
    webtransport?: WebTransportOptions;
    transport?: WebTransport;
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

#### `PrimadbMoqSession`

Kind: class

```ts
export declare class PrimadbMoqSession {
    readonly db: PrimadbLike;
    readonly connection: MoqConnection;
    readonly path: string;
    readonly track: string;
    readonly intervalMs: number;
    constructor(db: PrimadbLike, connection: MoqConnection, options: PrimadbMoqSessionOptions);
    publish(): MoqBroadcast;
    subscribe(path?: string): MoqTrack;
    startAutoFlush(): void;
    flushPending(): Promise<number>;
    close(): void;
}
```

#### `createPrimadbMoqLoopback`

Kind: function

```ts
export declare function createPrimadbMoqLoopback(options: PrimadbMoqLoopbackOptions): Promise<PrimadbMoqLoopback>;
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

## Related pages

- [Browser runtime API](browser-runtime)
- [Threaded browser package API](browser-threads)
- [Gun runtime API](gun-runtime-api)
