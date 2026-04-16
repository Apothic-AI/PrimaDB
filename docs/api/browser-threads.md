---
title: Threaded Browser Package API
sidebar_position: 4
---

This page covers the `primadb/threads` entrypoint. It extends the browser runtime with thread bootstrap helpers and still shares the same core runtime classes documented on the browser runtime page.

> This page is generated from the current package source declarations.

## `packages/primadb/threads.ts`

Primary `primadb/threads` entrypoint.

### Direct exports

#### `PrimadbThreadsInitInput`

Kind: type alias

```ts
export type PrimadbThreadsInitInput = Parameters<typeof initWasm>[0];
```

#### `PrimadbThreadsInitOutput`

Kind: type alias

```ts
export type PrimadbThreadsInitOutput = Awaited<ReturnType<typeof initWasm>>;
```

#### `ThreadedPrimadbInitOptions`

Kind: interface

```ts
export interface ThreadedPrimadbInitOptions {
    input?: PrimadbThreadsInitInput;
    threads?: number;
}
```

#### `suggestedThreadCount`

Kind: function

```ts
export declare function suggestedThreadCount(fallback?: number): number;
```

#### `initPrimadbThreads`

Kind: function

```ts
export declare function initPrimadbThreads(input?: PrimadbThreadsInitInput): Promise<PrimadbThreadsInitOutput>;
```

#### `bootstrapPrimadbThreads`

Kind: function

```ts
export declare function bootstrapPrimadbThreads(options?: ThreadedPrimadbInitOptions): Promise<PrimadbThreadsInitOutput>;
```

#### `createThreadedPrimadb`

Kind: function

```ts
export declare function createThreadedPrimadb(replicaId?: string | null, options?: ThreadedPrimadbInitOptions): Promise<Primadb>;
```

### Re-exports

```ts
export * from "./vendor/threads/primadb.js";
```

```ts
export * from "./hooks.js";
```

```ts
export { initWasm as initWasmThreads };
```

```ts
export default bootstrapPrimadbThreads;
```

## Thread pool bootstrap

The threaded build also re-exports the wasm thread-pool bootstrap helper when built with `wasm-threads`:

```ts
function initThreadPool(threads: number): Promise<void>;
```

Shared runtime classes such as `Primadb`, `Chain`, `WebSocketSync`, and `WebRtcMesh` are documented on the [browser runtime API](browser-runtime) page.
