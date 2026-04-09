# `primadb` TypeScript Package

This package wraps Primadb's Rust/WASM browser runtime and exposes three entrypoints:

- `primadb`: default browser build
- `primadb/threads`: opt-in threaded browser build
- `primadb/gun`: Gun-compatible browser runtime on top of the default build

This is a browser-first package. The Rust core remains the source of truth for database semantics,
sync, auth, and merge behavior.

## Build From The Repo

From the repo root:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
```

That build:

- runs the repo's default WASM build with `crypto`
- runs the repo's threaded WASM build with `crypto`
- vendors the generated bindings into the package
- compiles the TypeScript entrypoints into `dist/`

The threaded subpath still has the same runtime requirements as the rest of Primadb's
`wasm-threads` support:

- nightly Rust for source builds
- `SharedArrayBuffer`
- COOP/COEP headers

## Default Build

```ts
import { Primadb, initPrimadb } from "primadb";

await initPrimadb();

const db = new Primadb("browser-a");
const relay = db.connectRelay({
  url: "ws://127.0.0.1:9010",
  retryIntervalMs: 1500,
});
```

## Threaded Build

```ts
import {
  Primadb,
  bootstrapPrimadbThreads,
  parallelEnabled,
  parallelThreadCount,
} from "primadb/threads";

await bootstrapPrimadbThreads({ threads: 4 });

const db = new Primadb("threaded-browser");
console.log(parallelEnabled(), parallelThreadCount());
```

If you want more control, you can call `initPrimadbThreads(...)` and `initThreadPool(...)`
yourself instead of using `bootstrapPrimadbThreads(...)`.

## Gun Runtime

```ts
import initPrimadbGun from "primadb/gun";

const Gun = await initPrimadbGun();
const gun = Gun({
  peers: ["ws://127.0.0.1:9010/gun"],
});
```

## Verification

Useful repo-local checks:

```bash
npm run build
npm run typecheck
npm run smoke
npm run pack:check
```
