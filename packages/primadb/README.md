# `primadb` TypeScript Package

This package wraps Primadb's Rust/WASM browser runtime and exposes three entrypoints:

- `primadb`: default browser build
- `primadb/threads`: opt-in threaded browser build
- `primadb/gun`: Gun-compatible browser runtime on top of the default build

This is a browser-first package. The Rust core remains the source of truth for database semantics,
sync, auth, and merge behavior.

The browser bindings also expose live remote watch helpers on relay and mesh transports:

- `watchRemoteGet(...)`
- `watchRemoteMap(...)`
- `watchRemoteQuery(...)`
- `watchRemoteLex(...)`
- `watchRemoteSnapshot(...)`

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

## Package Examples

Runnable package-local examples live under [examples/](/home/bitnom/Code/gunport/primadb/packages/primadb/examples):

- [examples/default-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/default-notes/README.md)
- [examples/threaded-mesh/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/threaded-mesh/README.md)

Serve them with:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
./examples/serve.sh
```

## Default Build

```ts
import { Primadb, initPrimadb } from "primadb";

await initPrimadb();

const db = new Primadb("browser-a");
const relay = db.connectRelay({
  url: "ws://127.0.0.1:9010",
  retryIntervalMs: 1500,
});
db.openBlobStorage({
  kind: "indexed_db",
  databaseName: "primadb-browser-demo",
  storeName: "blobs",
  namespace: "main",
});
db.chain("assets").field("avatar").putBytes(new Uint8Array([1, 2, 3, 4]));
await db
  .chain("assets")
  .field("archive")
  .putBlob(new Uint8Array([5, 6, 7, 8]), "application/octet-stream");
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
const mesh = db.connectMesh({
  room: "demo-room",
  relayUrl: "ws://127.0.0.1:9010",
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    {
      urls: ["turn:turn.example.com:3478?transport=udp"],
      username: "user",
      credential: "pass",
    },
  ],
});
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
