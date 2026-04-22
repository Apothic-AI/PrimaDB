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

They also now expose typed network-boundary hook helpers through:

- `setNetworkHooks(db, hooks)`
- `clearNetworkHooks(db)`

Those wrap the underlying browser binding methods and give TypeScript apps typed callback
signatures for:

- `onConnect(...)`
- `onJoinRoom(...)`
- `onPull(...)`
- `onWatch(...)`
- `onServeResult(...)`

## Build From The Repo

From the repo root:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
pnpm install
pnpm run build
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

Run them with Vite:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

## Default Build

```ts
import { Primadb, initPrimadb, setNetworkHooks } from "primadb";

await initPrimadb();

const db = new Primadb("browser-a");
setNetworkHooks(db, {
  onPull(context) {
    if (context.request.kind === "get" && context.request.path.anchor === "private") {
      return "private root denied";
    }
    return undefined;
  },
});
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
  setNetworkHooks,
  parallelEnabled,
  parallelThreadCount,
} from "primadb/threads";

await bootstrapPrimadbThreads({ threads: 4 });

const db = new Primadb("threaded-browser");
setNetworkHooks(db, {
  onServeResult(_context, result) {
    return result;
  },
});
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
pnpm run build
pnpm run typecheck
pnpm run smoke
pnpm run pack:check
```
