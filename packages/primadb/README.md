# `primadb` TypeScript Package

This package wraps Primadb's Rust/WASM browser runtime and exposes three entrypoints:

- `primadb`: default browser build
- `primadb/threads`: opt-in threaded browser build
- `primadb/gun`: Gun-compatible browser runtime on top of the default build

This is a browser-first package. The Rust core remains the source of truth for database semantics,
sync, auth, and merge behavior.

The browser crypto bindings expose `derivePasswordKey(...)` for Argon2id password-derived
secret-box keys, plus SEA-style sign/verify/encrypt/decrypt helpers.

The browser bindings also expose live remote watch helpers on relay and mesh transports:

- `watchRemoteGet(...)`
- `watchRemoteMap(...)`
- `watchRemoteQuery(...)`
- `watchRemoteLex(...)`
- `watchRemoteSnapshot(...)`

The browser bindings also expose `db.scope(...)`, `db.transaction(...)`, and
`scope.transaction(...)` for step-based local transactions and strict-scope proposal workflows.
Relay sync clients can submit strict-scope transactions to an authority peer with
`remoteTransaction(...)`.

The browser bindings expose graph-native keyed records with `putRecord(...)`, `putRecordBytes(...)`,
`putRecordBlob(...)`, `getRecord(...)`, `scanRecords(...)`, `applyRecordBatch(...)`, and
`deleteRecord(...)`. Records are stored through the same graph engine and durable browser segment
paths, so they participate in normal watches, sync, transactions, and blob storage.

The browser package build includes node-attached scripting through `attachNodeScript(...)`,
`nodeScripts(...)`, `removeNodeScript(...)`, and `executeNodeScripts(...)`. Script execution is
explicit and capability-scoped; scripts do not receive encryption keys or host/network access.

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

Relay and relay-signaled mesh transports can also use `sessionAuth` config. When a peer has an
authenticated local user, presence advertises its public key and the transport verifies it with a
nonce challenge/response before exposing `context.verifiedIdentity` to hooks.

## Build From The Repo

From the repo root:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
pnpm install
pnpm run build
```

That build:

- runs the repo's default WASM build with `crypto,scripting`
- runs the repo's threaded WASM build with `crypto,scripting`
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
- [examples/indexeddb-segments/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/indexeddb-segments/README.md)
- [examples/opfs-segments/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/opfs-segments/README.md)
- [examples/threaded-mesh/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/threaded-mesh/README.md)
- [examples/binary-stream-room/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/binary-stream-room/README.md)
- [examples/text-voice-chat/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/text-voice-chat/README.md)
- [examples/moq-sync/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/moq-sync/README.md)

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
    if (context.verifiedIdentity?.alias === "team-a") {
      return undefined;
    }
    if (context.request.kind === "get" && context.request.path.anchor === "private") {
      return "private root denied";
    }
    return undefined;
  },
});
db.scope("ledger").configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "browser-a" },
});
db.scope("ledger").transaction(
  [
    {
      kind: "increment",
      path: { anchor: "alice", segments: ["balance"] },
      by: 10,
    },
  ],
  null,
);
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
await db.openDurableStorage({
  kind: "opfs_segments",
  directory: "primadb-browser-demo",
  namespace: "main",
});
db.putRecord("agentfs/inodes/1", { mode: "file", size: 4 });
db.putRecordBytes("agentfs/chunks/1/000000", new Uint8Array([1, 2, 3, 4]));
const recordPage = db.scanRecords({ prefix: "agentfs/chunks/1/", limit: 100 });
console.log(recordPage.entries.length);
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
