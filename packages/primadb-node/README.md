# `primadb-node`

`primadb-node` is a native Node.js package for Primadb. Unlike the browser package in
[packages/primadb](../primadb), this package wraps the
native Rust runtime directly through a Node addon.

Current surface:

- `Primadb` and `Chain` for local graph operations
- `Scope` and step-based transactions for local ACID writes and coordinated strict-scope proposals
- durable native storage through `openDurableStorage(...)`
- explicit SegmentFiles sync/recovery/close helpers through `syncStorage()`, `storageRecoveryReport()`, and `closeDurableStorage()`
- content-addressed native blob storage through `openBlobStorage(...)`
- first-class binary helpers through `putBytes()`, `onceBytes()`, `putBlob()`, and `getBlob()`
- graph-native keyed records through `putRecord(...)`, `putRecordBytes(...)`, `putRecordBlob(...)`, `getRecord(...)`, `scanRecords(...)`, `watchRecords(...)`, `applyRecordBatch(...)`, and `deleteRecord(...)`
- subscriptions
- native relay server hosting through `RelayServer.listen(...)`
- native relay sync through `connectRelay(...)`, including disconnected startup with background relay retry
- remote strict-scope transaction submission through `remoteTransaction(...)` on relay sync clients
- native WebRTC mesh through `connectMesh(...)`, including disconnected startup with background relay retry
- peer-agnostic relay pulls through `get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)`, with optional `RemoteInterestPolicy` constraints
- peer-agnostic relay/mesh watches through `watchGet(...)`, `watchMap(...)`, `watchQuery(...)`, `watchLex(...)`, `watchRecords(...)`, `watchNode(...)`, and `watchSnapshot(...)`
- application RouteEnvelope payloads through `publishApplication(...)`, `sendApplication(...)`, and `subscribeApplications(...)`
- source-tagged multi-peer record fan-in through `recordsFanIn(...)` and `watchRecordsFanIn(...)`
- BM25 text search through `textSearch(...)`, `watchTextSearch(...)`, `textSearchFanIn(...)`, and `watchTextSearchFanIn(...)`
- explicit peer-targeted pulls and watches through `remoteGet(...)`, `remoteQuery(...)`, `remoteLex(...)`, `remoteRecords(...)`, `remoteNode(...)`, `remoteSnapshot(...)`, `watchRemoteGet(...)`, `watchRemoteMap(...)`, `watchRemoteQuery(...)`, `watchRemoteLex(...)`, `watchRemoteRecords(...)`, `watchRemoteNode(...)`, and `watchRemoteSnapshot(...)`
- authenticated relay/mesh session identity through `generateIdentity()`, `authenticateLocalUser(...)`, `sessionAuth` config, and `context.verifiedIdentity`
- Argon2id password-derived secret-box keys through `derivePasswordKey(...)`, usable with `setSnapshotEncryptionKey(...)` and `setTransportEncryptionKey(...)`
- node-attached scripting through `attachNodeScript(...)`, `nodeScripts(...)`, `removeNodeScript(...)`, and `executeNodeScripts(...)`
- network-boundary hooks through `setNetworkHooks(...)` / `clearNetworkHooks()`
- experimental MoQ sync helpers through `primadb-node/moq`

## Package Examples

Runnable package-local examples live under [examples/](examples):

- [examples/local-notes/README.md](examples/local-notes/README.md)
- [examples/mesh-peer/README.md](examples/mesh-peer/README.md)
- [examples/full-node/README.md](examples/full-node/README.md)
- [examples/moq-sync/README.md](examples/moq-sync/README.md)

## Build

```bash
cd /path/to/primadb/packages/primadb-node
pnpm install
pnpm run build
```

## Example

```js
import { Primadb, derivePasswordKey } from "primadb-node";

const db = new Primadb("node-a");
const key = derivePasswordKey("correct horse battery staple", {
  memoryCostKiB: 64 * 1024,
  timeCost: 3,
  parallelism: 1,
});
db.setSnapshotEncryptionKey(key.keyBase64);
db.openDurableStorage({
  kind: "segment_files",
  directory: "/tmp/primadb-node-demo",
  durability: "full",
  lockMode: { kind: "exclusive" },
});
db.openBlobStorage({
  kind: "files",
  directory: "/tmp/primadb-node-demo-blobs",
  durability: "full",
});

db.chain("notes").field("items").set({
  title: "Node note",
  body: "Stored through the native addon",
  createdAt: new Date().toISOString(),
});
db.scope("ledger").configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "native:node-a" },
});
db.scope("ledger").transaction([
  {
    kind: "increment",
    path: { anchor: "alice", segments: ["balance"] },
    by: 10,
  },
]);
db.chain("assets").field("avatar").putBytes(Buffer.from([1, 2, 3, 4]));
await db
  .chain("assets")
  .field("archive")
  .putBlob(Buffer.from([5, 6, 7, 8]), "application/octet-stream");

db.putRecord("agentfs/inodes/1", { mode: "file", size: 4 });
db.putRecordBytes("agentfs/chunks/1/000000", Buffer.from([1, 2, 3, 4]));
const chunks = db.scanRecords({ prefix: "agentfs/chunks/1/", limit: 100 });
console.log(chunks.entries.length);
db.applyRecordBatch({
  preconditions: [{ kind: "exists", key: "agentfs/inodes/1" }],
  mutations: [],
});
db.syncStorage();

const scriptPath = { anchor: "notes", segments: ["scripted"] };
const scriptCapabilities = {
  read: [{ root: "notes", recursive: true }],
  write: [{ root: "derived", recursive: true }],
  transaction: [{ root: "derived", recursive: true }],
};
db.chain("notes").field("scripted").put({ title: "Scripted note" });
db.attachNodeScript(scriptPath, {
  id: "derive-title",
  source: `
    fn main(ctx) {
      let note = db_get("notes/scripted");
      db_put("derived/scripted", #{ title: note.title, source: ctx.path.display });
      return #{ title: note.title };
    }
  `,
  capabilities: scriptCapabilities,
});
db.executeNodeScripts(scriptPath, { capabilities: scriptCapabilities });

db.setNetworkHooks({
  onPull(context) {
    if (context.verifiedIdentity?.alias === "team-a") {
      return undefined;
    }
    if (context.request.kind === "get" && context.request.path.anchor === "private") {
      return "private root denied";
    }
  },
  onServeResult(_context, result) {
    if (result.kind === "get") {
      return { kind: "get", value: { masked: true } };
    }
  },
});

db.closeDurableStorage();
```

## Smoke Tests

```bash
pnpm run smoke:core
pnpm run smoke:hooks
pnpm run smoke:relay-server
pnpm run smoke:relay
pnpm run smoke:mesh
```
