# `primadb-node`

`primadb-node` is a native Node.js package for Primadb. Unlike the browser package in
[packages/primadb](/home/bitnom/Code/gunport/primadb/packages/primadb), this package wraps the
native Rust runtime directly through a Node addon.

Current surface:

- `Primadb` and `Chain` for local graph operations
- `Scope` and step-based transactions for local ACID writes and coordinated strict-scope proposals
- durable native storage through `openDurableStorage(...)`
- content-addressed native blob storage through `openBlobStorage(...)`
- first-class binary helpers through `putBytes()`, `onceBytes()`, `putBlob()`, and `getBlob()`
- subscriptions
- native relay server hosting through `RelayServer.listen(...)`
- native relay sync through `connectRelay(...)`, including disconnected startup with background relay retry
- remote strict-scope transaction submission through `remoteTransaction(...)` on relay sync clients
- native WebRTC mesh through `connectMesh(...)`, including disconnected startup with background relay retry
- live remote watches through `watchRemoteGet(...)`, `watchRemoteMap(...)`, `watchRemoteQuery(...)`, `watchRemoteLex(...)`, and `watchRemoteSnapshot(...)`
- authenticated relay/mesh session identity through `generateIdentity()`, `authenticateLocalUser(...)`, `sessionAuth` config, and `context.verifiedIdentity`
- network-boundary hooks through `setNetworkHooks(...)` / `clearNetworkHooks()`
- experimental MoQ sync helpers through `primadb-node/moq`

## Package Examples

Runnable package-local examples live under [examples/](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples):

- [examples/local-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/local-notes/README.md)
- [examples/mesh-peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/mesh-peer/README.md)
- [examples/full-node/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/full-node/README.md)
- [examples/moq-sync/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/moq-sync/README.md)

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
pnpm install
pnpm run build
```

## Example

```js
import { Primadb } from "primadb-node";

const db = new Primadb("node-a");
db.openDurableStorage({
  kind: "segment_files",
  directory: "/tmp/primadb-node-demo",
});
db.openBlobStorage({
  kind: "files",
  directory: "/tmp/primadb-node-demo-blobs",
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
```

## Smoke Tests

```bash
pnpm run smoke:core
pnpm run smoke:hooks
pnpm run smoke:relay-server
pnpm run smoke:relay
pnpm run smoke:mesh
```
