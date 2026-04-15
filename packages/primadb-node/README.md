# `primadb-node`

`primadb-node` is a native Node.js package for Primadb. Unlike the browser package in
[packages/primadb](/home/bitnom/Code/gunport/primadb/packages/primadb), this package wraps the
native Rust runtime directly through a Node addon.

Current surface:

- `Primadb` and `Chain` for local graph operations
- durable native storage through `openDurableStorage(...)`
- content-addressed native blob storage through `openBlobStorage(...)`
- first-class binary helpers through `putBytes()`, `onceBytes()`, `putBlob()`, and `getBlob()`
- subscriptions
- native relay sync through `connectRelay(...)`, including disconnected startup with background relay retry
- native WebRTC mesh through `connectMesh(...)`, including disconnected startup with background relay retry
- live remote watches through `watchRemoteGet(...)`, `watchRemoteMap(...)`, `watchRemoteQuery(...)`, `watchRemoteLex(...)`, and `watchRemoteSnapshot(...)`

## Package Examples

Runnable package-local examples live under [examples/](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples):

- [examples/local-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/local-notes/README.md)
- [examples/mesh-peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/mesh-peer/README.md)

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run build
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
db.chain("assets").field("avatar").putBytes(Buffer.from([1, 2, 3, 4]));
await db
  .chain("assets")
  .field("archive")
  .putBlob(Buffer.from([5, 6, 7, 8]), "application/octet-stream");
```

## Smoke Tests

```bash
npm run smoke:core
npm run smoke:relay
npm run smoke:mesh
```
