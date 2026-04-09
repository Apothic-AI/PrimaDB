# `primadb-node`

`primadb-node` is a native Node.js package for Primadb. Unlike the browser package in
[packages/primadb](/home/bitnom/Code/gunport/primadb/packages/primadb), this package wraps the
native Rust runtime directly through a Node addon.

Current surface:

- `Primadb` and `Chain` for local graph operations
- durable native storage through `openDurableStorage(...)`
- subscriptions
- native relay sync through `connectRelay(...)`
- native WebRTC mesh through `connectMesh(...)`

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

db.chain("notes").field("items").set({
  title: "Node note",
  body: "Stored through the native addon",
  createdAt: new Date().toISOString(),
});
```

## Smoke Tests

```bash
npm run smoke:core
npm run smoke:relay
npm run smoke:mesh
```
