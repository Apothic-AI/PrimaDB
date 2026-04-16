---
title: Node Package
sidebar_position: 2
---

`primadb-node` is the native Node package. It wraps the Rust runtime through a Node addon instead of
going through the browser WASM path.

Source:

- [packages/primadb-node](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node)

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run build
```

## Surface

- local graph operations
- durable native storage
- native blob storage
- bytes helpers
- subscriptions
- relay transport
- WebRTC mesh transport
- remote watches
- network hooks

## Example

```js
import { Primadb } from "primadb-node";

const db = new Primadb("node-a");
db.openDurableStorage({
  kind: "segment_files",
  directory: "/tmp/primadb-node-demo",
});

db.setNetworkHooks({
  onPull(context) {
    if (context.request.kind === "get" && context.request.path.anchor === "private") {
      return "private root denied";
    }
  },
});
```

## Package Examples

- [local-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/local-notes)
- [mesh-peer](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/mesh-peer)
