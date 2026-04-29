---
title: Node Package
sidebar_position: 2
---

`primadb-node` is the native Node package. It wraps the Rust runtime through a Node addon instead of
going through the browser WASM path.

Source:

- [packages/primadb-node](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node)

Full API reference:

- [Node package API](../api/node-package)

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
pnpm install
pnpm run build
```

## Surface

- local graph operations
- durable native storage
- native blob storage
- bytes helpers
- subscriptions
- native relay server hosting
- relay transport
- WebRTC mesh transport
- remote watches
- local transactions and strict scope policies
- network hooks
- experimental MoQ sync helpers through `primadb-node/moq`

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

## Transactions And Strict Scopes

```js
const ledger = db.scope("ledger");
ledger.configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "native:node-a" },
  offlineWrites: "reject",
});

const report = ledger.transaction([
  {
    kind: "increment",
    path: { anchor: "alice", segments: ["balance"] },
    by: 10,
  },
]);
console.log(report.status);
```

When a different peer is the authority, submit over a relay sync client:

```js
const sync = await db.connectRelay({ url: "ws://127.0.0.1:9010" });
await sync.remoteTransaction("native:ledger", "ledger", [
  {
    kind: "increment",
    path: { anchor: "alice", segments: ["balance"] },
    by: 10,
  },
]);
```

## Package Examples

- [local-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/local-notes)
- [mesh-peer](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/mesh-peer)
- [full-node](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/full-node)
- [moq-sync](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/examples/moq-sync)
