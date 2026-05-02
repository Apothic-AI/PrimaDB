---
title: TypeScript Package
sidebar_position: 1
---

The in-repo TypeScript package is browser-first and wraps the Rust/WASM browser runtime instead of
reimplementing PrimaDB in TypeScript.

Source:

- [packages/primadb](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb)

Full API reference:

- [Browser TypeScript package API](../api/browser-typescript)
- [Browser runtime API](../api/browser-runtime)
- [Threaded browser package API](../api/browser-threads)

## Entry Points

- `primadb`
- `primadb/threads`
- `primadb/gun`
- `primadb/moq`

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
pnpm install
pnpm run build
```

## Example

```ts
import { Primadb, derivePasswordKey, initPrimadb, setNetworkHooks } from "primadb";

await initPrimadb();

const db = new Primadb("browser-a");
const key = derivePasswordKey("correct horse battery staple");
db.setSnapshotEncryptionKey(key.keyBase64);

setNetworkHooks(db, {
  onPull(context) {
    if (context.request.kind === "get" && context.request.path.anchor === "private") {
      return "private root denied";
    }
    return undefined;
  },
});
```

## Transactions And Strict Scopes

The browser runtime exposes the same step-based transaction payloads as the native packages:

```ts
db.transaction([
  {
    kind: "put",
    path: { anchor: "drafts", segments: ["welcome"] },
    value: { title: "Welcome" },
  },
]);

db.scope("ledger").configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "browser-ledger" },
  offlineWrites: "queue_provisional",
});

const report = db.scope("ledger").transaction([
  {
    kind: "increment",
    path: { anchor: "alice", segments: ["balance"] },
    by: 10,
  },
]);

if (report.status === "provisional") {
  console.log(db.scope("ledger").proposals());
}
```

Relay clients can submit strict-scope transactions to an authority peer with
`remoteTransaction(...)`. Mesh transports currently expose remote watches, not a public remote
transaction helper.

## Threaded Build

Use `primadb/threads` when you want the threaded browser runtime. It still inherits the
`wasm-threads` runtime constraints.

## Guides

- [Auth, encryption, and password keys](../guides/auth-encryption)
- [Relay, full node, and mesh](../guides/relay-full-node-and-mesh)
- [Query, watch, and traversal](../guides/query-watch-and-traversal)
- [Binary data, media, and MoQ](../guides/binary-media-and-moq)
- [Node-attached scripting](../guides/scripting)

## Package Examples

- [default-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/default-notes)
- [threaded-mesh](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/threaded-mesh)
- [binary-stream-room](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/binary-stream-room)
- [text-voice-chat](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/text-voice-chat)
- [moq-sync](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/moq-sync)
