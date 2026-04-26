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

## Build

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
pnpm install
pnpm run build
```

## Example

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
```

## Threaded Build

Use `primadb/threads` when you want the threaded browser runtime. It still inherits the
`wasm-threads` runtime constraints.

## Package Examples

- [default-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/default-notes)
- [threaded-mesh](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/threaded-mesh)
- [binary-stream-room](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/examples/binary-stream-room)
