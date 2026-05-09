---
title: Browser Quickstart
sidebar_position: 3
---

PrimaDB supports two browser build profiles:

- default WASM build
- opt-in threaded WASM build

The default build is the compatibility-first path. The threaded build is the performance path.

## Default Browser Flow

Build:

```bash
cd /path/to/primadb
./build-wasm.sh
```

Minimal usage:

```js
import init, { Primadb } from "./pkg/primadb.js";

await init();

const db = new Primadb("browser-a");
db.useBrowserStorage("primadb-demo");

db.chain("users").field("alice").put({
  name: "Alice",
  profile: { timezone: "America/New_York" },
});
```

For larger browser-local datasets, prefer OPFS segment storage when supported:

```js
await db.openDurableStorage({
  kind: "opfs_segments",
  directory: "primadb-app",
  namespace: "main",
});
```

## Threaded Browser Flow

Build:

```bash
cd /path/to/primadb
./build-wasm-threads.sh
```

Bootstrap:

```js
import init, * as primadb from "./pkg/primadb.js";

await init();
await primadb.initThreadPool(Math.max(2, navigator.hardwareConcurrency || 4));

const db = new primadb.Primadb("threaded-browser");
```

## Relay And Mesh

Relay:

```js
const relay = db.connectRelay({
  url: "ws://127.0.0.1:9010",
  retryIntervalMs: 1500,
});
```

Mesh:

```js
const mesh = db.connectMesh({
  room: "demo-room",
  relayUrl: "ws://127.0.0.1:9010",
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
});
```

PrimaDB core does not hard-code STUN servers. The examples explicitly provide them.

## Best Examples

- [browser-relay-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/examples/browser-relay-notes)
- [browser-mesh-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/examples/browser-mesh-notes)
- [browser-threaded-mesh-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/examples/browser-threaded-mesh-notes)
- [browser-package-notes-vite](https://github.com/Apothic-AI/PrimaDB/tree/master/examples/browser-package-notes-vite)
