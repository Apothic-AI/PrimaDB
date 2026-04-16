---
title: Gun-Compatible Browser Runtime
sidebar_position: 4
---

PrimaDB includes a Gun-compatible browser runtime layered on top of its own core model and
transport stack.

Source:

- [js/primadb-gun.js](https://github.com/Apothic-AI/PrimaDB/tree/master/js/primadb-gun.js)

## What It Covers

- `get`
- `put`
- `set`
- `on`
- `once`
- `open`
- `load`
- `map`
- `then`
- `back`
- `not`
- `user()` flows
- SEA-style browser crypto helpers

## Important Boundary

PrimaDB is not wire-compatible with Gun peers. The runtime is Gun-like on top of PrimaDB’s own
protocol and merge model.

## Example

```ts
import initPrimadbGun from "primadb/gun";

const Gun = await initPrimadbGun();
const gun = Gun({
  peers: ["ws://127.0.0.1:9010/gun"],
});
```

## Runnable Example

- [browser-gun-notes](https://github.com/Apothic-AI/PrimaDB/tree/master/examples/browser-gun-notes)
