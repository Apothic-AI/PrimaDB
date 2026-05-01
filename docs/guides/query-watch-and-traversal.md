---
title: Query, Watch, And Traversal
sidebar_position: 5
---

PrimaDB has local queries, local subscriptions, remote watches, lexical scans, and bounded graph
traversal. This guide explains when to use each.

## Local Query

Use local query APIs when the data is already in the local replica.

```ts
const results = db.chain("notes").field("items").query({
  filters: [{ kind: "prefix", path: "title", value: "Release" }],
  order: { path: "createdAt", direction: "desc" },
  limit: 20,
});
```

The current storage engine supports nested scalar indexes and bounded direct-index scans for common
set-backed query paths.

## Local Subscription

Use local subscriptions for reactive UI state based on local graph state.

Browser:

```ts
const subscription = db.chain("notes").field("items").on((snapshot) => {
  renderNotes(snapshot);
});
```

Node:

```ts
const sub = db.chain("notes").field("items").subscribe();
const initial = await sub.next();
```

Python uses `subscribe()` with `next()` / `try_next()`. Subscriptions emit an initial snapshot and
then updated snapshots when relevant touched paths change.

## Remote Watch

Use remote watches when another peer may have the data and you want updates over relay or mesh.

```ts
const watch = sync.watchRemoteQuery("native:peer-a", {
  anchor: "notes",
  segments: ["items"],
}, {
  filters: [{ kind: "exists", path: "title" }],
  limit: 50,
});
```

Remote watches stream an initial result and then updates. Large responses are chunked.

## Lexical Scan

Use lexical scans for ordered path/value traversal:

```ts
const entries = db.chain("notes").field("items").scan({
  prefix: "release",
  limit: 25,
});
```

Remote lexical watches use the same idea over transport.

## Graph Traversal

Use traversal for link/set-member walks.

```ts
const result = db.chain("people").field("alice").traverse({
  maxDepth: 2,
  direction: "outbound",
  includeValues: true,
});
```

Traversal is local-first and bounded. When relay or mesh transports are active, missing linked nodes
can be scheduled for background peer fetch without forcing a full graph sync first.

Prefer `watchTraverse(...)` for UI flows:

Browser:

```ts
const watch = db.chain("people").field("alice").watchTraverse(
  {
    maxDepth: 2,
    includeValues: true,
  },
  (result) => renderGraph(result),
);
```

Node:

```ts
const watch = db.chain("people").field("alice").watchTraverse({
  maxDepth: 2,
  includeValues: true,
});
```

Python uses `watch_traverse(...)`. The watch receives updated traversal results as fetched nodes
merge into the local graph.

## Performance Notes

- Query and watch invalidation tracks touched paths so unrelated writes avoid broad recomputation.
- Remote watches hash result content to avoid needless repeated sends.
- Use `limit` on query, lex, and traversal requests whenever possible.
- Use blob storage for large binary payloads rather than putting large media frames into every query
  result shape.

See also:

- [Query and watch](../concepts/query-and-watch)
- [Routing and mesh](../concepts/routing-and-mesh)
- [Browser runtime API](../api/browser-runtime)
