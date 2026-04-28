# Lazy Graph Traversal Plan

This document captures the intended direction for first-class graph traversal in PrimaDB.

The key design decision is that traversal should be a normal graph operation, not a separate
remote-specific API. PrimaDB should behave like a local-first graph that can lazily ask peers for
missing graph elements as traversal reaches them.

## Current State

PrimaDB already has traversal-adjacent primitives:

- chain path traversal through `chain(...).field(...)`
- `map` and set-backed `query`
- lexical scans through `LexSpec`
- bounded recursive lexical scans through `LexSpec.follow_links`
- remote transport pull primitives for `get`, `map`, `query`, `lex`, `node`, and `snapshot`

That is enough for useful graph-shaped documents and simple linked scans. The current development
pass adds a first-class bounded traversal layer.

Implemented pieces:

- explicit `TraversalSpec`, `TraversalEntry`, and `TraversalResult` types
- user-facing `chain.traverse(...)` / `chain.watchTraverse(...)`
- node-addressed `PullRequestKind::Node { id }`
- relationship indexes for outbound and inbound links/set-members
- relay and mesh lazy node-fetch schedulers for native and browser WASM transports
- Rust, browser WASM, Node, and Python package API surfaces

Remaining follow-up:

- traversal fetches are intentionally bounded and local-first, so immediate `traverse(...)` results
  can be partial while `watchTraverse(...)` observes fetched data as it arrives

## Product Direction

Traversal should be local-first and peer-assisted by default:

- Start with the local replica immediately.
- Traverse only the graph fragments needed by the traversal spec.
- Do not require full graph sync before traversal begins.
- When traversal reaches a missing linked node or set member, request only that missing graph element
  from connected peers.
- Merge fetched graph fragments into the local replica.
- Continue traversal as data arrives.
- Return metadata that tells the caller whether the result is complete, partial, timed out, or
  stopped by limits.

This aligns with the useful part of Gun's DX: application code traverses the graph, and the runtime
pulls graph fragments on demand.

## API Direction

Do not make `remoteTraverse(...)` or `watchRemoteTraverse(...)` the main API.

Preferred high-level API:

```ts
const result = await db.chain("users").field("alice").traverse({
  direction: "outbound",
  maxDepth: 4,
  limit: 500,
});
```

Preferred watch API:

```ts
const sub = db.chain("users").field("alice").watchTraverse({
  direction: "outbound",
  maxDepth: 2,
  limit: 200,
});
```

On-demand peer fetch should be enabled by default. Options should constrain or disable it, not enable
it:

```ts
const result = await db.chain("users").field("alice").traverse(
  {
    direction: "outbound",
    maxDepth: 4,
    limit: 500,
  },
  {
    fetch: {
      enabled: true,
      peers: "any",
      timeoutMs: 750,
      maxFetches: 128,
      concurrency: 8,
    },
  },
);
```

The exact host-language shape can vary, but the semantics should be shared across Rust, browser,
Node, and Python.

## TraversalSpec

Initial `TraversalSpec` should be deliberately bounded:

- `direction`: `outbound`, `inbound`, or `both`
- `strategy`: `bfs` or `dfs`
- `maxDepth`
- `limit`
- `edgeFields`: optional field allowlist
- `filters`: query-like predicates over shallow materialized node values
- `followSets`: whether set members are traversable edges
- `followLinks`: whether links are traversable edges
- `includeStart`: whether the start node/path appears in output
- `includeValues`: whether shallow node values are included
- `fetchMissing`: whether missing nodes should be scheduled for peer fetch
- `maxFetches`: strict budget for background peer fetch scheduling

Everything must be bounded by default. Unbounded graph traversal should not be a supported default.

## Result Shape

Traversal results should carry status metadata:

```ts
type TraversalResult = {
  entries: TraversalEntry[];
  complete: boolean;
  timedOut: boolean;
  depthLimitReached: boolean;
  resultLimitReached: boolean;
  fetched: number; // number of background node fetches scheduled by this evaluation
  missing: string[];
  denied: string[];
};
```

The important property is not this exact shape. The important property is that callers can distinguish
complete answers from partial local-first answers.

## Protocol Direction

Existing transport-level remote methods should remain as advanced peer-targeted escape hatches.
They should not define the high-level traversal DX.

To support lazy traversal efficiently, the pull protocol should gain node-addressed primitives:

- `PullRequestKind::Node { id }`
- possibly `PullRequestKind::NodeEdges { id, direction, edge_fields }`
- eventually chunked traversal internals if a single peer serves large traversal fragments

Why `Node { id }` matters:

- links are stored as node IDs
- not every linked node is naturally reachable through a root/path request
- traversal should fetch missing linked graph elements directly instead of falling back to full
  snapshot or broad path scans

Fetched fragments should still enter the normal merge path so local indexes, watches, auth checks,
and persistence see ordinary graph state.

## Watch Direction

`watchTraverse(...)` should be implemented as a normal watch over a materialized traversal result.

Requirements:

- reuse touched-path and touched-node invalidation
- include traversal dependencies discovered during the prior evaluation
- update when a fetched node changes
- coalesce refreshes like existing watches
- avoid recomputing unrelated traversals

The watch implementation should not fan out over every write.

## Index Direction

Traversal needs relationship indexes, not just scalar query indexes.

Minimum useful indexes:

- outbound edge index by source node and field
- inbound edge index by target node and field
- set membership edge index

These indexes support:

- neighbors
- inbound references
- bounded BFS/DFS
- efficient dependency tracking for `watchTraverse(...)`

The reverse-edge index is the highest-value addition for traversal because outbound links can be
loaded from the source node, but inbound traversal otherwise requires broad scans.

## Auth And Hooks

Traversal should reuse existing network-boundary hooks and auth behavior:

- peer fetch requests should pass through pull/watch hooks
- served traversal fragments should be redactable or deniable
- encrypted private data should remain private by default
- traversal should never imply a universal graph read ACL engine

For on-demand fetch, a peer may deny or redact a node request. The traversal result should then mark
the node as missing or denied rather than failing the whole traversal by default.

## Cross-Platform Requirement

Traversal is a core graph capability, not a browser or native feature.

The implementation should ship across:

- Rust core
- browser WASM package
- Node package
- Python package
- relay transport
- mesh transport

The method names may follow host conventions, but behavior and result semantics should match.

## Initial Work Plan

1. Add internal traversal types in Rust:
   - `TraversalSpec`
   - `TraversalEntry`
   - `TraversalResult`
2. Implement local bounded outbound traversal in `src/db.rs`.
3. Add relationship index maintenance for outbound and inbound links/set edges.
4. Add `PullRequestKind::Node { id }` and serve/apply it through relay and mesh transports.
5. Add on-demand peer fetch to traversal evaluation, enabled by default with strict budgets.
6. Expose `chain.traverse(...)` in browser, Node, and Python packages.
7. Add `watchTraverse(...)` using traversal dependency tracking.
8. Add examples that traverse a partially replicated graph without snapshot-syncing the whole peer.

## Implementation Notes

- Link and set references should not manufacture empty target nodes. Absent targets are missing
  graph elements; explicit empty objects are valid known nodes.
- Traversal should use shallow value materialization for filters and returned values so a traversal
  does not accidentally recursively materialize the whole reachable graph.
- `watchTraverse(...)` should reuse touched-path dependency tracking and should refresh only when the
  previous traversal's start path, reached nodes, edges, missing nodes, or denied nodes overlap the
  change event.

## Verification Plan

- local traversal returns bounded deterministic results
- traversal handles cycles without looping
- traversal returns partial metadata when limits are hit
- traversal fetches a missing linked node from a peer without full snapshot sync
- denied or unavailable nodes appear as missing/denied metadata
- watch traversal only recomputes when touched nodes or dependency edges overlap
- browser, Node, Python, relay, and mesh examples exercise the same behavior

## Non-Goals

- Do not add broad graph analytics algorithms to core yet.
- Do not make traversal require full graph synchronization.
- Do not make users choose local versus remote traversal methods.
- Do not add unbounded network crawling.
- Do not make remote traversal a separate first-class API family.
