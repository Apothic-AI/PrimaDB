## Watch, Index, and Storage Plan

### Goals

1. Narrow watch invalidation so local and remote watchers do not recompute on unrelated writes.
2. Broaden index pushdown to cover nested scalar paths and cheaper range/prefix scans.
3. Improve native durable-storage hygiene with explicit vacuum/GC and denser hot-path encoding.
4. Prepare watch and index internals for bounded graph traversal without broad recomputation.

### Constraints

- Do not change the logical merge model.
- Do not weaken current sync correctness.
- Keep relay/mesh watch behavior identical at the API level.
- Avoid automatic destructive GC for shared blob stores unless the live set is explicit.
- Do not introduce traversal features that require full graph sync or unbounded network fetches.

### Work Plan

#### 1. Watch Invalidation and Recompute Narrowing

- Add change-scope metadata to change events:
  - touched logical paths
  - full-refresh marker for snapshot-level mutations
- Derive local change scopes from applied operations.
- Store per-subscription interest paths and last delivered hashes.
- Only recompute subscriptions whose interest overlaps the touched path set, unless the event is a full refresh.
- Reuse the same overlap logic for incoming relay/mesh watches.
- Coalesce bursts of queued change events in relay/mesh/watch tasks before recomputing watches.

#### 2. Broader and Cheaper Index Pushdown

- Extend storage indexing from direct scalar fields to nested scalar leaf paths materialized through linked child nodes.
- Expand incremental transaction touch sets to include hierarchical ancestors so parent nested indexes stay current when child nodes change.
- Add bounded direct-index scans:
  - exact
  - prefix
  - lower/upper range
- Use bounded scans plus indexed filter grouping/intersection in query planning.
- Push offset/limit earlier when the indexed scan already determines final ordering.
- Add relationship indexes for traversal:
  - outbound links by source node and field
  - inbound links by target node and field
  - set membership edges

#### 3. Storage Vacuum / Denser Encoding / GC

- Replace pretty-printed JSON writes on native hot storage paths with compact encoding.
- Add explicit native vacuum support for the segment store:
  - stale node files
  - stale auth files
  - stale node-index manifests
  - stale direct-index entries and empty index directories
  - journal pruning
- Add explicit blob-store GC against the current live blob-reference set.
- Expose a Rust-level vacuum entrypoint returning a report so cleanup is explicit and testable.

### Verification

- Extend DB tests for:
  - narrowed local subscriptions
  - indexed nested-path query pushdown
  - range/prefix index scans
  - vacuum removing stale files without removing live state
- Re-run native mesh/relay watch tests to confirm no behavior regression.
- Add traversal-focused tests once traversal lands:
  - bounded traversal over cycles
  - on-demand missing-node fetch
  - reverse-edge traversal from the relationship index
  - watch traversal invalidation only on dependent nodes/edges
