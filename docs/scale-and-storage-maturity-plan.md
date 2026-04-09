# Scale And Storage Maturity Plan

This document lays out the next storage and scalability phase for Primadb.

The immediate reason for this plan is a real architectural gap versus Gun's stronger storage path:

- Primadb's current adapter boundary is snapshot-centered.
- Gun's storage path can answer targeted reads and range queries from persistence without rehydrating the full database first.
- Primadb already has remote `get` / `query` / `lex` / `snapshot` over the wire, but the storage layer beneath it still wants full snapshots and replay.

This plan closes that gap without copying Gun's internals or its experimental `Book` rewrite 1:1.

## Summary

Primadb should evolve from:

- snapshot load + op-log replay
- whole-database materialization before serving reads
- high-level storage adapters that only know `load_snapshot()` and `flush(...)`

to:

- an incremental storage engine with targeted reads and range scans
- page/segment-oriented persistence
- pushdown for remote pull and query workloads
- tiered indexes for lexical and ordered traversal
- crash-safe manifests, journals, and compaction
- browser and native backends that share the same logical storage contract

The right shape is not a direct port of Gun `Book`.
The right shape is a Primadb-native page/indexed graph store that borrows the good ideas:

- ordered keyspace
- page splitting
- lazy range reads
- chunked iteration
- adapter-friendly low-level persistence

while keeping Primadb's strengths:

- typed Rust data structures
- explicit operations
- deterministic merge semantics
- signed/delegated field auth
- clean WASM support

## Current State

Today the storage contract in `src/storage.rs` is:

- `load_snapshot() -> Option<DatabaseSnapshot>`
- `flush(ops, snapshot)`

That is simple and correct, but it has hard limits:

- startup cost grows with full snapshot size and full log length
- targeted remote reads still depend on in-memory materialization
- query pushdown into storage is minimal
- compaction is coarse
- browser persistence is mostly whole-snapshot oriented
- large datasets pay too much deserialize/replay cost before useful work begins

The current RADisk-style adapter is therefore best understood as a durability layer, not as a mature storage engine.

## Goals

- Support targeted node, field, path, and lexical reads directly from storage.
- Support incremental range scans for `lex`, ordered queries, and remote pull.
- Preserve Primadb's current merge semantics and auth rules.
- Make native and browser storage engines share one logical contract.
- Keep the current developer-facing API stable wherever practical.
- Improve cold start, memory footprint, and large-dataset behavior.
- Preserve the simple snapshot path as a compatibility and debugging tier.

## Non-Goals

- Do not port Gun `Book` or RADisk serialization directly.
- Do not adopt Gun's loose JS-centric internal encodings.
- Do not make storage responsible for conflict resolution policy.
- Do not require threaded WASM for correctness.
- Do not block current examples or the simple storage path while the new engine matures.

## Design Principles

1. Keep the logical model explicit.
Primadb should continue storing explicit operations and materialized state, not fall back to opaque event routing.

2. Separate logical storage from physical storage.
The engine should define logical reads/writes once, then map them onto filesystem, IndexedDB, OPFS, or memory.

3. Make reads incremental by default.
Remote pull, lex traversal, and query should be able to stop early and stream.

4. Keep storage typed.
Use versioned Rust-owned encodings instead of custom string grammars.

5. Keep auth close to the data.
Owned-field signatures and delegated certificates must survive storage/index projection cleanly.

6. Treat snapshots as a tool, not the primary engine boundary.
Snapshots remain useful for export/import, debugging, fast clone, and tests.

## Proposed Architecture

### 1. Split The Storage Contract

Introduce a lower-level contract beneath today's `StorageAdapter`.

Proposed logical layers:

- `DurableStore`
  - page/segment persistence
  - manifest, journal, checksum, listing
- `StateStore`
  - materialized field and set-member records
  - key lookup and range scan
- `IndexStore`
  - lexical and ordered secondary indexes
  - optional at first, required for mature query performance
- `SnapshotStore`
  - compatibility/debug/export path
  - may be implemented on top of the lower layers

The current `StorageAdapter` can remain temporarily as a compatibility wrapper while the engine migrates.

### 2. Introduce A Canonical Storage Keyspace

Primadb needs a stable internal keyspace for page-oriented persistence.

Suggested logical key families:

- `n/{node_id}/f/{field}` for materialized fields
- `n/{node_id}/s/{field}/{member_id}` for set membership
- `o/{op_id}` for journaled operations
- `r/{owner}/{path}` for auth ownership metadata
- `c/{cert_id}` for certificates and delegated-authority material
- `i/{index_name}/{encoded_term}/{node_id}` for secondary indexes
- `m/...` for manifests and compaction metadata

This gives us:

- prefix/range scans
- deterministic chunking
- storage-level pull by path family
- a clean place for auth metadata without overloading user fields

### 3. Add A Journal + Segment Model

The mature engine should use:

- append-only journal segments for durability
- background materialization into sorted segments/pages
- periodic compaction into larger immutable segments
- a manifest that tracks live segments, generations, and index state

That gives Primadb:

- crash recovery from the journal
- fast append on write
- efficient ordered reads after compaction
- a native place for chunking and streaming

This is closer to an LSM/page-index hybrid than to a literal `Book` port.

### 4. Add Read Pushdown

Today remote pull and local query live mostly above storage.
The new engine should support storage-backed primitives such as:

- get exact field
- get full node
- scan node prefix/path prefix
- scan ordered field index by range
- scan set members incrementally
- fetch auth metadata for a path

That allows:

- remote `get` without full DB materialization
- remote `lex` and `query` streaming from disk
- partial hydration in the browser
- better memory behavior for large result sets

### 5. Add Secondary Indexes

Primadb's current query layer is correct, but mature scale needs index support.

Recommended index classes:

- lexical path index
- scalar equality index
- ordered scalar index
- owner/auth index
- set membership index

Index rollout should be staged:

- first support explicit system indexes
- then add automatic indexes for high-value paths
- finally add planner hints or adaptive index creation

### 6. Make Auth First-Class In Storage

Data-level auth is already in the core model.
The storage engine must preserve it as storage-native metadata.

Required capabilities:

- load and verify signed field payloads without reconstructing unrelated state
- fetch path ownership metadata by prefix
- fetch delegation certificates by issuer/target/path scope
- support index filtering that respects ownership/cert rules
- record auth-verification status in storage caches without mutating canonical data

This keeps SEA-style trust semantics tied to stored data rather than to the transport wrapper.

### 7. Keep A Browser-Native Path

The mature design must not assume native filesystems.

Browser tiers should be:

- Tier 0: current snapshot + IndexedDB path
- Tier 1: IndexedDB segment/journal backend
- Tier 2: OPFS backend where available
- Tier 3: worker-assisted compaction and query pushdown

The browser engine should keep:

- full correctness on single-threaded stable WASM
- optional acceleration via workers / `wasm-threads`
- predictable fallback behavior when advanced storage APIs are absent

### 8. Keep A Native-Native Path

Native tiers should be:

- Tier 0: current snapshot file and append-log adapter
- Tier 1: segment manifest + journal backend
- Tier 2: background compaction and index build workers
- Tier 3: larger-scale operational tooling, metrics, repair, and benchmarking

## Why Not Port Gun Book Directly

Gun `Book` has good ideas:

- sorted pages
- prefix lookup
- page splits
- lazy parsing

But it also carries costs Primadb should avoid:

- JS-specific internal assumptions
- string/escape-heavy encodings
- many edge-case TODOs around split/read/update behavior
- tight coupling to an experimental Gun RAD rewrite

The correct move is to adopt the storage ideas, not the implementation.

## API And Compatibility Strategy

User-facing Primadb APIs should stay stable where possible.

The major changes should be internal first:

- add a new incremental storage engine behind feature flags or internal adapters
- preserve snapshot import/export
- preserve current browser and native examples
- preserve current wire protocol shapes

Then expand capabilities behind existing APIs:

- make `remoteGet` and `remoteLex` hit storage-backed pull paths
- make `chain.lex()` and query builders use indexes when present
- allow partial hydration for large datasets

## Execution Plan

### Phase 0: Planning And Contracts

- Define the new logical storage traits.
- Define the canonical storage keyspace.
- Define versioned record encodings.
- Define manifest, segment, and journal metadata.
- Define corruption-detection and recovery rules.

Exit criteria:

- design doc finalized
- internal trait signatures agreed
- migration path from current `StorageAdapter` documented

### Phase 1: Compatibility Layer

- Keep current `StorageAdapter`.
- Add an internal adapter shim that can expose snapshot-based storage through the new engine traits.
- Add record encoders/decoders and storage-key utilities.

Exit criteria:

- no user-visible behavior change
- snapshot adapters work through the new engine shim

### Phase 2: Native Journal + Segment Backend

- Implement append-only journal segments.
- Implement manifest tracking.
- Implement exact-key reads and prefix scans.
- Implement crash recovery from manifest + journal replay.

Exit criteria:

- native backend can load without full snapshot materialization
- direct field/node fetch works from storage
- corruption and partial-write tests pass

### Phase 3: Query Pushdown And Pull Pushdown

- Route remote `get` / `lex` / `query` through storage-backed primitives.
- Add chunked iterators directly from the backend.
- Add ordered range scan support for indexed fields.

Exit criteria:

- remote pull works without fully loading the DB into memory
- large query responses stream incrementally
- memory footprint during pull/query drops materially in benchmarks

### Phase 4: Browser Segment Backend

- Implement IndexedDB segment/journal backend.
- Add manifest and compaction in browser-safe chunks.
- Keep explicit fallback to snapshot path.

Exit criteria:

- same logical tests pass on native and browser backends
- browser large-dataset reload is materially faster than snapshot-only path

### Phase 5: Secondary Indexes

- Implement explicit index definitions.
- Back lexical and ordered query paths with secondary indexes.
- Add index consistency checks and rebuild tools.

Exit criteria:

- query planner can choose index-backed path
- lex/range workloads show clear benchmark wins

### Phase 6: Operational Maturity

- metrics and stats for cache hit rate, compaction, segment count, replay time
- integrity checker and repair tooling
- import/export between snapshot and segment formats
- workload benchmarks and regression gates

Exit criteria:

- storage backend is observable, testable, and supportable

## Testing Plan

The new engine should be held to a higher bar than the current adapter path.

Required test classes:

- record encode/decode round trips
- exact key and range scan correctness
- compaction correctness under concurrent writes
- crash recovery with torn manifest and torn segment scenarios
- auth metadata and certificate lookup correctness
- browser reload and offline persistence tests
- end-to-end remote `get` / `lex` / `query` from persisted data
- large-dataset benchmarks on native and browser targets

Important invariants:

- materialized reads match snapshot semantics
- merge outcomes do not change
- signed owned fields survive compaction unchanged
- delegated writes remain verifiable after reload
- chunk ordering and dedupe remain stable across relays and peers

## Benchmarks To Add

- cold start from 10k, 100k, and 1M logical records
- exact node fetch latency
- prefix scan latency
- ordered range query latency
- remote query memory footprint
- browser reload time with IndexedDB segments
- compaction throughput
- journal replay throughput

## Risks

- introducing two storage paths increases maintenance burden during migration
- index consistency bugs can create subtle stale-read failures
- browser backends have very different performance profiles than native filesystems
- auth metadata pushdown can become too coupled to query planning if designed poorly
- overly aggressive compaction may fight with sync/persistence hooks

## Recommended Near-Term Priority

The first real implementation priority should be:

1. new internal storage traits
2. canonical storage keyspace
3. native journal + exact/prefix reads
4. storage-backed remote pull
5. browser segment backend
6. secondary indexes

This ordering gets the biggest practical win fastest:

- better startup behavior
- better remote pull scalability
- better query scalability
- no immediate dependency on a full query planner rewrite

## Decision

Primadb should pursue a storage maturity rewrite.

It should not port Gun `Book` directly.
It should instead build a Primadb-native incremental storage engine that:

- narrows the real architectural gap with Gun
- supports current Primadb features cleanly
- improves scale without importing Gun's unstable internals
- creates a durable foundation for the next production-hardening pass
