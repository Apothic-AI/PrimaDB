# PrimaDB Vector Support Sprint Plan

## Goal

Implement first-class vector storage and search for PrimaDB using the agreed two-layer design:

- Authoritative vector facts are PrimaDB keyed records, so they sync through existing graph/record ops, relay, and mesh.
- Exact and ANN indexes are local acceleration caches derived from authoritative records.

No vector mutation replication protocol is introduced. ANN topology is never authoritative state.

## Authoritative Layout

Use chunk-compatible record keys from the first implementation:

```text
__primadb_vectors/{collection}/meta
__primadb_vectors/{collection}/items/{id}/meta
__primadb_vectors/{collection}/items/{id}/chunks/{n}
```

Collection and item IDs are encoded storage components. MVP unchunked vectors use one chunk at `chunks/0`.

Every item write uses a shared `writeId`. The vector manager indexes an item only when item metadata and all required chunks exist, every chunk has the same `writeId`, byte lengths match, checksums match, and decoded dimensions match collection metadata.

Deletes write a tombstone item metadata record with a new `writeId` and `deleted: true`. Missing chunks without a tombstone mean incomplete, not deleted.

## Runtime Design

PrimaDB owns a backend trait and mandatory exact backend. EdgeVec integration is feature-gated behind the backend boundary.

Implementation update:

- Exact search remains mandatory and independent.
- With `vector-edgevec`, EdgeVec HNSW is active for `backend: "edgevec"` searches when `exact: false` and no correctness-sensitive filter requires exact fallback.
- EdgeVec IDs remain private. PrimaDB maps EdgeVec `VectorId` values back to stable logical vector IDs and recalculates public distances from exact cache entries before returning matches.

The first local runtime keeps search-friendly exact state:

- packed f32 vector slab
- logical key to slot map
- slot to logical key map
- metadata side table
- tombstone and incomplete tracking
- collection manager state

The manager lazily rebuilds dirty collections from authoritative records and keeps serving based on `stalePolicy`.

## Persistent Caches

Native cache path beside segment storage:

```text
vector-cache/{collection}/manifest.json
vector-cache/{collection}/vectors.f32
vector-cache/{collection}/keys.bin
vector-cache/{collection}/metadata.bin
vector-cache/{collection}/backend.edgevec
```

Browser OPFS cache path:

```text
primadb-vector-cache/{db_id}/{collection}/manifest.json
primadb-vector-cache/{db_id}/{collection}/vectors.f32
primadb-vector-cache/{db_id}/{collection}/keys.bin
primadb-vector-cache/{db_id}/{collection}/metadata.bin
primadb-vector-cache/{db_id}/{collection}/backend.edgevec
```

Implementation update:

- Native segment storage writes and reads `vector-cache/{collection}` files.
- Native vector slab reads use mmap-backed loading and deterministic source-hash validation.
- Browser OPFS exposes explicit async cache helpers because OPFS is async while core vector search is synchronous:
  - `saveVectorCacheOpfs(directory, namespace, collection)`
  - `loadVectorCacheOpfs(directory, namespace, collection)`
- Cache import never mutates authoritative records and is accepted only when manifest/config/backend/source-hash validation succeeds.

## Public API

Rust:

```rust
db.create_vector_collection(name, config)
db.put_vector(collection, id, vector, metadata)
db.delete_vector(collection, id)
db.get_vector(collection, id)
db.search_vectors(collection, query, spec)
db.watch_vector_search(collection, query, spec)
```

Equivalent WASM/browser, Node, and Python bindings should be exposed after the core Rust API compiles and tests pass.

Implementation update: Rust, WASM/browser, Node, and Python bindings are exposed for local vector APIs and remote vector search/watch APIs. Browser WASM additionally exposes OPFS vector cache load/save helpers.

## Remote Search

Add `VectorSearch` to pull/watch protocol variants and route it through existing request authorization, served-result filtering, chunking, watch recompute, and remote-interest APIs.

Vector search capability strings:

- `pull_vector_search`
- `watch_vector_search`
- `vector_exact`
- `vector_metric:cosine`
- `vector_metric:l2`
- `vector_metric:dot`
- `vector_ann:edgevec`
- `vector_collection:{encodedCollection}:{dim}:{metric}:{state}:{backend}`

Peer selection should prefer ready peers with matching collection metadata when that information is advertised.

## Verification

Required tests:

- split-record completeness and out-of-order chunk handling
- exact search correctness for cosine, L2, and dot
- metadata and ID filters
- vector watches emit initial and changed results only
- remote vector search and watch protocol conversion
- payload coverage for 384, 768, and 1536 dimensions
- wasm32 compile check for core vector types
