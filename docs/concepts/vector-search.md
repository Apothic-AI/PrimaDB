# Vector Storage And Search

PrimaDB vector support uses two layers:

- Authoritative vector data is stored as normal keyed records under `__primadb_vectors/...`.
- Search indexes are local acceleration caches derived from those records.

Vector writes sync through existing graph/record operations. There is no separate vector mutation protocol and no replicated ANN topology.

## Record Layout

Each collection has metadata:

```text
__primadb_vectors/{collection}/meta
```

Each item is split into metadata plus chunk records:

```text
__primadb_vectors/{collection}/items/{id}/meta
__primadb_vectors/{collection}/items/{id}/chunks/{n}
```

Collection and item IDs are storage-component encoded. A one-chunk vector still uses `chunks/0`.

Every item write has a shared `writeId`. Search indexes only accept an item when metadata and all chunks exist, every chunk has the same `writeId`, byte length and checksums match, and decoded dimensions match the collection config. Partial sync state is treated as incomplete, not deleted.

## APIs

Rust, WASM, Node, and Python expose:

```text
create_vector_collection / createVectorCollection
put_vector / putVector
delete_vector / deleteVector
get_vector / getVector
search_vectors / searchVectors
watch_vector_search / watchVectorSearch
```

Search specs support `limit`, `ef`, `filter`, `includeVector`, `includeMetadata`, `exact`, and `stalePolicy`.

The mandatory backend is exact search over the local derived cache. The optional `vector-edgevec` Cargo feature enables EdgeVec HNSW as an ANN backend behind PrimaDB's backend boundary. EdgeVec IDs and topology stay private cache state; PrimaDB still returns stable logical vector IDs.

Filtered searches route to the exact cache unless an ANN backend can guarantee filtered top-k correctness. That avoids oversample/post-filter results being presented as exact.

## Persistent Caches

Native segment storage writes and reads validated vector cache files beside the segment store:

```text
vector-cache/{collection}/manifest.json
vector-cache/{collection}/vectors.f32
vector-cache/{collection}/keys.bin
vector-cache/{collection}/metadata.bin
vector-cache/{collection}/backend.edgevec
```

The native vector slab is loaded with an mmap-backed read path and accepted only when the manifest matches collection config, backend version, cache format, and the deterministic source hash of authoritative vector records.

Browser OPFS cache helpers use the same file format:

```text
saveVectorCacheOpfs(directory, namespace, collection)
loadVectorCacheOpfs(directory, namespace, collection)
```

OPFS cache import also validates against authoritative records before installing the cache. If validation fails, authoritative records remain untouched and the collection can rebuild normally.

## Remote Search

Remote vector search is a pull/watch request:

```text
PullRequestKind::VectorSearch { collection, query, spec }
```

It uses the same transport, authorization hooks, served-result filtering, chunking, and remote-interest policy path as other remote pulls and watches.

Capability strings:

```text
pull_vector_search
watch_vector_search
vector_exact
vector_metric:cosine
vector_metric:l2
vector_metric:dot
vector_ann:edgevec
vector_collection:{encodedCollection}:{dim}:{metric}:{state}:{backend}
```

Remote results are per-peer derived search results. Authoritative data convergence still comes from normal record sync.

Remote peer selection prefers advertised `vector_collection` hints with matching collection and query dimension. Peers that advertise a mismatched or non-ready collection are skipped unless the caller allows stale results; peers without detailed hints remain usable for compatibility if they advertise the generic vector search capability.
