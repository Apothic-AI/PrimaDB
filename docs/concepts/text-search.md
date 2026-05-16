---
title: BM25 Text Search
sidebar_position: 8
---

PrimaDB text search follows the same local-first model as records, watches, and vector search.
Callers can search local data directly, or use relay/mesh handles to let PrimaDB choose reachable
peers through `RemoteInterestPolicy`.

## Record Layout

Authoritative text data is stored as normal keyed records:

```text
__primadb_text/{collection}/config
__primadb_text/{collection}/docs/{doc_id}
```

Collection and document IDs are storage-component encoded. The BM25 index is derived cache state;
normal record sync carries the authoritative collection config and documents.

## APIs

Rust, WASM, Node, and Python expose:

```text
create_text_collection / createTextCollection
put_text_document / putTextDocument
delete_text_document / deleteTextDocument
get_text_document / getTextDocument
text_search / textSearch
watch_text_search / watchTextSearch
text_index_stats / textIndexStats
```

Relay and mesh handles also expose:

```text
text_search_fan_in / textSearchFanIn
watch_text_search_fan_in / watchTextSearchFanIn
```

A collection name is shorthand for `TextSearchSource::Collection`:

```rust
let result = db.text_search("notes", "secure mesh routing", Default::default())?;
```

Query-scoped search is explicit:

```rust
db.text_search(
    TextSearchSource::Records { scan },
    "trust proposal",
    TextSearchSpec::default(),
)?;
```

## Collection Search

Declared collections use PrimaDB's shared exact BM25 implementation. The core analyzer and scorer
are pure Rust and compile for native and `wasm32-unknown-unknown`.

Collection scores use collection-wide corpus statistics and report:

```text
scoreScope = "collection"
```

## Query-Scoped Search

PrimaDB can rank arbitrary graph-query and record-scan candidate sets:

- `TextSearchSource::GraphQuery { path, spec }` ranks materialized `MapEntry` values.
- `TextSearchSource::Records { scan }` ranks materialized `RecordEntry` values.

When `fields` is omitted, PrimaDB extracts all string leaves from JSON values. Binary and blob
records are skipped in the first tranche.

Candidate-set search reports:

```text
candidateCount
searchedCount
truncatedCandidates
scoreScope = "candidate_set"
```

Graph queries with `order`, `offset`, or `limit` are rejected by default because ranking only a
preselected page is not a global top-k result. Set `candidatePolicy` to
`allow_preselected_candidates` when that is the desired behavior; PrimaDB then marks
`truncatedCandidates`.

## Remote Search

Remote text search uses the same pull/watch protocol as other remote interests:

```text
PullRequestKind::TextSearch { source, query, spec }
```

Relay and mesh handles expose ambient helpers plus explicit peer-targeted helpers. Selected peers
execute query-scoped BM25 locally; PrimaDB does not pull every remote candidate to the caller and
rank client-side by default.

Capability strings:

```text
pull_text_search
watch_text_search
text_bm25_exact
text_collection:{encodedCollection}:{state}:{backend}:{analyzerVersion}
```

Generic scores from different peers or different candidate sets are not automatically comparable.
Use `scoreScope`, source metadata, and fan-in diagnostics when merging multi-peer results.
The built-in fan-in merge tags matches with `__primadb_source_peer` and
`__primadb_source_transport` metadata and reports `scoreScope = "peer_local"`.

## Cache Behavior

Authoritative collection configs and documents remain normal records. Native segment storage writes
derived text cache files under `text-cache/{collection}/` next to the vector cache. Browser/WASM
bindings expose `saveTextCacheOpfs(...)` and `loadTextCacheOpfs(...)` for explicit OPFS cache
management. Cache manifests validate collection name, record prefix, config hash, analyzer version,
backend version, and source hash before PrimaDB accepts them.

## Relationship To Other Search

`QueryFilter::Contains` remains a boolean scalar filter. BM25 text search is a ranked retrieval
operation.

Vector search remains the semantic-nearest-neighbor primitive. Text search is exact lexical BM25.
Both use normal records as authoritative data and local derived indexes for acceleration.
