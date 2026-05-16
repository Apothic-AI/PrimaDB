# PrimaDB BM25 Text Search Sprint Plan

## Goal

Add first-class BM25 text search to PrimaDB as a local-first, route-aware, peer-agnostic primitive. The default developer experience should not require callers to choose local versus remote data; PrimaDB should use the same ambient remote-interest and RouteEnvelope machinery already used for records, watches, fan-in, and vector search.

## Direction

- Store authoritative text collections as normal records under `__primadb_text/{collection}/...`.
- Treat BM25 indexes as derived local caches that can rebuild from records.
- Persist declared-collection cache files through native segment storage and explicit browser OPFS helpers.
- Implement a shared pure-Rust exact BM25 backend for native and WASM.
- Support declared collection search plus ad-hoc BM25 over graph-query and record-scan candidate sets.
- Make candidate-set semantics explicit with candidate counts, searched counts, truncation flags, and score scope.
- Extend pull/watch with `TextSearch` so selected peers execute query-scoped BM25 locally.
- Keep Tantivy as an optional future backend study, not a first-tranche dependency.

## Implementation Tranches

1. Shared `text_search` module with types, analyzer, exact BM25 scorer, in-memory index, cache file shape, and tests.
2. Record-backed text collection CRUD in `Primadb`, dirty rebuild behavior, local `text_search`, `watch_text_search`, and stats.
3. Query-scoped BM25 over `TextSearchSource::GraphQuery` and `TextSearchSource::Records`.
4. Pull/watch protocol support: `PullRequestKind::TextSearch`, `PullResponseBody::TextSearch`, and `RemoteResult::TextSearch`.
5. Native WebSocket/MoQ/WebRTC relay and mesh APIs, including ambient policy methods and fan-in helpers.
6. WASM/browser, Node, and Python bindings/types for local, remote, watch, and fan-in text search where each surface already supports equivalent vector/record APIs.
7. Native and browser cache integration for declared text collections.
8. Docs and generated API pages covering local-first ambient search, collection versus candidate-set scoring, cache behavior, and capability names.
9. Focused Rust, SDK, and docs checks.

## Acceptance Criteria

- Plain local DB text search and text watches work.
- Text documents replicate as normal records and indexes rebuild from records.
- BM25 ranks declared collections, graph-query candidates, and record-scan candidates.
- Query-scoped BM25 rejects misleading paginated graph queries by default.
- Remote text search uses existing pull/watch/auth/hook/chunking paths.
- Ambient relay/mesh text search and fan-in preserve source metadata and partial failures.
- Declared text collection caches persist through native segment storage and explicit browser OPFS helpers.
- Native and WASM share the exact BM25 core.
- SDKs expose consistent text search APIs and result shapes.
- Docs distinguish scalar `contains`, vector search, collection BM25, and candidate-set BM25.

## Risks

- BM25 scores across peers or different candidate sets are not globally comparable; `scoreScope` must be surfaced.
- Query/scan pagination can produce misleading top-k results; reject or mark truncation.
- Snippets can leak content through served-result hooks; snippets remain opt-in.
- Large corpora may need a future segment/postings-optimized cache backend; the first cache format is exact and rebuild-safe.
