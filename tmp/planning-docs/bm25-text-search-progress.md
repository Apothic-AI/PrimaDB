# PrimaDB BM25 Text Search Progress

## 2026-05-15

- Branch: `feat/primadb-bm25`.
- Added shared pure-Rust BM25 text search core in `src/text_search.rs`.
- Added record-backed text collection config/document storage under `__primadb_text/{collection}/...`.
- Added local `Primadb` text APIs: collection CRUD, `text_search`, `watch_text_search`, and `text_index_stats`.
- Added query-scoped BM25 for graph-query and record-scan candidate sets with explicit candidate counts, searched counts, truncation flags, and score scopes.
- Added text cache file manifests, import/export helpers, native segment-storage cache load/write, and browser OPFS save/load helpers.
- Extended route pull/watch protocol with `PullRequestKind::TextSearch`, `PullResponseBody::TextSearch`, and `RemoteResult::TextSearch`.
- Added native WebSocket and MoQ relay text search, watches, explicit remote calls, policy-based ambient calls, fan-in, and fan-in watches.
- Added native WebRTC mesh text watches, policy-based watches, text fan-in, and fan-in watches.
- Added WASM/browser local text APIs, relay/mesh text search, fan-in, watches, and OPFS text cache methods.
- Added Node and Python binding methods and declarations for local, remote, watch, fan-in, and fan-in-watch text search.
- Added text-search presence capabilities: `pull_text_search`, `watch_text_search`, `text_bm25_exact`, and collection state capabilities.
- Added docs:
  - `docs/concepts/text-search.md`
  - README capability updates
  - query/watch concept updates
  - relay/full-node/mesh guide updates
  - package README updates
  - generated API docs
- Verification completed:
  - `cargo check --lib`
  - `cargo check --features "native-websocket native-webrtc native-moq" --lib`
  - `cargo check --target wasm32-unknown-unknown --lib`
  - `cargo check --manifest-path packages/primadb-node/Cargo.toml`
  - `cargo check --manifest-path packages/primadb-python/Cargo.toml`
  - `python -m py_compile packages/primadb-python/python/primadb/__init__.pyi`
  - `cargo test --lib`
  - `cargo test --lib --features "native-websocket native-webrtc native-moq"`
  - `pnpm --dir packages/primadb typecheck`
  - `pnpm run generate:api`
  - `pnpm run build` in `website/`

## Notes

- Tantivy remains intentionally unimplemented in this tranche. The public API and cache boundary are backend-neutral, so a native-only optional backend can be evaluated later without changing caller-facing types.
- Analyzer enhancements beyond the deterministic simple analyzer remain future optional features. Analyzer config is versioned and cache validation includes analyzer version and config hash.
- Fan-in merged text results report `scoreScope = "peer_local"` because peer-local BM25 scores are not a single globally comparable corpus.
