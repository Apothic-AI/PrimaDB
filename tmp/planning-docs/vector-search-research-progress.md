# Vector Search Research Progress

## Completed

- Reviewed Primadb's current crate metadata and README to confirm native and
  WASM targets, browser persistence, segment-file storage, and scalar
  direct-index support.
- Inspected the current storage engine boundary in `src/engine.rs` and the
  query pushdown path in `src/db.rs` to confirm Primadb currently exposes
  scalar direct-index scans but not a general vector-index abstraction.
- Defined the candidate set and evaluation criteria for crate research.
- Collected current crate metadata from crates.io via `cargo info` for
  `hnsw_rs`, `instant-distance`, `kiddo`, `usearch`, `faiss`, `lance`,
  `lancedb`, `sqlite-vec`, `sqlite-wasm-vec`, `libsql`, and `tantivy`.
- Cross-checked capability claims against upstream docs and READMEs for
  persistence, dynamic update/delete support, metric coverage, filtering, and
  approximate vs exact search behavior.
- Compared embeddable crate options against external-service baselines
  (Qdrant and Chroma) to separate "good vector engine" from "good fit for
  PrimaDB's local-first/browser model."

## In Progress

- Preparing the compact findings report and final recommendation ranking.

## Verification Pending

- None.
