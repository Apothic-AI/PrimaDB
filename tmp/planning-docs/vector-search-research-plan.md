# Vector Search Research Plan

This document captures the research scope for evaluating embeddable vector
storage and search options for Primadb.

## Goals

- Identify Rust crates or libraries that minimize implementation effort for
  first-class vector storage and exact/approximate search in Primadb.
- Prefer current primary sources: crates.io, docs.rs, official docs, and
  upstream READMEs.
- Evaluate fit against Primadb's actual constraints:
  - browser WASM package
  - native Rust core with segment-file persistence
  - Node/Python bindings
  - local-first keyed-record and graph APIs
  - existing scalar direct-index pushdown

## Candidate Set

- `hnsw_rs`
- `instant-distance`
- `kiddo`
- `usearch` Rust bindings
- `faiss` Rust bindings
- `lance` / `lancedb`
- `sqlite-vec`
- `libsql` vector support
- `tantivy` vector support if relevant
- Qdrant and Chroma as external-service comparison points only

## Evaluation Criteria

- Embeddable crate/library status
- Pure Rust vs native dependency burden
- Whether it stores vectors itself or is search-index-only
- Exact vs approximate search support
- Persistence model
- Incremental insert/delete/update support
- Metadata or key-prefix filtering options
- WASM/browser viability
- Memory behavior
- Dimension and metric support
- License
- Apparent maturity and maintenance posture

## Deliverable

- Compact findings report with a shortlist, source links, per-option capability
  notes, and a ranked recommendation for a Primadb implementation path.
