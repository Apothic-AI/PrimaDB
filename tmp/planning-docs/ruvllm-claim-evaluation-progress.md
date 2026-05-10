# RuvLLM Claim Evaluation Progress

## Completed

- Confirmed the current published crate metadata for `ruvllm` from crates.io:
  `2.2.1`, with docs at `https://docs.rs/ruvllm/2.2.1`.
- Compared the published crate README cached from crates.io with the local
  workspace README and noted that the published README already advertises
  features labeled as newer than the published crate version.
- Inspected the main `ruvllm` implementation paths for vector-related claims:
  `policy_store.rs`, `witness_log.rs`, `session_index.rs`,
  `claude_flow/hnsw_router.rs`, `ruvector_integration.rs`,
  `context/semantic_cache.rs`, `quantize/turbo_quant.rs`,
  `optimization/sona_llm.rs`, `sona/integration.rs`, and
  `backends/candle_backend.rs`.
- Inspected related crates when the `ruvllm` claim depends on them:
  `ruvector-core`, `sona`, and `ruvllm-wasm`.
- Verified several implementation gaps and doc mismatches:
  - `ruvllm` docs/examples reference non-existent or stale APIs such as
    `Engine`, `embed`, `embed_batch`, and `VectorDb::add_document`.
  - `PolicyStore`, `SessionIndex`, and `WitnessLog` reconstruct search results
    with the query embedding instead of the stored embedding.
  - `WitnessLog` does not persist `context_doc_ids` or `response_embedding`.
  - `PolicyStore::delete`, `PolicyStore::get`, `PolicyStore::search_by_type`,
    and `PolicyStore::stats` are cache-only and do not fully reflect persisted
    state.
  - `ruvllm` advertises browser/WASM support on the main crate, but the actual
    browser-facing implementation lives in `ruvllm-wasm`, where integrated
    "intelligent LLM" wiring is still commented out.
- Identified that `ruvector-core` provides real embeddable vector-search
  building blocks such as persistence, delete, metadata filters, and reranking,
  but those capabilities are only partially surfaced through `ruvllm`.

## In Progress

- Final claim-by-claim status matrix and PrimaDB fit assessment.

## Verification Pending

- A broad `cargo test -p ruvllm --no-default-features --features minimal --lib`
  run was started from `/home/bitnom/Code/RuVector`, but it had not completed by
  the time the source review reached report stage.
