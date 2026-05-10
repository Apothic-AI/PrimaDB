# RuvLLM Claim Evaluation Plan

This document captures the scope for evaluating `ruvllm` public claims against
the local RuVector source tree.

## Goals

- Inspect the current published `ruvllm` crate metadata and README claims.
- Compare those claims against the local implementation in
  `/home/bitnom/Code/RuVector/crates/ruvllm`.
- Pull in related crates only where `ruvllm` depends on them directly or the
  claim clearly relies on them (`ruvector-core`, `sona`, `ruvllm-wasm`).
- Focus on implemented behavior for:
  - vector search
  - reranking
  - vector/state self-adaptation
  - automatic optimization / learning
  - persistence
  - update/delete behavior
  - metadata/filter support
  - WASM/native suitability
  - PrimaDB applicability

## Evaluation Method

- Treat local source as the primary implementation truth.
- Treat crates.io/docs.rs/GitHub-facing docs as claim sources, not proof of
  implementation.
- Prefer direct code-path evidence over README language.
- Distinguish `ruvllm` crate capabilities from capabilities that only exist in
  adjacent crates or in internal-only modules.

## Deliverable

- Concise claim-by-claim matrix with:
  - claim
  - evidence found
  - status (`implemented`, `partially implemented`, `stub/demo`, `unclear`, or
    `not found`)
- Notes on misleading or overbroad claims.
- Notes on usable embeddable APIs and whether PrimaDB should depend on them.
