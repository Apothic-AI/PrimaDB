# Direct-Index Tranche 4 Plan

## Goal

Provide storage-level ordered direct-index range scans that can stop after the
requested page, and reduce repeated graph materialization during direct-index
construction without changing query semantics.

## Scope

- Extend the existing `DirectIndexScan` descriptor with an offset and optional
  candidate membership set.
- Traverse order-preserving physical scalar-key directories and bucket files in
  the requested direction, applying filters and the page window while reading.
- Keep a sorted fallback for hashed physical components used by long scalar
  keys.
- Cache compact root-relative scalar-leaf fragments behind `Arc` ownership;
  retain root-relative cycle truncation and signed-scalar inspection.
- Cover bounds, reverse scans, candidates, pagination, ties, long-key physical
  hashing, shared fan-out, cycles, and crypto query/index behavior.

## Verification

- Run formatting, focused direct-index/query tests, full native test/check
  matrices, and the wasm library check when the target is available.
- Inspect jj status, diff statistics, log, and conflict state before handoff.
