# P2 BM25 Optimization

## Goal

Bound BM25 collection result selection and avoid constructing a full postings
index for one-shot candidate searches while preserving exact result behavior.

## Plan

- [x] Add bounded top-k selection with deterministic score and ID ordering.
- [x] Score one-shot candidates directly from analyzed query-term frequencies.
- [x] Preserve scores, field hits, metadata, snippets, explanations, and paging.
- [x] Complete formatting, Rust checks, and full relevant tests.
