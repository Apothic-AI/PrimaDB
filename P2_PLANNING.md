# Exact-Vector Top-K Optimization

## Goal

Avoid allocating and sorting the full exact-vector result corpus for small
top-k requests while preserving exact ordering, tie-breaking, filtering, and
the public search API.

## Plan

- [x] Use a bounded heap of borrowed candidates for exact search.
- [x] Precompute requested ID filters outside the per-entry predicate.
- [x] Clone metadata and vectors only for retained matches.
- [x] Add focused ordering, filtering, payload, and large-corpus tests.
- [x] Complete formatting and relevant default/all-features verification.
