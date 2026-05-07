# SegmentFiles Direct Index Filename Hardening Plan

## Problem

Native `SegmentFileStore` stores direct scalar index entries under filesystem paths derived from logical index keys:

`indexes/direct/<encoded path>/<sortable scalar key>/<encoded node id>.json`

String sortable keys currently include the full hex-encoded scalar string. Large application values, such as encrypted checkpoint ciphertext, can therefore become oversized filesystem path components and fail with `File name too long`.

## Goals

- Never place unbounded scalar values directly in native SegmentFiles path components.
- Preserve direct-index query semantics for equality, prefix, range, ordering, and set query pushdown.
- Handle physical-path hash collisions without overwriting unrelated logical index entries.
- Keep the fix storage-engine-level and application-agnostic.
- Add focused regression coverage for large string scalar persistence with SegmentFiles.

## Implementation

1. Keep logical direct index keys and `DirectScalarIndexEntry.sortable_key` unchanged so query semantics remain defined by the full scalar value.
2. Add bounded physical path components for native SegmentFiles direct index storage.
3. Use short literal physical components for small logical components and hashed physical components for oversized logical components.
4. Store direct index files as buckets keyed by the full logical direct index key so physical hash collisions can coexist safely.
5. Update direct index writes, stale removal, scans, and vacuum to operate on bucket files.
6. During scans, filter entries by stored logical `path` and full `sortable_key`, sort by logical sortable key, then apply direction/limit.
7. Add regression tests that save, reload, query, and update nodes containing very large string scalar values using SegmentFiles.

## Validation

- Reproduce the filename-limit class with a large scalar test.
- Run targeted native SegmentFiles tests.
- Run `cargo fmt --check`.
- Run relevant `cargo test --lib` coverage.
