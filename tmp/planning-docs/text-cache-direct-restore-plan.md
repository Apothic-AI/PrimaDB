# Persisted Text Cache Direct Restore Plan

## Goal

Restore the serialized exact BM25 terms and postings structures directly when a persisted native or OPFS text cache is loaded. The loader must retain the existing cache file/API shape while rejecting incompatible or internally inconsistent data.

## Scope

- Decode and validate the manifest, serialized config, documents, term frequencies, and postings.
- Reconstruct only the derived in-memory lookup maps needed by BM25 scoring.
- Preserve source-hash, analyzer, backend, and collection validation behavior.
- Add focused direct-restore and native persistence regression coverage.

## Verification

- `cargo fmt --all -- --check`
- Focused text-cache tests
- `cargo test --lib`
- `cargo check --lib`
