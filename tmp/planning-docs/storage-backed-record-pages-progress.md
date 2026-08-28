# Storage-Backed Record Pages Progress

## 2026-08-28

- Created the isolated `primadb-tranche3-storage-pages-20260828-a` jj workspace
  from clean `primadb-staging` at `cd81a2e9`.
- Confirmed the existing native segment trie walker already bounds storage
  enumeration by scan limits and key-prefix roots.
- Replaced database record overlay merging over all `inner.nodes` with a
  maintained key-indexed overlay and node-to-key bookkeeping for key changes.
- Added persistence lifecycle handling so successful storage flushes remove
  matching overlay entries while concurrent newer state remains indexed.
- Kept lazy nodes loaded from current storage out of the overlay index, while
  preserving rollback snapshots for genuinely changed record nodes.
- Added a large loaded-node instrumentation regression covering a bounded page
  over 256 persisted records, 256 lazily loaded records, and 10,000 unrelated
  loaded graph nodes.
- Improved native record trie iteration by filtering directory entries before
  sorting and using `DirEntry::file_type` for directory checks.
- Verification passes: `cargo fmt --all -- --check`; focused record tests (10
  passed); `cargo test --lib` (128 passed); `cargo test --all-targets` (128
  passed); `cargo test --all-targets --all-features` (157 passed); `cargo check
  --all-targets --all-features` (0 errors, one pre-existing dead-code warning);
  and `cargo check --target wasm32-unknown-unknown --lib` (pass).
- No benchmark command was run; the instrumentation test is the performance
  regression evidence for this tranche.

## Completion

- Final jj review and commit completed below.
