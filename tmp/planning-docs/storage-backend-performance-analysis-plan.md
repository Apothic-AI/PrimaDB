# Storage Backend Performance Analysis Plan

## Objective

Determine whether PrimaDB still relies on JSON object/file storage in a way that should be replaced
with a more performant storage backend.

## Questions

- Which public storage paths are snapshot-centered JSON persistence paths?
- Which paths are incremental segment-backed paths?
- Do the segment-backed paths still serialize JSON per record/file, and if so is that the primary
  scaling bottleneck?
- Is replacement warranted now, or should the current implementation be improved in smaller steps?

## Evidence To Collect

- Read `src/storage.rs`, `src/persistence.rs`, `src/durable.rs`, `src/engine.rs`, `src/db.rs`,
  `src/wasm.rs`, and `src/wasm_opfs.rs`.
- Compare implementation behavior against `docs/concepts/storage.md` and existing planning docs.
- Run targeted storage tests where practical.
- Inspect actual file layout from a small native segment store if needed.

