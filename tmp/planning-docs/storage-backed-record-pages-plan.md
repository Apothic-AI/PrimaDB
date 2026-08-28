# Storage-Backed Record Pages Plan

## Objective

Eliminate the database-side O(total loaded node count) merge scan for bounded
storage-backed record pages while preserving record scan ordering, cursors,
lazy loading, overlays, deletes, and reopen behavior.

## Design

- Maintain a record-key overlay indexed independently from `Inner::nodes`.
- Update the index when record node state is changed locally, merged from a
  snapshot, or restored at a transaction boundary.
- Merge storage pages with only indexed overlay keys, then apply the public
  limit and continuation cursor semantics.
- Remove successfully persisted overlay entries so current storage pages can be
  returned without consulting loaded graph nodes.

## Verification

- Preserve the existing record scan regression matrix.
- Add instrumentation coverage proving unrelated loaded nodes are not visited
  during a bounded storage page merge.
- Run formatting, focused tests, full native checks, all-feature checks, and a
  WASM library check where the target is installed.
