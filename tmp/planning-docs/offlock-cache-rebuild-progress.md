# Off-Lock Cache Rebuild Progress

## 2026-08-27

- Created isolated `jj` workspace `primadb-offlock-cache-rebuild-20260827`.
- Located the global `Inner` mutex hot paths in `db.rs`: text/vector readiness,
  cache reconstruction, cache decode, cache serialization, and native writes.
- Existing native text/vector cache persistence tests provide baseline coverage.
- Added per-collection text/vector rebuild gates backed by a condition variable,
  so concurrent callers share one rebuild rather than duplicating work.
- Read authoritative records and `change_revision` under the mutex, then perform
  cache-file reads, index reconstruction, cache serialization, and native cache
  writes outside the mutex.
- Reacquire the mutex to install only matching-revision results; stale builds are
  discarded and retried from a fresh source snapshot.
- Moved cache export serialization and cache import decoding out of the global
  mutex, with revision checks for safe installation.
- Added a regression test covering eight concurrent text/vector rebuild callers.
- Verification completed: formatting, library check, full library tests (100),
  focused native cache tests, and `vector-edgevec` library check all pass.
