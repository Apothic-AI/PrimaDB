# Off-Lock Cache Rebuild Plan

## Objective

Keep expensive text/vector cache reconstruction and native cache-file I/O out of
Primadb's global `Inner` mutex while preserving the synchronous API and cache
correctness.

## Scope

- Capture collection records, source hash, and `change_revision` under lock.
- Rebuild or decode caches outside the lock.
- Serialize and write native cache files outside the lock.
- Install a completed cache only when its captured revision is still current.
- Serialize cache exports outside the lock with a revision check.
- Gate one rebuild per collection and wake waiters when it completes.
- Add focused concurrency and stale-rebuild regression coverage where practical.

## Verification

- `cargo fmt --check`
- `cargo test --lib`
- Relevant native cache tests and `cargo check`
