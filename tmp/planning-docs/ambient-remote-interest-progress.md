# Ambient Remote Interest Progress

## Completed

- Added `RemoteInterestPolicy` and `RemoteInterestTarget` to the shared sync API.
- Added native relay peer selection over recommended/known peers with capability preference.
- Added native mesh peer selection over open mesh channels with capability preference.
- Added Rust relay pull/watch `*_with_policy(...)` helpers.
- Added Rust mesh watch `*_with_policy(...)` helpers.
- Exposed ambient relay pulls and relay/mesh watches through browser WASM, Node, and Python bindings.
- Added `RemoteInterestPolicy` typing/stubs for browser TypeScript, Node, and Python packages.
- Updated authored docs, generated API docs, and package READMEs.
- Verified native relay/mesh, Node, Python, and WASM cargo checks.
- Verified browser package build/type generation, Node declaration type check, Python stub syntax check,
  Rust tests, docs API generation, docs site build, and diff whitespace checks.

## In Progress

- Ready for review.
