# Ambient Remote Interest Plan

## Goals

- Stop requiring application-level callers to pass a peer id for the common connected/meshed read-watch path.
- Preserve explicit peer targeting for authority reads, tests, debugging, and advanced routing.
- Keep the transport protocol unchanged: pull/watch messages still route to concrete peers internally.
- Expose an optional policy object so callers can constrain target selection only when needed.

## API Direction

- Keep existing explicit methods such as `remoteRecords(peerId, scan)` and `watchRemoteRecords(peerId, scan)`.
- Add ambient helpers on transport handles:
  - relay pull helpers (`get`, `query`, `lex`, `records`, `node`, `snapshot`) select a peer from connected/recommended peers.
  - relay and mesh watch helpers (`watchGet`, `watchMap`, `watchQuery`, `watchLex`, `watchRecords`, `watchNode`, `watchSnapshot`) select an open/capable peer.
- Use `RemoteInterestPolicy` for advanced selection:
  - `{ target: "any" }` is the default.
  - `{ target: "peer", peerId: "..." }` preserves explicit targeting through the policy shape.
  - `{ target: "peers", peers: ["..."] }` constrains selection to an ordered peer set.
  - `requireCapability` makes capability advertisement mandatory instead of preferred.

## Non-Goals

- Do not remove the existing explicit peer APIs.
- Do not broadcast one pull request to every peer in this tranche; direct pulls still pick one peer.
- Do not make strict-scope transactions peer-agnostic; authority targeting remains explicit until scope authority routing is implemented.
- Do not build full multi-peer watch fan-in yet; ambient watches currently bind to the selected peer.

## Verification

- Compile native relay/mesh features.
- Compile WASM/browser bindings.
- Compile Node and Python native package wrappers.
- Run focused type/stub checks and existing watch tests.
