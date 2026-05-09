# Record Watch Primitive Analysis Plan

## Goals

- Verify whether PrimaDB currently exposes local or remote record-scan watch APIs.
- Check whether existing record writes and watch invalidation machinery can support a first-class record watch cleanly.
- Give a recommendation for using record watches as the substrate for higher-level APIs such as `starla.db.watch`.
- Implement the record watch primitive in a Gun-like way: one core request/watch shape served by local, relay, and mesh transports rather than distinct local and remote semantics.

## Scope

- Inspect record API types and graph storage representation.
- Inspect sync request/result/watch protocol variants.
- Inspect local and remote watch refresh/invalidation behavior.
- Review browser, Node, Python, and generated docs surfaces for existing record watch APIs.
- Add record scans to pull results, watch events, result chunking/accumulation, host bindings, and docs.
- Track logical touched record keys through change events so record-scan watches can invalidate by `RecordScan::matches_key` instead of depending on hashed storage node ids.

## Design Decision

Use the Gun-like version of the plan: add one shared record interest shape to the core protocol and
let local subscriptions, relay pulls, relay watches, and mesh watches all serve that same
`PullRequestKind::Records { scan }` request. The public APIs are still named for each host
environment (`watch_records`, `watchRecords`, `remoteRecords`, `watchRemoteRecords`), but those are
thin helpers over the same request/result machinery rather than separate local-vs-remote semantics.

## Verification

- Use source inspection against the current monorepo checkout.
- Prefer concrete file references over relying on the separate agent's path or commit.
- Run focused Rust tests and feature checks after implementation.
