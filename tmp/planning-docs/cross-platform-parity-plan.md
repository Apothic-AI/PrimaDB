# Cross-Platform Parity Plan

This document defines the next parity phase for Primadb across native and WASM targets.

Primadb already shares a large core across both targets, but the host integration surface is
still asymmetric:

- the browser build has the strongest direct peer-to-peer story
- the native build has the strongest filesystem-backed storage story
- relay and storage semantics are close, but not yet presented as one canonical cross-platform
  capability set

We are not optimizing for backwards-compatibility with prior Primadb releases.
That allows this phase to improve the public surface instead of preserving convenience wrappers
that no longer represent the best model.

## Summary

Primadb should not force identical raw APIs on every host.
It should provide equivalent cross-platform capabilities through a shared core and a small number
of host adapters.

The canonical capability set should include:

- graph/query/auth core behavior
- local-first graph traversal with default on-demand peer fetch
- durable incremental storage
- relay-backed sync and remote pull/query
- direct peer mesh

The important change from earlier planning is that mesh is not browser-only.
Mesh is now a required cross-platform capability.

## Goals

- define one canonical cross-platform capability model
- normalize persistence semantics across native and WASM
- normalize relay semantics across native and WASM
- make mesh truly cross-platform, including browser <-> native interoperability
- keep browser-only and native-only convenience helpers where they add real value
- prove parity through an explicit test matrix

## Non-Goals

- do not make Primadb wire-compatible with Gun
- do not force browser-only convenience APIs onto native
- do not force file-path helpers into the browser
- do not preserve old Primadb entrypoints if a cleaner surface is better

## Canonical Surface

The shared product surface should be centered on the cross-platform core:

- `src/db.rs`
- `src/router.rs`
- `src/sync.rs`
- `src/auth.rs`
- `src/engine.rs`

Host modules should be adapters around that capability model:

- browser/WASM adapter
- native relay adapter
- native direct mesh adapter
- browser storage adapter
- native storage adapter

## Persistence Plan

Persistence parity should be capability parity, not backend identity.

Canonical capability:

- durable incremental storage
- startup metadata restore
- lazy node restore
- indexed/queryable materialized state
- crash-safe persistence hooks

Host implementations:

- native: segment/file-backed incremental store
- browser: IndexedDB-backed incremental store

Compatibility helpers may remain as thin adapters:

- browser local storage snapshot helper
- native snapshot-file helper

## Relay Plan

Relay parity should be semantic parity.

Both targets should expose the same high-level relay behavior:

- connect
- sync with ack/retry/requeue
- remote get/query/lex/snapshot
- node-addressed lazy fetch for traversal internals
- peer presence
- peer recommendations

The transport implementation can remain host-specific, but the configuration model and runtime
semantics should match.

## Mesh Plan

Mesh is a canonical cross-platform feature.

Requirements:

- browser peers and native peers can join the same room
- browser peers and native peers can signal through the same relay
- browser peers and native peers can exchange the same sync and routed pull frames over the direct
  peer channel
- relay-backed signaling is the default cross-platform mesh path
- browser-local signaling may remain as an optional browser-only fallback
- ICE server configuration must be supported on both sides

Implementation decision:

- browser keeps WebRTC data-channel mesh
- native gains a WebRTC data-channel adapter that speaks the same signaling and route protocol

This keeps the direct peer path genuinely shared instead of splitting the ecosystem into
browser-only mesh and native-only direct transports.

## Host-Specific Extras

These do not need forced 1:1 parity:

- browser `localStorage`
- browser IndexedDB convenience hooks
- browser `wasm-bindgen` bootstrap helpers
- browser thread-pool bootstrap helpers
- native file-path convenience helpers

They are acceptable as platform sugar as long as the underlying capability is represented in the
canonical model.

## Traversal Plan

Traversal is a canonical graph capability. The public surface should be high-level graph traversal,
not transport-specific `remoteTraverse(...)` methods.

Requirements:

- expose normal `traverse(...)` / `watchTraverse(...)` graph operations across Rust, browser, Node,
  and Python
- enable on-demand peer fetch by default, with options to bound or disable it
- avoid full graph sync before traversal begins
- use node-addressed pull internally when a linked node is missing locally
- merge fetched fragments into the local replica through the normal sync path
- return completion metadata so applications can distinguish complete, partial, timed-out, and
  limit-bounded results

## Test Matrix

Parity needs proof, not assumption.

Required end-to-end coverage:

- browser <-> browser relay
- browser <-> browser mesh on the default WASM build
- browser <-> browser mesh on the threaded WASM build
- native <-> native relay
- native <-> native mesh
- browser <-> native relay
- browser <-> native mesh

Required verification outputs:

- connectivity state
- peer count
- signaling mode
- live replication
- pull/query behavior where applicable
- durable storage restore where applicable

## Execution Order

1. add shared cross-platform configuration types for relay, mesh, and durable storage
2. add cleaned-up relay facades on native and WASM
3. add cleaned-up durable storage facades on native and WASM
4. add native WebRTC mesh interoperable with the browser mesh protocol
5. update examples so both default and threaded browser mesh demos support non-local peers
6. add the parity test matrix and run it end to end

## Success Criteria

This phase is complete when:

- native and WASM expose one coherent cross-platform story for storage, relay, and mesh
- browser-only and native-only conveniences are clearly separated from canonical capabilities
- native and browser peers can join the same relay-backed mesh room and replicate directly
- the end-to-end test matrix passes
