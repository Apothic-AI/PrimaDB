# Native MoQ WebTransport Investigation Progress

## Started

- Began local evidence review for MoQ/WebTransport support across browser, Node, Python, native Rust transports, docs, examples, and git history.

## Evidence Collected

- Current browser package MoQ support is implemented in TypeScript via `@moq/lite`, with `connectPrimadbMoq(...)`, WebTransport options, injected transport support, and WebSocket fallback options.
- Current Node package MoQ support is implemented in JavaScript via `@moq/lite`, not in the Rust native addon.
- Current Python package MoQ support is a deterministic SDK-level loopback adapter. Its docstring says Python MoQ bindings do not yet expose stable generic byte tracks on Python 3.14.
- The Rust crate has no Cargo features or dependencies for MoQ, WebTransport, QUIC, `moq-native`, `moq-lite`, `web-transport`, or `wtransport`. Native Rust transport features remain `native-websocket` and `native-webrtc`.
- Node and Python native binding crates enable only `crypto`, `native-websocket`, `native-webrtc`, and `scripting` on the core crate.
- Docs describe MoQ helpers as experimental and package-local, and say they model path/track/sequence frames on top of PrimaDB package surfaces.
- The historical MoQ-introduction commit was `d4a1620 Add experimental MoQ package helpers and examples`. Its Rust changes only added/exposed `drain_pending_envelope_json`; it did not add a native Rust transport module or dependency.
- After fetching the just-pushed upstream `master`, the only new commits above the local subrepo point were vector storage/search and familiarization docs; no native Rust MoQ/WebTransport work was added.
- Current crates.io ecosystem check shows native Rust MoQ/WebTransport options now exist: `moq-native 0.14.0`, `moq-lite 0.16.0`, `web-transport 0.10.5`, and `wtransport 0.7.x`.

## Findings

- Direct conclusion: PrimaDB's current MoQ support is an SDK-level sync-envelope adapter, not a core native Rust transport.
- Direct conclusion: Native Rust transports are currently WebSocket relay and WebRTC mesh only.
- Inference: Native Rust WebTransport/MoQ was likely deferred rather than intentionally rejected. The implementation was scoped as experimental/package-local and used `@moq/lite` where it was immediately available for browser and Node; Python got only a loopback because its binding ecosystem was not stable enough.
- Inference: A Rust-native implementation is feasible now, but it would be a real transport feature, not just parity glue. It would need a new optional Cargo feature, dependency choice, route/envelope integration, session/auth/hook behavior, fallback semantics, tests, and SDK bindings.
