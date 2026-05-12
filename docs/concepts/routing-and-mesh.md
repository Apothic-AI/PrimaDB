---
title: Routing And Mesh
sidebar_position: 5
---

PrimaDB has a DAM-like routing layer, but not Gun’s exact wire format.

## Routing Layer

Transport messages move through typed route envelopes with:

- targeted peer routing
- broadcast/topic-style delivery
- presence
- peer recommendation exchange
- pull requests and responses
- watch events
- application payloads for caller-defined protocols
- signaling payloads
- batching and chunking
- dedupe metadata such as BLAKE3 content hashes and seen-by hints

Application routes use the same envelope as sync, watch, pull, and signaling traffic. Handles expose
`publishApplication(...)` / `sendApplication(...)` / `subscribeApplications(...)` in JS and
`publish_application(...)` / `send_application(...)` / `subscribe_applications(...)` in Python and
Rust-oriented surfaces, so applications can route custom payloads without raw transport handles.

## Relay Mode

`connectRelay(...)` keeps the relay in the data path.

That means:

- peer discovery still works
- peer-agnostic remote `get/query/lex/records/node/snapshot` pulls still work
- peer-agnostic remote watches still work
- explicit record fan-in APIs can query or watch all policy-matching reachable peers
- relay `remoteTransaction(...)` / `remote_transaction(...)` can submit strict-scope proposals to
  an authority peer
- but peer-to-peer traffic continues through the relay

## Mesh Mode

`connectMesh(...)` uses the relay for discovery and signaling, then attempts direct peer links over
WebRTC data channels.

That means:

- same room plus relay-backed signaling is what triggers direct peering
- sharing a relay alone does not automatically upgrade relay traffic into direct mesh traffic
- public mesh APIs expose peer-agnostic watches, application routes, and record fan-in
- strict remote transactions still target a concrete authority peer rather than fan-in

This is close to how Gun behaves when its WebRTC plugin is active.

## MoQ Signaling And Fallbacks

Browser mesh can use MoQ/WebTransport as the signaling underlay through `connectMeshViaMoq(...)`.
That does not make MoQ the direct P2P data path; WebRTC data channels carry direct mesh traffic after
signaling succeeds.

If a MoQ/WebTransport signaling session fails, PrimaDB does not silently convert that session into a
PrimaDB WebSocket relay connection. Applications that want a fallback ladder should explicitly try
MoQ signaling, then normal relay-backed `connectMesh(...)`, then local `BroadcastChannel` signaling
where appropriate. The JS MoQ helper's WebSocket option is for MoQ-over-WebSocket-compatible
endpoints, not for PrimaDB's WebSocket relay protocol.

## ICE Servers

PrimaDB does not hard-code STUN defaults in core. Mesh configs accept `iceServers`, and the example
apps choose practical public defaults such as `stun:stun.cloudflare.com:3478`.
