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
- signaling payloads
- batching and chunking
- dedupe metadata such as BLAKE3 content hashes and seen-by hints

## Relay Mode

`connectRelay(...)` keeps the relay in the data path.

That means:

- peer discovery still works
- peer-agnostic remote `get/query/lex/records/node/snapshot` pulls still work
- peer-agnostic remote watches still work
- relay `remoteTransaction(...)` / `remote_transaction(...)` can submit strict-scope proposals to
  an authority peer
- but peer-to-peer traffic continues through the relay

## Mesh Mode

`connectMesh(...)` uses the relay for discovery and signaling, then attempts direct peer links over
WebRTC data channels.

That means:

- same room plus relay-backed signaling is what triggers direct peering
- sharing a relay alone does not automatically upgrade relay traffic into direct mesh traffic
- current public mesh APIs expose peer-agnostic remote watches, not a dedicated remote transaction
  helper

This is close to how Gun behaves when its WebRTC plugin is active.

## ICE Servers

PrimaDB does not hard-code STUN defaults in core. Mesh configs accept `iceServers`, and the example
apps choose practical public defaults such as `stun:stun.cloudflare.com:3478`.
