---
title: Network Hooks
sidebar_position: 2
---

PrimaDB exposes optional network-boundary hooks instead of deep built-in graph read ACLs.

## Hook Surface

- `onConnect`
- `onJoinRoom`
- `onPull`
- `onWatch`
- `onServeResult`

Rust uses trait methods on `NetworkHooks`. Browser, Node, and Python expose matching callback
registration on the SDK surface.

## What They Are For

- connection gating
- room gating
- request denial
- request rewrite
- served-result redaction or reshape

## What They Are Not

They are not:

- a full graph ACL system
- encrypted-query infrastructure
- a replacement for value encryption

## Decision Semantics

Across browser, Node, and Python, the current semantics are aligned:

- `null` / `undefined` / `None` => allow unchanged
- `true` => allow unchanged
- `false` => deny
- string => deny with message
- wrapper object/dict => allow or deny with optional rewritten request/result

## Why The Boundary Is Important

This keeps the core graph/storage/index/watch model simpler while still giving applications a place
to enforce operational policy.
