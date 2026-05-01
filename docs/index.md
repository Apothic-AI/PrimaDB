---
slug: /
title: PrimaDB
sidebar_position: 1
---

PrimaDB is a local-first graph database inspired by Gun, but built around explicit versioned
operations, deterministic merge rules, and a transport boundary that stays inspectable.

It is one product with multiple host surfaces:

- Rust crate
- browser WASM runtime
- TypeScript package
- native Node package
- native Python package

The core database model is shared across those targets. Host-specific layers handle browser
storage, native storage, relay transport, WebRTC mesh transport, package ergonomics, and callback
bridges.

## What PrimaDB Already Covers

- graph-shaped documents with nested path traversal
- deterministic merge behavior based on explicit operations and hybrid logical revisions
- local transactions, scoped transaction boundaries, and opt-in coordinated strict scopes
- query filters, ordering, limits, lexical traversal, and remote watches
- relay-routed replication and relay-signaled mesh replication
- browser and native WebRTC mesh support
- signed values, delegated write certificates, encrypted payloads, and SEA-style browser crypto
- bytes in the graph plus separate blob storage for larger binary payloads
- browser, Node, and Python SDKs with package-local runnable examples

## Start Here

- [Install and build](getting-started/install-and-build)
- [Rust quickstart](getting-started/rust)
- [Browser quickstart](getting-started/browser)
- [TypeScript package](sdk/typescript)
- [Node package](sdk/node)
- [Python package](sdk/python)
- [Guides](category/guides)
- [API reference](category/api-reference)

## Core Concepts

- [Data model](concepts/data-model)
- [Replication and convergence](concepts/replication)
- [Strict consistency](concepts/strict-consistency)
- [Routing and mesh](concepts/routing-and-mesh)
- [Query and watch](concepts/query-and-watch)
- [Auth and privacy](concepts/auth-and-privacy)
- [Storage and durability](concepts/storage)

## Guides

- [Auth, encryption, and password keys](guides/auth-encryption)
- [Relay, full node, and mesh](guides/relay-full-node-and-mesh)
- [Binary data, media, and MoQ](guides/binary-media-and-moq)
- [Transactions and strict scopes](guides/transactions-and-strict-scopes)
- [Query, watch, and traversal](guides/query-watch-and-traversal)

## Examples And Operations

- [API reference overview](api)
- [Examples overview](examples/overview)
- [Running examples](examples/running-examples)
- [Build targets](reference/build-targets)
- [Network hooks](reference/network-hooks)
- [Versioning and releases](reference/versioning-and-releases)
- [Verification matrix](reference/verification)

## Project Status

PrimaDB is still pre-release. The codebase is real and heavily exercised, but the product surface is
still moving. That is why the docs site is organized around the current behavior of the repo rather
than a frozen public compatibility promise.
