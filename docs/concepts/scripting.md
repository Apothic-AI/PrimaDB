---
title: Scripting
sidebar_position: 8
---

PrimaDB scripting lets applications attach executable scripts to graph nodes and explicitly execute
those scripts later.

Scripts are stored in the graph so they can replicate with the rest of the data, but they are not
automatically trusted or run when received from another peer.

## Execution Model

The first scripting runtime is `rhai`, enabled by the Rust `scripting` feature and included in the
browser, Node, and Python package builds.

Execution is explicit:

- attach a script to a node path
- grant capabilities for the current execution
- call `executeNodeScripts(...)` / `execute_node_scripts(...)`
- review the returned value, generated transaction steps, and commit report

Scripts receive a context object containing:

- the node path
- the current node value
- the current node state when available
- outgoing link/set-member edge metadata
- the script metadata
- caller-provided args

## Capabilities

Capabilities are granted by the local application at execution time. A replicated script cannot
grant itself access to your graph.

Script-attached capabilities are treated as requested limits. Effective access requires local
execution capabilities and, when the script declares requested capabilities for an operation, the
script request must also cover the path.

Supported capability groups:

- `read`
- `query`
- `traverse`
- `write`
- `transaction`

Writes are collected as transaction steps. They are applied atomically only when `applyWrites` is
enabled and the execution has matching `transaction` capability.

## Sandbox Boundary

The script facade exposes graph operations, not host powers. Scripts do not receive encryption keys,
filesystem access, environment variables, raw network transports, or arbitrary host callbacks.

Current facade functions:

- `db_get(path)`
- `db_map(path)`
- `db_query(path, spec)`
- `db_traverse(path, spec)`
- `db_put(path, value)`
- `db_unset(path)`
- `db_set(path, value)`
- `db_remove(path, value)`
- `db_increment(path, by)`

## Storage

Script attachments are stored under an internal graph root keyed by the target node path hash. The
stored manifest is chunked so large scripts do not create oversized scalar-index filenames in native
segment storage.

## Trust

The current implementation enforces local capability grants. Signed script-author verification is a
planned follow-up. Until then, applications should treat replicated scripts as inert data unless
local policy chooses to execute them.
