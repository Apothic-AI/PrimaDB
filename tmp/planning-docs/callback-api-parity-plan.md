# Callback API Parity Plan

## Goal

Make network hook callbacks consistent across browser, Node, and Python without regressing:

- thread safety
- offline-first behavior
- transport throughput
- package ergonomics

## Canonical Contract

All SDKs should expose the same five hook concepts:

- `onConnect`
- `onJoinRoom`
- `onPull`
- `onWatch`
- `onServeResult`

All SDKs should use the same decision semantics:

- `undefined` / `null` / `None`: allow unchanged
- `true`: allow unchanged
- `false`: deny with the default message
- string: deny with that message
- wrapper object / dict:
  - `allow?: boolean`
  - `message?: string`
  - `request?: PullRequestKind`
  - `result?: RemoteResult`
- raw `PullRequestKind`: rewrite request
- raw `RemoteResult`: rewrite result

## SDK Surface

Browser:

- keep `db.setNetworkHooks(hooks)` / `db.clearNetworkHooks()`
- keep the typed TS helpers in `packages/primadb/hooks.ts`

Node:

- add `db.setNetworkHooks(hooks)` / `db.clearNetworkHooks()`
- use the same camelCase callback names as browser
- expose the same hook context and decision types in `index.d.ts`

Python:

- add `db.set_network_hooks(hooks)` / `db.clear_network_hooks()`
- prefer snake_case callback names:
  - `on_connect`
  - `on_join_room`
  - `on_pull`
  - `on_watch`
  - `on_serve_result`
- accept camelCase aliases too so the conceptual contract stays aligned with browser and Node

## Implementation Strategy

### 1. Shared decision parsing in core

Move decision parsing into shared Rust helpers so browser, Node, and Python all resolve hook responses through the same logic.

This avoids three diverging implementations of:

- allow / deny defaults
- wrapper-object parsing
- request/result rewrite parsing

### 2. Node callback bridge

Use `ThreadsafeFunction` to marshal hook invocation onto the Node event loop safely.

Requirements:

- no direct JS callback invocation from transport/runtime threads
- preserve JS exceptions as hook denials instead of process crashes
- normalize `undefined` to `null` before Rust parses the response

Approach:

- accept a hook object from JS
- extract optional callback functions
- wrap each function so `undefined` becomes `null`
- create `ThreadsafeFunction`s for each callback
- block the Rust hook call until the JS response is returned
- parse the response through the shared core helper

### 3. Python callback bridge

Use stored `Py<PyAny>` callables and acquire the GIL inside hook invocations.

Requirements:

- safe cross-thread access
- clear sync callback semantics
- no ad hoc background polling or callback queues

Approach:

- accept a hook object or dict
- resolve snake_case or camelCase callback members
- store the optional callables
- call them under `Python::with_gil(...)`
- parse the response through the shared core helper

### 4. Package parity work

Update package-level API surfaces:

- Node `index.d.ts`
- Python `__init__.pyi`
- package docs
- smoke tests for both native packages

## Verification

Minimum verification:

- core Rust tests
- wasm target check
- browser package build/typecheck/smoke
- Node package build plus hook smoke
- Python package sync plus hook smoke

Behavior to verify:

- deny connection
- deny room join
- rewrite pull/watch request
- rewrite served result
- clear hooks restores default behavior
