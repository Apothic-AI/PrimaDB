# PrimaDB Familiarization Plan

## Goals

- Rebuild a current mental model of the repo's architecture, supported targets, and primary capabilities.
- Confirm that the source tree, docs, and package wrappers still agree on PrimaDB's intended product surface.
- Validate the current local state with lightweight verification rather than relying only on static reading.

## Scope

- Review the top-level crate manifest, README, docs index, concept docs, and representative core modules.
- Inspect the browser, Node, and Python package READMEs to understand how the Rust core is surfaced externally.
- Check example and verification docs to understand the current operational and testing expectations.
- Run a small set of high-signal commands to confirm the crate metadata and current test/check baseline.

## Verification

- Run `cargo metadata --no-deps --format-version 1`.
- Run `cargo test --lib --quiet`.
- Run `cargo check --features "crypto native-websocket native-webrtc scripting" --quiet`.
- Review `git status --short` before concluding.
