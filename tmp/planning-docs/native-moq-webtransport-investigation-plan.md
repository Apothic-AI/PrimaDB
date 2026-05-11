# Native MoQ WebTransport Investigation Plan

## Goal

- Determine why PrimaDB has MoQ/WebTransport helpers in browser/package surfaces but no native Rust transport implementation.
- Separate repo-backed facts from likely inference.

## Evidence To Check

- Current MoQ helper implementations in browser, Node, and Python packages.
- Native Rust transport modules and Cargo features.
- Docs, examples, and generated API references for MoQ wording.
- Git history/blame around MoQ helper introduction.
- Current dependency/ecosystem constraints only where repo evidence is insufficient.

## Output

- Report the most likely reason, confidence level, and concrete evidence.
- Identify what would be needed to implement native Rust WebTransport/MoQ now.
