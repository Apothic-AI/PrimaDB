# Gun Record Watch Comparison Plan

## Goals

- Understand how Gun handles live reads without exposing separate local and remote watch concepts.
- Compare Gun's graph GET/PUT event model to PrimaDB's record scan and remote watch design.
- Identify a PrimaDB implementation shape that stays close to Gun's model while supporting keyed record scans.

## Scope

- Inspect the local Gun checkout under `/home/bitnom/Code/gunport/gun`.
- Review Gun chain `get`, `on`, `map`, root `in/out/get/put`, storage adapters, mesh transport, and AXE subscription routing.
- Translate the relevant design into PrimaDB terms without requiring Starla or application code to poll.

## Verification

- Use source inspection of `/home/bitnom/Code/gunport/gun` at commit `ed27b48`.
- Ground conclusions in concrete file references.
