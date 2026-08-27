# Progress

## 2026-08-27

- Added a full-build materialization cache shared by every direct-index root.
- Cached only completed acyclic subgraphs; cycle-tainted traversals continue to
  materialize per root so back-edge truncation remains root-relative.
- Kept scalar inspection in the materialization path, including crypto signed
  value verification and auth metadata generation.
- Added focused tests for shared fan-out correctness, a 512-root/24-node shared
  chain with one materialization visit per graph node, root-relative cycles,
  and signed scalar indexing under `crypto`.
- Verification completed: formatting, 110 default library tests, 139
  all-feature library tests, 139 all-target/all-feature tests, and
  all-target/all-feature checking pass. The check reports one pre-existing
  dead-code warning.
