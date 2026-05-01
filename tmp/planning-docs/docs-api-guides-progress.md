# API Docs And Guides Progress

## Completed

- Added a `Guides` docs category.
- Added task-oriented guides for auth/encryption/password keys, relay/full-node/mesh, binary/media/MoQ, transactions/strict scopes, and query/watch/traversal.
- Updated the main docs index to link the new guides.
- Updated SDK pages to link relevant guides and show password-derived key usage.
- Expanded the API reference overview with the current generated coverage and source-of-truth inputs.
- Fixed the Node package declaration surface for binary/blob chain helpers so generated API docs match the native addon.
- Corrected watch/traversal guide examples to distinguish browser callbacks from native pull-style subscriptions.
- Clarified native/browser blob storage examples and aligned Python full-node commands with the package example README.

## Verification

- `pnpm --dir website build`
- `pnpm --dir packages/primadb-node exec tsc --noEmit --allowJs false index.d.ts`
- `git diff --check`
- Artifact tracking review: `git ls-files` found no tracked build output, native addons, WASM bundles, virtualenvs, or dependency directories.
