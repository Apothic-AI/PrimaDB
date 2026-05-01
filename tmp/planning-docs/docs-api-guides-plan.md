# API Docs And Guides Plan

## Goals

- Verify the generated API reference covers the current browser, Node, Python, MoQ, hook, strict-scope, crypto, and Rust surfaces.
- Make the docs site easier to use by adding workflow-oriented guides that complement the generated API reference.
- Keep SDK pages linked to the relevant guides and package-local examples.
- Validate the docs with the Docusaurus build so broken links and generated API drift fail visibly.

## Scope

- Add a `docs/guides` category for task-oriented material.
- Add guides for authentication/encryption/password keys, relay/full-node/mesh topology, binary/media/MoQ usage, transactions/strict scopes, and query/watch/traversal behavior.
- Update the API reference landing page to document the source-of-truth files used by the API generator.
- Update the main docs index and SDK guide pages to point users to the new guide material.

## Verification

- Run `pnpm --dir website build` to regenerate API docs and build the Docusaurus site.
- Run `git diff --check` to catch whitespace errors.
- Review git status for accidentally tracked build artifacts.
