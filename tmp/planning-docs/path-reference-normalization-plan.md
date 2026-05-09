# Path Reference Normalization Plan

## Goals

- Remove stale hardcoded checkout paths that point at `/path/to/primadb`.
- Make documentation commands portable across arbitrary local clone locations.
- Make smoke tests and helper scripts derive repo-local paths from their own file locations instead of a user-specific absolute path.

## Scope

- Update Markdown links that currently embed absolute filesystem targets so they use repo-relative links.
- Update Markdown command snippets to use `/path/to/primadb/...` placeholders instead of one developer's checkout path.
- Update example and smoke scripts to preserve environment-variable overrides while deriving default repo roots from `import.meta.url` or the script directory.

## Verification

- Run a stale-path search for the retired checkout path with target and virtualenv directories excluded.
- Run targeted smoke-script syntax checks for the touched JavaScript files.
- Review `git diff --check` and `git status --short`.
