# Path Reference Normalization Progress

## Completed

- Confirmed the current repo lives under `libs/rust/primadb`, while many docs and scripts still referenced a stale checkout path.
- Identified affected categories: top-level docs, package/example READMEs, planning-doc indexes, browser smoke scripts, native smoke scripts, package example smokes, and one Python shell smoke.
- Converted Markdown links that embedded absolute filesystem targets into repo-relative links.
- Replaced command snippets that assumed a specific checkout with `/path/to/primadb/...` placeholders.
- Updated smoke-script default roots to derive from `import.meta.url` or the shell script directory while preserving environment-variable overrides.
- Added the path-normalization task docs to the planning-doc index.
- Verified there are no remaining stale checkout-path references in the tracked project files.
- Verified touched JavaScript files with `node --check`.
- Verified the Python mesh retry shell script with `bash -n`.
- Verified the docs site with `pnpm --dir website build` after a website-local `pnpm install --ignore-workspace --lockfile=false`.

## In Progress

- Preparing the final commit for the normalized path references.

## Verification Pending

- None.
