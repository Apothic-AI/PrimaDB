# PrimaDB Docs Site

This directory contains the Docusaurus site for PrimaDB.

Authored docs content lives in the repo’s top-level
[docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/docs) directory. The `website/` folder is
only the site shell, theme config, and pnpm-managed toolchain.

## Local Development

```bash
cd /path/to/primadb/website
pnpm install
pnpm run generate:api
pnpm run start
```

## Production Build

```bash
cd /path/to/primadb/website
pnpm run generate:api
pnpm run build
```

## Cloudflare Workers

The docs site is configured for static asset deploys through Wrangler using
[wrangler.toml](https://github.com/Apothic-AI/PrimaDB/tree/master/website/wrangler.toml).

Current public URL:

- `https://primadb-docs.apothic.workers.dev`

Deploy it with:

```bash
cd /path/to/primadb/website
pnpm install
pnpm run deploy
```

The CI workflow in [.github/workflows/docs.yml](https://github.com/Apothic-AI/PrimaDB/tree/master/.github/workflows/docs.yml)
continues to validate the build, but deployment now happens through Cloudflare rather than GitHub
Pages.

## Notes

- `docs/` is the canonical location for Markdown and MDX docs pages.
- `pnpm run generate:api` refreshes the generated API reference pages and bundled Rust rustdoc.
- planning notes were intentionally moved to
  [tmp/planning-docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/tmp/planning-docs) so the
  public docs directory can stay clean.
