# PrimaDB Docs Site

This directory contains the Docusaurus site for PrimaDB.

Authored docs content lives in the repo’s top-level
[docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/docs) directory. The `website/` folder is
only the site shell, theme config, and pnpm-managed toolchain.

## Local Development

```bash
cd /home/bitnom/Code/gunport/primadb/website
pnpm install
pnpm run start
```

## Production Build

```bash
cd /home/bitnom/Code/gunport/primadb/website
pnpm run build
```

## GitHub Pages

The docs site is configured for GitHub Pages at:

- `https://apothic-ai.github.io/PrimaDB/`

The Pages deploy runs from [.github/workflows/docs.yml](https://github.com/Apothic-AI/PrimaDB/tree/master/.github/workflows/docs.yml).
Pull requests still build the site for validation, but only pushes to `master` publish it.

## Notes

- `docs/` is the canonical location for Markdown and MDX docs pages.
- planning notes were intentionally moved to
  [tmp/planning-docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/tmp/planning-docs) so the
  public docs directory can stay clean.
