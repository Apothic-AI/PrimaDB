# PrimaDB Docs Site

This directory contains the Docusaurus site for PrimaDB.

Authored docs content lives in the repo’s top-level
[docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/docs) directory. The `website/` folder is
only the site shell, theme config, and npm toolchain.

## Local Development

```bash
cd /home/bitnom/Code/gunport/primadb/website
npm install
npm run start
```

## Production Build

```bash
cd /home/bitnom/Code/gunport/primadb/website
npm run build
```

## Notes

- `docs/` is the canonical location for Markdown and MDX docs pages.
- planning notes were intentionally moved to
  [tmp/planning-docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/tmp/planning-docs) so the
  public docs directory can stay clean.
