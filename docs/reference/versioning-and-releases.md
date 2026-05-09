---
title: Versioning And Releases
sidebar_position: 3
---

PrimaDB uses lockstep versioning across:

- [Cargo.toml](https://github.com/Apothic-AI/PrimaDB/tree/master/Cargo.toml)
- [packages/primadb/package.json](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb/package.json)
- [packages/primadb-node/package.json](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-node/package.json)
- [packages/primadb-python/pyproject.toml](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/pyproject.toml)

`Cargo.toml` is the source of truth.

## Version Sync Script

```bash
cd /path/to/primadb
node ./scripts/version-sync.mjs check
node ./scripts/version-sync.mjs sync
node ./scripts/version-sync.mjs set 0.1.1
node ./scripts/cut-release.mjs 0.1.1
```

`cut-release.mjs` creates the release commit and matching annotated `v*.*.*` tag locally. Push the
result with:

```bash
git push --follow-tags origin master
```

## GitHub Automation

Current GitHub Actions cover:

- version drift checking
- tagged release validation
- GitHub release creation
- release asset attachment

The release workflow creates GitHub release artifacts but does not publish to crates.io, npm, or
PyPI.
