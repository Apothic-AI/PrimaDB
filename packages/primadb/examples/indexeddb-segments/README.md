# IndexedDB Segment Regression

This browser-only regression page verifies that package IndexedDB segment persistence writes incremental transactions after the initial full flush.

Run it directly:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm run smoke:indexeddb
```

The test seeds a larger graph, repeatedly updates one large checkpoint value, then asserts that the persistence hook reports one full replacement and bounded incremental writes for the updates.
