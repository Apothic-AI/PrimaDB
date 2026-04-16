# Package Examples

These examples run directly against the local [`primadb-node` package](/home/bitnom/Code/gunport/primadb/packages/primadb-node).

Build the addon first:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
pnpm install
pnpm run build
```

Available projects:

- [examples/local-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/local-notes/README.md): local durable storage, byte fields, and blob storage through the native addon.
- [examples/mesh-peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/mesh-peer/README.md): a relay-signaled mesh peer you can run in multiple terminals.
