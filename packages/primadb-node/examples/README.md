# Package Examples

These examples run directly against the local [`primadb-node` package](..).

Build the addon first:

```bash
cd /path/to/primadb/packages/primadb-node
pnpm install
pnpm run build
```

Available projects:

- [examples/local-notes/README.md](local-notes/README.md): local durable storage, byte fields, and blob storage through the native addon.
- [examples/mesh-peer/README.md](mesh-peer/README.md): a relay-signaled mesh peer you can run in multiple terminals.
- [examples/full-node/README.md](full-node/README.md): a Node anchor-node example that hosts a local relay and joins the mesh through it.
- [examples/moq-sync/README.md](moq-sync/README.md): sync envelopes published over a MoQ track using the Node helper.
