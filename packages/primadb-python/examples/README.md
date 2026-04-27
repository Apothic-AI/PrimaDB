# Package Examples

These examples run directly against the local [`primadb-python` package](/home/bitnom/Code/gunport/primadb/packages/primadb-python).

Each example is a small `uv` project with a local editable dependency on `../..`, so the intended
flow is:

```bash
uv sync
uv run python main.py
```

Available projects:

- [examples/local_notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/local_notes/README.md): durable local notes, byte fields, and blob storage.
- [examples/mesh_peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/mesh_peer/README.md): relay-signaled mesh peer you can run in multiple terminals.
- [examples/full_node/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/full_node/README.md): a Python anchor-node example that hosts a local relay and joins the mesh through it.
- [examples/moq_sync/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/moq_sync/README.md): sync envelopes published through the Python MoQ path/track/frame helper.
