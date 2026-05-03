# Package Examples

These examples run directly against the local [`primadb` package](/home/bitnom/Code/gunport/primadb/packages/primadb)
through a small Vite app rooted in this directory.

Install and run the examples:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

That Vite app serves both the default and threaded examples from the same host with the COOP/COEP
headers required for the threaded build.

Open:

```text
http://127.0.0.1:4181/
```

For a production-style static build:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm run build
pnpm run preview
```

Available projects:

- [examples/default-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/default-notes/README.md): local-first notes board using the default browser build, IndexedDB segment persistence, byte fields, and blob storage.
- [examples/indexeddb-segments/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/indexeddb-segments/README.md): browser regression that verifies IndexedDB segment persistence uses bounded incremental writes after the initial full flush.
- [examples/opfs-segments/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/opfs-segments/README.md): browser regression that verifies OPFS segment persistence uses bounded incremental writes after the initial full flush.
- [examples/threaded-mesh/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/threaded-mesh/README.md): threaded browser build using `primadb/threads`, shared room replication over mesh, and optional relay signaling for cross-browser peers.
- [examples/binary-stream-room/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/binary-stream-room/README.md): browser media chunks streamed through PrimaDB byte fields with a rolling graph buffer and mesh replication.
- [examples/text-voice-chat/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/text-voice-chat/README.md): text messages and voice chunks transported through PrimaDB over the mesh.
- [examples/moq-sync/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/moq-sync/README.md): sync envelopes published over a MoQ track using the `primadb/moq` helper.
