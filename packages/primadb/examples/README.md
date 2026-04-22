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
- [examples/threaded-mesh/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/threaded-mesh/README.md): threaded browser build using `primadb/threads`, shared room replication over mesh, and optional relay signaling for cross-browser peers.
