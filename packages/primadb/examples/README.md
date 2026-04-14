# Package Examples

These examples run directly against the local [`primadb` package](/home/bitnom/Code/gunport/primadb/packages/primadb).

Build the package first:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
```

Start the package example server:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
./examples/serve.sh
```

That serves the package root with COOP/COEP headers, which means both default and threaded
examples work from the same host.

Available projects:

- [examples/default-notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/default-notes/README.md): local-first notes board using the default browser build, IndexedDB segment persistence, byte fields, and blob storage.
- [examples/threaded-mesh/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/threaded-mesh/README.md): threaded browser build using `primadb/threads`, shared room replication over mesh, and optional relay signaling for cross-browser peers.
