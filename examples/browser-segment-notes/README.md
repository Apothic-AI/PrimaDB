# browser-segment-notes

`browser-segment-notes` is a backend-free browser example for Primadb's segment-backed
IndexedDB persistence path.

It demonstrates:

- Primadb compiled to WebAssembly.
- Canonical node/index records persisted in IndexedDB via `enableIndexedDbSegmentPersistence(...)`.
- Reactive UI updates via `chain.on(...)`.
- Filtered and ordered queries via `chain.query(...)`.
- Cross-tab sync over `BroadcastChannel`.

## Run

1. Build the WASM package from the Primadb repo root:

```bash
./examples/browser-segment-notes/build.sh
```

2. Serve the example:

```bash
./examples/browser-segment-notes/serve.sh
```

3. Open `http://127.0.0.1:4176/examples/browser-segment-notes/` in your browser.

Open a second tab to watch cross-tab sync happen in real time, then reload either tab to verify
that the task list restores from the segment-backed IndexedDB store.
