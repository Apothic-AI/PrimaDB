# browser-notes

`browser-notes` is a backend-free browser example for Primadb.

It demonstrates:

- Primadb compiled to WebAssembly.
- Automatic IndexedDB persistence hooks.
- Reactive UI updates via `chain.on(...)`.
- Filtered and ordered queries via `chain.query(...)`.
- Cross-tab sync over `BroadcastChannel`.

## Run

1. Install `wasm-pack` if you do not already have it:

```bash
cargo install wasm-pack
```

2. Build the WASM package from the Primadb repo root:

```bash
./examples/browser-notes/build.sh
```

3. Serve the example:

```bash
./examples/browser-notes/serve.sh
```

4. Open `http://127.0.0.1:4173/examples/browser-notes/` in your browser.

Open a second tab to watch cross-tab sync happen in real time.

## Notes

- IndexedDB is used when available.
- If IndexedDB setup fails, the example falls back to Primadb's `localStorage` persistence.
- Archiving removes the item from the underlying Primadb set membership instead of using a soft-delete workaround.
