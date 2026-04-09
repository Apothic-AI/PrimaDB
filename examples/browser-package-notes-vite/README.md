# Browser Package Notes Vite Example

`browser-package-notes-vite` is a real browser app that consumes the in-repo npm package in
[packages/primadb](/home/bitnom/Code/gunport/primadb/packages/primadb) through Vite instead of
importing raw generated WASM bindings directly.

It demonstrates:

- `npm install` against the local `primadb` package
- `Primadb` and `initPrimadb()` imports from the package entrypoint
- IndexedDB segment persistence through `openDurableStorage(...)`
- live note rendering through package-level chain subscriptions
- optional relay-signaled WebRTC mesh mode via `?room=...&relay=...`

## Run

```bash
cd /home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite
npm install
npm run dev
```

Open:

```text
http://127.0.0.1:4182/
```

For shared mesh mode, append a room and relay:

```text
http://127.0.0.1:4182/?room=demo-room&signal=relay&relay=ws://127.0.0.1:9010
```

## Smoke Test

```bash
cd /home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite
npm run smoke
```

The smoke test builds the Vite app, starts `vite preview`, opens Chromium through
`playwright-core`, creates a note, reloads the page, and confirms the note persisted through the
package-backed Primadb runtime.
