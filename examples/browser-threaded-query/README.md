# Browser Threaded Query

This example is the opt-in `wasm-threads` build for Primadb.

It exists separately from the default browser examples so the normal `wasm32-unknown-unknown`
build stays on the simpler stable path, while this example can require the extra pieces that
threaded WASM needs:

- `wasm-threads` feature
- nightly toolchain with `-Z build-std`
- `SharedArrayBuffer`
- COOP/COEP response headers

Build:

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm-threads.sh --out-dir examples/browser-threaded-query/pkg --features wasm-threads
```

or use the example wrapper:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-query/build.sh
```

Serve:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-query/serve.sh
```

Open:

```text
http://127.0.0.1:4174/examples/browser-threaded-query/
```

The page initializes `initThreadPool(...)` before constructing `Primadb`, seeds 4,000 notes, and
then runs a query through the Rayon-backed path. The UI reports whether parallel mode is active and
how many threads Rayon sees.
