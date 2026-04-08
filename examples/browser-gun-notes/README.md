# browser-gun-notes

`browser-gun-notes` is the Gun-compatible Primadb browser demo. It runs the
browser runtime from [`js/primadb-gun.js`](/home/bitnom/Code/gunport/primadb/js/primadb-gun.js)
on top of the Primadb WASM bindings and exercises:

- `gun.user().create(...)`, `auth(...)`, `recall(...)`, and `leave()`
- signed user profile data on the `~pub/...` graph
- shared notes through `get().set().open()` chains
- peer discovery and sync through the DAM relay example

## Run it

1. Build the WASM package with crypto enabled:

   ```bash
   ./examples/browser-gun-notes/build.sh
   ```

2. Start the relay in another terminal:

   ```bash
   cargo run --example ws_relay_server -- 127.0.0.1:9010
   ```

3. Serve the repo root:

   ```bash
   ./examples/browser-gun-notes/serve.sh
   ```

4. Open:

   ```text
   http://127.0.0.1:4173/examples/browser-gun-notes/
   ```

5. Open the page in a second tab or browser and sign in as another user. Peer
   discovery happens over the relay, and shared notes sync across clients.
