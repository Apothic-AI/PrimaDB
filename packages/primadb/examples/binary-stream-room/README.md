# Binary Stream Room

This Vite browser example streams browser media chunks through PrimaDB byte fields.

It demonstrates:

- `MediaRecorder` chunk capture
- `Chain.putBytes(...)` for live media payloads
- a rolling per-publisher graph buffer
- mesh replication of byte chunks
- local relay signaling by default
- synthetic capture for permission-free testing

The example does not set a room-size cap. Each publisher keeps its own rolling chunk window so live
buffers do not grow without bound.

## Run

Start the local relay:

```bash
cd /path/to/primadb
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

Run the package examples app:

```bash
cd /path/to/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/binary-stream-room/
```

Use `Synthetic` source if you want to test without camera or microphone permission.

## URL Options

- `room`: room name. Defaults to `package-binary-stream`.
- `name`: participant display name.
- `signal`: `relay` or `broadcast`. Defaults to `relay`.
- `relay`: relay WebSocket URL. Defaults to `ws://127.0.0.1:9010`.
- `capture`: `camera` or `synthetic`.
- `autostart`: set to `1` to start publishing on load.
- `chunkMs`: recorder chunk cadence. Defaults to `500`.
- `windowMs`: rolling graph buffer window. Defaults to `8000`.
- `bitrate`: recorder video bitrate. Defaults to `260000`.
- `ice`: repeated STUN/TURN server entries.

Same-browser test mode:

```text
http://127.0.0.1:4181/binary-stream-room/?signal=broadcast&capture=synthetic
```

## Smoke Test

```bash
cd /path/to/primadb/packages/primadb/examples
bash ./binary-stream-room/test-smoke.sh
```
