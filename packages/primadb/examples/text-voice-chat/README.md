# Text + Voice Chat

This Vite browser example uses PrimaDB as the transport for both text messages and voice chunks.

It demonstrates:

- text chat stored as normal graph records
- voice capture with `MediaRecorder`
- voice chunks written with `Chain.putBytes(...)`
- remote playback from a rolling per-speaker graph buffer
- relay-backed mesh signaling by default
- synthetic byte publishing for permission-free transport testing

The example does not set a room-size cap. Each speaker manages a rolling local voice buffer so live
audio chunks do not accumulate forever.

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
http://127.0.0.1:4181/text-voice-chat/
```

Use `Synthetic` source if you want to test PrimaDB byte transport without microphone permission.
Use `Microphone` for actual browser voice capture and remote playback.

## URL Options

- `room`: room name. Defaults to `package-text-voice-chat`.
- `name`: participant display name.
- `message`: optional text message inserted on load.
- `signal`: `relay` or `broadcast`. Defaults to `relay`.
- `relay`: relay WebSocket URL. Defaults to `ws://127.0.0.1:9010`.
- `capture`: `microphone` or `synthetic`.
- `autostart`: set to `1` to start voice publishing on load.
- `chunkMs`: audio recorder chunk cadence. Defaults to `350`.
- `windowMs`: rolling voice buffer window. Defaults to `6000`.
- `bitrate`: recorder audio bitrate. Defaults to `48000`.
- `ice`: repeated STUN/TURN server entries.

Same-browser test mode:

```text
http://127.0.0.1:4181/text-voice-chat/?signal=broadcast&capture=synthetic
```

## Smoke Test

```bash
cd /path/to/primadb/packages/primadb/examples
bash ./text-voice-chat/test-smoke.sh
```
