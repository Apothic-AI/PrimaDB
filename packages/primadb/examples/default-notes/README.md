# Default Notes

This is a Vite browser example for the default `primadb` package entrypoint.

It demonstrates:

- `initPrimadb()` and `Primadb`
- IndexedDB segment persistence through `openDurableStorage(...)`
- blob storage through `openBlobStorage(...)`
- binary fields through `putBytes(...)`
- content-addressed blobs through `putBlob(...)`
- live note rendering through `chain.on(...)`

## Run

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/default-notes/
```
