# Default Notes

This is a standalone browser project for the default `primadb` package entrypoint.

It demonstrates:

- `initPrimadb()` and `Primadb`
- IndexedDB segment persistence through `openDurableStorage(...)`
- blob storage through `openBlobStorage(...)`
- binary fields through `putBytes(...)`
- content-addressed blobs through `putBlob(...)`
- live note rendering through `chain.on(...)`

## Run

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
./examples/serve.sh
```

Open:

```text
http://127.0.0.1:4181/examples/default-notes/
```
