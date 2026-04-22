import { resolve } from "node:path";
import { defineConfig } from "vite";

const host = process.env.PRIMADB_PACKAGE_HOST ?? "127.0.0.1";
const port = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");

const headers = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "same-origin",
  "Origin-Agent-Cluster": "?1",
  "Cache-Control": "no-store",
};

export default defineConfig({
  server: {
    host,
    port,
    strictPort: true,
    headers,
  },
  preview: {
    host,
    port,
    strictPort: true,
    headers,
  },
  build: {
    rollupOptions: {
      preserveEntrySignatures: "strict",
      input: {
        index: resolve(import.meta.dirname, "index.html"),
        defaultNotes: resolve(import.meta.dirname, "default-notes/index.html"),
        threadedMesh: resolve(import.meta.dirname, "threaded-mesh/index.html"),
      },
      output: {
        preserveModules: true,
      },
    },
  },
});
