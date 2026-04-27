import { resolve } from "node:path";
import { defineConfig } from "vite";

const host = process.env.PRIMADB_PACKAGE_HOST ?? "127.0.0.1";
const port = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const packageRoot = resolve(import.meta.dirname, "..");

const headers = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "same-origin",
  "Origin-Agent-Cluster": "?1",
  "Cache-Control": "no-store",
};

export default defineConfig({
  optimizeDeps: {
    exclude: ["primadb", "primadb/threads", "primadb/gun"],
  },
  server: {
    host,
    port,
    strictPort: true,
    headers,
    fs: {
      allow: [packageRoot],
    },
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
        binaryStreamRoom: resolve(import.meta.dirname, "binary-stream-room/index.html"),
        defaultNotes: resolve(import.meta.dirname, "default-notes/index.html"),
        moqSync: resolve(import.meta.dirname, "moq-sync/index.html"),
        textVoiceChat: resolve(import.meta.dirname, "text-voice-chat/index.html"),
        threadedMesh: resolve(import.meta.dirname, "threaded-mesh/index.html"),
      },
      output: {
        preserveModules: true,
      },
    },
  },
});
