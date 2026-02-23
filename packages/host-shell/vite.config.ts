import federation from "@originjs/vite-plugin-federation";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    react(),
    federation({
      name: "hostShell",
      remotes: {
        broker: "http://localhost:4173/assets/remoteEntry.js",
        remoteA: "http://localhost:4171/assets/remoteEntry.js",
        remoteB: "http://localhost:4172/assets/remoteEntry.js"
      },
      shared: {
        react: { singleton: true },
        "react-dom": { singleton: true }
      }
    })
  ],
  build: {
    target: "esnext",
    minify: false
  }
});

