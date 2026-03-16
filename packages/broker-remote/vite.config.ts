import federation from "@originjs/vite-plugin-federation";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    react(),
    federation({
      name: "broker",
      filename: "remoteEntry.js",
      exposes: {
        "./client": "./src/client/index.ts",
        "./contracts": "./src/contracts.ts",
        "./devtools": "./src/devtools/DevtoolsApp.tsx"
      },
      shared: ["react", "react-dom", "effect", "@kierkegaard/contracts"]
    })
  ],
  build: {
    target: "esnext",
    minify: false
  },
  preview: {
    port: 4173
  }
});

