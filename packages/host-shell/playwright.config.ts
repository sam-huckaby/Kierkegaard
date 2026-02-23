import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  use: {
    baseURL: "http://127.0.0.1:4170",
    headless: true
  },
  webServer: [
    {
      command: "pnpm -F @kierkegaard/server dev",
      url: "http://127.0.0.1:7777/health",
      timeout: 120_000,
      reuseExistingServer: true
    },
    {
      command:
        "pnpm -F @kierkegaard/broker-remote build && pnpm -F @kierkegaard/broker-remote exec vite preview --host 127.0.0.1 --port 4173",
      url: "http://127.0.0.1:4173",
      timeout: 180_000,
      reuseExistingServer: true
    },
    {
      command:
        "pnpm -F @kierkegaard/remote-a build && pnpm -F @kierkegaard/remote-a exec vite preview --host 127.0.0.1 --port 4171",
      url: "http://127.0.0.1:4171",
      timeout: 180_000,
      reuseExistingServer: true
    },
    {
      command:
        "pnpm -F @kierkegaard/remote-b build && pnpm -F @kierkegaard/remote-b exec vite preview --host 127.0.0.1 --port 4172",
      url: "http://127.0.0.1:4172",
      timeout: 180_000,
      reuseExistingServer: true
    },
    {
      command:
        "VITE_BROKER_DRIVER=server VITE_BROKER_WS_URL=ws://127.0.0.1:7777/ws VITE_BROKER_HTTP_URL=http://127.0.0.1:7777 pnpm -F @kierkegaard/host-shell build && pnpm -F @kierkegaard/host-shell exec vite preview --host 127.0.0.1 --port 4170",
      url: "http://127.0.0.1:4170",
      timeout: 180_000,
      reuseExistingServer: true
    }
  ]
});

