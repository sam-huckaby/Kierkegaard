import { defineConfig } from "vitest/config";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "broker/client": path.resolve(rootDir, "test/mocks/broker-client.ts"),
      "remoteA/App": path.resolve(rootDir, "test/mocks/remote-a.tsx"),
      "remoteB/App": path.resolve(rootDir, "test/mocks/remote-b.tsx"),
      "broker/devtools": path.resolve(rootDir, "test/mocks/broker-devtools.tsx")
    }
  },
  test: {
    include: ["test/**/*.test.ts", "test/**/*.test.tsx"],
    environment: "jsdom",
    setupFiles: ["./test/setup.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "html"]
    }
  }
});

