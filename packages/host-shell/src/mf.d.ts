declare module "remoteA/App" {
  import type { ComponentType } from "react";
  const App: ComponentType;
  export default App;
}

declare module "remoteB/App" {
  import type { ComponentType } from "react";
  const App: ComponentType;
  export default App;
}

declare module "broker/devtools" {
  import type { ComponentType } from "react";
  const DevtoolsApp: ComponentType;
  export default DevtoolsApp;
}

declare module "broker/client" {
  export type DriverConfig =
    | { type: "memory"; ringBufferSize?: number; requestTimeoutMs?: number }
    | { type: "server"; wsUrl: string; httpUrl: string; requestTimeoutMs?: number };

  export const setDriver: (config: DriverConfig) => Promise<void>;
}

