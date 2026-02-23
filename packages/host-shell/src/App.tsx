import { lazy, Suspense, useEffect, useState } from "react";
import { setDriver } from "broker/client";

const RemoteAApp = lazy(() => import("remoteA/App"));
const RemoteBApp = lazy(() => import("remoteB/App"));
const BrokerDevtools = lazy(() => import("broker/devtools"));

const resolveDriverConfig = () => {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env ?? {};
  const mode = (env.VITE_BROKER_DRIVER ?? "memory").toLowerCase();
  if (mode === "server") {
    return {
      type: "server" as const,
      wsUrl: env.VITE_BROKER_WS_URL ?? "ws://localhost:7777/ws",
      httpUrl: env.VITE_BROKER_HTTP_URL ?? "http://localhost:7777"
    };
  }
  return {
    type: "memory" as const,
    ringBufferSize: Number(env.VITE_BROKER_RING_BUFFER_SIZE ?? "100")
  };
};

export const App = (): JSX.Element => {
  const [showDevtools, setShowDevtools] = useState(true);
  const [driverLabel, setDriverLabel] = useState<string>("initializing");

  useEffect(() => {
    const config = resolveDriverConfig();
    setDriverLabel(config.type);
    void setDriver(config);
  }, []);

  return (
    <main style={{ fontFamily: "sans-serif", padding: 16 }}>
      <h1>Federated Kafka Host Shell</h1>
      <p data-testid="driver-mode">Driver mode: {driverLabel}</p>

      <div style={{ display: "grid", gap: 12, gridTemplateColumns: "1fr 1fr" }}>
        <Suspense fallback={<div>Loading Remote A...</div>}>
          <RemoteAApp />
        </Suspense>
        <Suspense fallback={<div>Loading Remote B...</div>}>
          <RemoteBApp />
        </Suspense>
      </div>

      <button type="button" onClick={() => setShowDevtools((current) => !current)} style={{ marginTop: 12 }}>
        Toggle Broker Devtools
      </button>
      {showDevtools ? (
        <Suspense fallback={<div>Loading broker devtools...</div>}>
          <BrokerDevtools />
        </Suspense>
      ) : null}
    </main>
  );
};

export default App;

