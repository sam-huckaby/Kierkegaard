declare module "broker/client" {
  import type { Envelope } from "@federated-kafka/contracts";

  export type DriverConfig =
    | { type: "memory"; ringBufferSize?: number; requestTimeoutMs?: number }
    | { type: "server"; wsUrl: string; httpUrl: string; requestTimeoutMs?: number };

  export const publish: (topic: string, payload: unknown) => Promise<Envelope>;
  export const request: <TReply = unknown>(
    topic: string,
    payload: unknown,
    options?: { timeoutMs?: number }
  ) => Promise<TReply>;
  export const setDriver: (config: DriverConfig) => Promise<void>;
}

