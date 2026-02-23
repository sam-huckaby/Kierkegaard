declare module "broker/client" {
  import type { Envelope } from "@federated-kafka/contracts";

  export type PublishOptions = {
    key?: string;
    producer?: string;
    correlationId?: string;
    causationId?: string;
    replyTo?: string;
    kind?: Envelope["kind"];
    schemaVersion?: number;
  };

  export type SubscribeOptions = {
    replayFromOffset?: number;
    replayLastN?: number;
  };

  export type RequestOptions = {
    timeoutMs?: number;
    signal?: AbortSignal;
    producer?: string;
    key?: string;
  };

  export type RespondOptions = {
    producer?: string;
  };

  export type ReplayOptions = {
    fromOffset?: number;
    lastN?: number;
  };

  export type DriverConfig =
    | {
        type: "memory";
        ringBufferSize?: number;
        requestTimeoutMs?: number;
        clientId?: string;
      }
    | {
        type: "server";
        wsUrl: string;
        httpUrl: string;
        requestTimeoutMs?: number;
        ackTimeoutMs?: number;
        reconnectMs?: number;
        clientId?: string;
      };

  export const publish: (topic: string, payload: unknown, options?: PublishOptions) => Promise<Envelope>;
  export const subscribe: (
    pattern: string,
    handler: (envelope: Envelope) => void | Promise<void>,
    options?: SubscribeOptions
  ) => () => void;
  export const request: <TReply = unknown>(
    topic: string,
    payload: unknown,
    options?: RequestOptions
  ) => Promise<TReply>;
  export const respond: (
    pattern: string,
    handler: (payload: unknown, envelope: Envelope) => unknown | Promise<unknown>,
    options?: RespondOptions
  ) => () => void;
  export const replay: (topic: string, options?: ReplayOptions) => AsyncIterable<Envelope>;
  export const stats: () => Record<string, unknown>;
  export const setDriver: (config: DriverConfig) => Promise<void>;
}

