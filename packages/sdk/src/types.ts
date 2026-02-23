import type { Envelope, Topic } from "@federated-kafka/contracts";

export type Unsubscribe = () => void;

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

export type MemoryDriverConfig = {
  type: "memory";
  ringBufferSize?: number;
  clientId?: string;
  requestTimeoutMs?: number;
};

export type ServerDriverConfig = {
  type: "server";
  wsUrl: string;
  httpUrl: string;
  clientId?: string;
  requestTimeoutMs?: number;
  ackTimeoutMs?: number;
  reconnectMs?: number;
};

export type DriverConfig = MemoryDriverConfig | ServerDriverConfig;

export type EventHandler = (envelope: Envelope) => void | Promise<void>;
export type RequestHandler = (payload: unknown, envelope: Envelope) => unknown | Promise<unknown>;

export type BrokerClientModule = {
  publish(topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope>;
  subscribe(topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe;
  request<TReply = unknown>(topic: Topic, payload: unknown, options?: RequestOptions): Promise<TReply>;
  respond(topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe;
  replay(topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope>;
  stats(): Record<string, unknown>;
  setDriver(config: DriverConfig): Promise<void>;
};

export type BrokerModuleLoader = () => Promise<BrokerClientModule>;

export type TopicClient<TEvent = unknown, TRequest = unknown, TReply = unknown> = {
  publish(payload: TEvent, options?: PublishOptions): Promise<Envelope<TEvent>>;
  subscribe(handler: (payload: TEvent, envelope: Envelope<TEvent>) => void | Promise<void>, options?: SubscribeOptions): Unsubscribe;
  request(payload: TRequest, options?: RequestOptions): Promise<TReply>;
  respond(
    handler: (payload: TRequest, envelope: Envelope<TRequest>) => TReply | Promise<TReply>,
    options?: RespondOptions
  ): Unsubscribe;
  replay(options?: ReplayOptions): AsyncIterable<Envelope<TEvent>>;
};

export type FederatedBrokerSdk = {
  publish(topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope>;
  subscribe(topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe;
  request<TReply = unknown>(topic: Topic, payload: unknown, options?: RequestOptions): Promise<TReply>;
  respond(topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe;
  replay(topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope>;
  stats(): Promise<Record<string, unknown>>;
  setDriver(config: DriverConfig): Promise<void>;
  setMemoryDriver(config?: Omit<MemoryDriverConfig, "type">): Promise<void>;
  setServerDriver(config: Omit<ServerDriverConfig, "type">): Promise<void>;
  topic<TEvent = unknown, TRequest = unknown, TReply = unknown>(topic: Topic): TopicClient<TEvent, TRequest, TReply>;
};

export class FederatedBrokerNotConfiguredError extends Error {
  constructor(message = "Federated broker SDK is not configured. Call configureFederatedBroker(() => import('broker/client')).") {
    super(message);
    this.name = "FederatedBrokerNotConfiguredError";
  }
}

