import type { Envelope, Topic } from "@kierkegaard/contracts";

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

export type EventHandler = (envelope: Envelope) => void | Promise<void>;

export type RequestHandler = (payload: unknown, envelope: Envelope) => unknown | Promise<unknown>;

export interface BrokerDriver {
  publish(topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope>;
  subscribe(topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe;
  request<TReply = unknown>(topic: Topic, payload: unknown, options?: RequestOptions): Promise<TReply>;
  respond(topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe;
  replay(topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope>;
  stats?(): Record<string, unknown>;
  close?(): Promise<void> | void;
}

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
  webSocketCtor?: WebSocketFactory;
  fetchFn?: typeof fetch;
};

export type DriverConfig = MemoryDriverConfig | ServerDriverConfig;

export type BrokerClient = {
  publish(topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope>;
  subscribe(topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe;
  request<TReply = unknown>(topic: Topic, payload: unknown, options?: RequestOptions): Promise<TReply>;
  respond(topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe;
  replay(topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope>;
  stats(): Record<string, unknown>;
  setDriver(config: DriverConfig): Promise<void>;
};

export class BrokerTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BrokerTimeoutError";
  }
}

export class BrokerResponseError extends Error {
  public readonly correlationId?: string;

  constructor(message: string, correlationId?: string) {
    super(message);
    this.name = "BrokerResponseError";
    this.correlationId = correlationId;
  }
}

export class BrokerUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BrokerUnavailableError";
  }
}

export type WebSocketLike = {
  readyState: number;
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent<string | Buffer | ArrayBuffer>) => void) | null;
  onerror: ((event: Event) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
};

export type WebSocketFactory = new (url: string) => WebSocketLike;

