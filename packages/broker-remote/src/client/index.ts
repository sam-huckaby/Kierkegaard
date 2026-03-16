import type { Envelope, Topic } from "@kierkegaard/contracts";
import { MemoryBrokerDriver } from "../drivers/memory/MemoryBrokerDriver";
import { ServerBrokerDriver } from "../drivers/server/ServerBrokerDriver";
import type {
  BrokerClient,
  BrokerDriver,
  DriverConfig,
  EventHandler,
  PublishOptions,
  ReplayOptions,
  RespondOptions,
  RequestHandler,
  RequestOptions,
  SubscribeOptions,
  Unsubscribe
} from "./types";

const BROKER_SYMBOL = Symbol.for("kierkegaard/broker");

type BrokerGlobal = {
  [BROKER_SYMBOL]?: FederatedBrokerClient;
};

const resolveDefaultConfig = (): DriverConfig => {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env ?? {};
  const mode = (env.BROKER_DRIVER ?? "memory").toLowerCase();
  if (mode === "server") {
    return {
      type: "server",
      wsUrl: env.BROKER_WS_URL ?? "ws://localhost:7777/ws",
      httpUrl: env.BROKER_HTTP_URL ?? "http://localhost:7777"
    };
  }

  return {
    type: "memory",
    ringBufferSize: Number(env.BROKER_RING_BUFFER_SIZE ?? "100")
  };
};

const createDriver = (config: DriverConfig): BrokerDriver => {
  if (config.type === "memory") {
    return new MemoryBrokerDriver(config);
  }
  return new ServerBrokerDriver(config);
};

class FederatedBrokerClient implements BrokerClient {
  private driver: BrokerDriver;

  private config: DriverConfig;

  constructor(initialConfig: DriverConfig) {
    this.config = initialConfig;
    this.driver = createDriver(initialConfig);
  }

  async setDriver(config: DriverConfig): Promise<void> {
    if (this.driver.close) {
      await this.driver.close();
    }
    this.config = config;
    this.driver = createDriver(config);
  }

  publish(topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope> {
    return this.driver.publish(topic, payload, options);
  }

  subscribe(topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe {
    return this.driver.subscribe(topicPattern, handler, options);
  }

  request<TReply = unknown>(topic: Topic, payload: unknown, options?: RequestOptions): Promise<TReply> {
    return this.driver.request(topic, payload, options);
  }

  respond(topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe {
    return this.driver.respond(topicPattern, handler, options);
  }

  replay(topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope> {
    return this.driver.replay(topic, options);
  }

  stats(): Record<string, unknown> {
    return {
      ...this.driver.stats?.(),
      config: this.config
    };
  }
}

const getOrCreateSingleton = (): FederatedBrokerClient => {
  const host = globalThis as BrokerGlobal;
  if (!host[BROKER_SYMBOL]) {
    host[BROKER_SYMBOL] = new FederatedBrokerClient(resolveDefaultConfig());
  }
  return host[BROKER_SYMBOL];
};

const singletonClient = getOrCreateSingleton();

export const getBrokerClient = (): BrokerClient => singletonClient;
export const setDriver = (config: DriverConfig): Promise<void> => singletonClient.setDriver(config);
export const publish = (topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope> =>
  singletonClient.publish(topic, payload, options);
export const subscribe = (topicPattern: string, handler: EventHandler, options?: SubscribeOptions): Unsubscribe =>
  singletonClient.subscribe(topicPattern, handler, options);
export const request = <TReply = unknown>(
  topic: Topic,
  payload: unknown,
  options?: RequestOptions
): Promise<TReply> => singletonClient.request<TReply>(topic, payload, options);
export const respond = (topicPattern: string, handler: RequestHandler, options?: RespondOptions): Unsubscribe =>
  singletonClient.respond(topicPattern, handler, options);
export const replay = (topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope> =>
  singletonClient.replay(topic, options);
export const stats = (): Record<string, unknown> => singletonClient.stats();

export type * from "./types";

