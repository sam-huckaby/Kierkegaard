import type { Envelope, Topic } from "@federated-kafka/contracts";
import type {
  BrokerClientModule,
  BrokerModuleLoader,
  DriverConfig,
  EventHandler,
  FederatedBrokerSdk,
  MemoryDriverConfig,
  PublishOptions,
  ReplayOptions,
  RequestHandler,
  RequestOptions,
  RespondOptions,
  ServerDriverConfig,
  SubscribeOptions,
  TopicClient
} from "./types";
import { FederatedBrokerNotConfiguredError } from "./types";

type SdkGlobalState = {
  loader?: BrokerModuleLoader;
  modulePromise?: Promise<BrokerClientModule>;
};

const STATE_SYMBOL = Symbol.for("federated-kafka/sdk-state");

const getState = (): SdkGlobalState => {
  const host = globalThis as Record<symbol, SdkGlobalState | undefined>;
  if (!host[STATE_SYMBOL]) {
    host[STATE_SYMBOL] = {};
  }
  return host[STATE_SYMBOL];
};

const assertBrokerModule = (value: unknown): BrokerClientModule => {
  const asRecord = value as Record<string, unknown>;
  const requiredMethods = ["publish", "subscribe", "request", "respond", "replay", "stats", "setDriver"];
  const missing = requiredMethods.filter((name) => typeof asRecord[name] !== "function");
  if (missing.length > 0) {
    throw new Error(`Broker module is missing required methods: ${missing.join(", ")}`);
  }
  return value as BrokerClientModule;
};

const makeLoader = (loader?: BrokerModuleLoader): (() => Promise<BrokerClientModule>) => {
  if (loader) {
    let localPromise: Promise<BrokerClientModule> | undefined;
    return async () => {
      if (!localPromise) {
        localPromise = loader().then(assertBrokerModule);
      }
      return localPromise;
    };
  }

  return async () => {
    const state = getState();
    if (!state.loader) {
      throw new FederatedBrokerNotConfiguredError();
    }
    if (!state.modulePromise) {
      state.modulePromise = state.loader().then(assertBrokerModule);
    }
    return state.modulePromise;
  };
};

const withTopicClient = <TEvent, TRequest, TReply>(
  sdk: FederatedBrokerSdk,
  topic: Topic
): TopicClient<TEvent, TRequest, TReply> => ({
  publish: (payload, options) => sdk.publish(topic, payload, options) as Promise<Envelope<TEvent>>,
  subscribe: (handler, options) =>
    sdk.subscribe(
      topic,
      (envelope) => handler(envelope.payload as TEvent, envelope as Envelope<TEvent>),
      options
    ),
  request: (payload, options) => sdk.request<TReply>(topic, payload, options),
  respond: (handler, options) =>
    sdk.respond(
      topic,
      (payload, envelope) => handler(payload as TRequest, envelope as Envelope<TRequest>),
      options
    ),
  replay: (options) => sdk.replay(topic, options) as AsyncIterable<Envelope<TEvent>>
});

export const configureFederatedBroker = (loader: BrokerModuleLoader): void => {
  const state = getState();
  state.loader = loader;
  state.modulePromise = undefined;
};

export const createFederatedBrokerSdk = (loader?: BrokerModuleLoader): FederatedBrokerSdk => {
  const load = makeLoader(loader);

  const publish = async (topic: Topic, payload: unknown, options?: PublishOptions): Promise<Envelope> => {
    const broker = await load();
    return broker.publish(topic, payload, options);
  };

  const subscribe = (topicPattern: string, handler: EventHandler, options?: SubscribeOptions): (() => void) => {
    let unsubscribed = false;
    let unsubscribeFromBroker: (() => void) | undefined;

    void load().then((broker) => {
      if (unsubscribed) {
        return;
      }
      unsubscribeFromBroker = broker.subscribe(topicPattern, handler, options);
    });

    return () => {
      unsubscribed = true;
      unsubscribeFromBroker?.();
    };
  };

  const request = async <TReply = unknown>(
    topic: Topic,
    payload: unknown,
    options?: RequestOptions
  ): Promise<TReply> => {
    const broker = await load();
    return broker.request<TReply>(topic, payload, options);
  };

  const respond = (topicPattern: string, handler: RequestHandler, options?: RespondOptions): (() => void) => {
    let unsubscribed = false;
    let unsubscribeFromBroker: (() => void) | undefined;

    void load().then((broker) => {
      if (unsubscribed) {
        return;
      }
      unsubscribeFromBroker = broker.respond(topicPattern, handler, options);
    });

    return () => {
      unsubscribed = true;
      unsubscribeFromBroker?.();
    };
  };

  const replay = (topic: Topic, options?: ReplayOptions): AsyncIterable<Envelope> => ({
    async *[Symbol.asyncIterator](): AsyncIterator<Envelope> {
      const broker = await load();
      for await (const envelope of broker.replay(topic, options)) {
        yield envelope;
      }
    }
  });

  const stats = async (): Promise<Record<string, unknown>> => {
    const broker = await load();
    return broker.stats();
  };

  const setDriver = async (config: DriverConfig): Promise<void> => {
    const broker = await load();
    await broker.setDriver(config);
  };

  const setMemoryDriver = async (config: Omit<MemoryDriverConfig, "type"> = {}): Promise<void> => {
    await setDriver({ type: "memory", ...config });
  };

  const setServerDriver = async (config: Omit<ServerDriverConfig, "type">): Promise<void> => {
    await setDriver({ type: "server", ...config });
  };

  const topic = <TEvent = unknown, TRequest = unknown, TReply = unknown>(topicName: Topic): TopicClient<TEvent, TRequest, TReply> =>
    withTopicClient<TEvent, TRequest, TReply>(sdk, topicName);

  const sdk: FederatedBrokerSdk = {
    publish,
    subscribe,
    request,
    respond,
    replay,
    stats,
    setDriver,
    setMemoryDriver,
    setServerDriver,
    topic
  };

  return sdk;
};

const defaultSdk = createFederatedBrokerSdk();

export const publish = defaultSdk.publish;
export const subscribe = defaultSdk.subscribe;
export const request = defaultSdk.request;
export const respond = defaultSdk.respond;
export const replay = defaultSdk.replay;
export const stats = defaultSdk.stats;
export const setDriver = defaultSdk.setDriver;
export const setMemoryDriver = defaultSdk.setMemoryDriver;
export const setServerDriver = defaultSdk.setServerDriver;
export const topic = defaultSdk.topic;

export const unsafeResetFederatedBrokerForTests = (): void => {
  const state = getState();
  state.loader = undefined;
  state.modulePromise = undefined;
};

