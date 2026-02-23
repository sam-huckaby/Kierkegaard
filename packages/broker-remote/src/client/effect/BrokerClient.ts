import type { Envelope, Topic } from "@federated-kafka/contracts";
import { Context, Effect, Layer } from "effect";
import type {
  BrokerClient,
  DriverConfig,
  EventHandler,
  PublishOptions,
  ReplayOptions,
  RespondOptions,
  RequestHandler,
  RequestOptions,
  SubscribeOptions,
  Unsubscribe
} from "../types";
import { getBrokerClient } from "../index";

export class BrokerClientService extends Context.Tag("federated-kafka/BrokerClientService")<
  BrokerClientService,
  BrokerClient
>() {}

export const BrokerClientLayer = Layer.succeed(BrokerClientService, getBrokerClient());

export const setDriverEffect = (config: DriverConfig): Effect.Effect<void, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) => Effect.promise(() => client.setDriver(config)));

export const publishEffect = (
  topic: Topic,
  payload: unknown,
  options?: PublishOptions
): Effect.Effect<Envelope, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) => Effect.promise(() => client.publish(topic, payload, options)));

export const subscribeEffect = (
  pattern: string,
  handler: EventHandler,
  options?: SubscribeOptions
): Effect.Effect<Unsubscribe, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) =>
    Effect.sync(() => client.subscribe(pattern, handler, options))
  );

export const requestEffect = <TReply = unknown>(
  topic: Topic,
  payload: unknown,
  options?: RequestOptions
): Effect.Effect<TReply, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) => Effect.promise(() => client.request<TReply>(topic, payload, options)));

export const respondEffect = (
  pattern: string,
  handler: RequestHandler,
  options?: RespondOptions
): Effect.Effect<Unsubscribe, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) => Effect.sync(() => client.respond(pattern, handler, options)));

export const replayEffect = (
  topic: Topic,
  options?: ReplayOptions
): Effect.Effect<AsyncIterable<Envelope>, never, BrokerClientService> =>
  Effect.flatMap(BrokerClientService, (client) => Effect.sync(() => client.replay(topic, options)));

