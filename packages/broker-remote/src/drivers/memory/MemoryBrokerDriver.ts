import {
  createEnvelope,
  matchTopicPattern,
  validateTopicPattern,
  type Envelope,
  type Topic
} from "@federated-kafka/contracts";
import { v7 as uuidv7 } from "uuid";
import {
  BrokerResponseError,
  BrokerTimeoutError,
  type BrokerDriver,
  type EventHandler,
  type MemoryDriverConfig,
  type PublishOptions,
  type ReplayOptions,
  type RespondOptions,
  type RequestHandler,
  type RequestOptions,
  type SubscribeOptions,
  type Unsubscribe
} from "../../client/types";

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
  signalCleanup?: () => void;
};

type Subscriber = {
  id: string;
  pattern: string;
  handler: EventHandler;
};

type Responder = {
  id: string;
  pattern: string;
  handler: RequestHandler;
};

export class MemoryBrokerDriver implements BrokerDriver {
  private readonly ringBufferSize: number;

  private readonly requestTimeoutMs: number;

  private readonly clientId: string;

  private readonly subscribers = new Map<string, Subscriber>();

  private readonly responders = new Map<string, Responder>();

  private readonly pendingRequests = new Map<string, PendingRequest>();

  private readonly topicBuffers = new Map<string, Envelope[]>();

  private readonly topicOffsets = new Map<string, number>();

  constructor(config: MemoryDriverConfig = { type: "memory" }) {
    this.ringBufferSize = config.ringBufferSize ?? 100;
    this.requestTimeoutMs = config.requestTimeoutMs ?? 5000;
    this.clientId = config.clientId ?? "memory-client";
  }

  async publish(topic: Topic, payload: unknown, options: PublishOptions = {}): Promise<Envelope> {
    const envelope = createEnvelope({
      topic,
      payload,
      key: options.key,
      producer: options.producer,
      correlationId: options.correlationId,
      causationId: options.causationId,
      replyTo: options.replyTo,
      kind: options.kind ?? "event",
      schemaVersion: options.schemaVersion
    });
    return this.publishEnvelope(envelope);
  }

  subscribe(topicPattern: string, handler: EventHandler, options: SubscribeOptions = {}): Unsubscribe {
    validateTopicPattern(topicPattern);
    const id = uuidv7();
    const subscriber: Subscriber = { id, pattern: topicPattern, handler };
    this.subscribers.set(id, subscriber);

    if (options.replayFromOffset !== undefined || options.replayLastN !== undefined) {
      void this.replayBuffered(topicPattern, {
        fromOffset: options.replayFromOffset,
        lastN: options.replayLastN
      }).then((events) => {
        events.forEach((event) => {
          void Promise.resolve(handler(event));
        });
      });
    }

    return () => {
      this.subscribers.delete(id);
    };
  }

  request<TReply = unknown>(topic: Topic, payload: unknown, options: RequestOptions = {}): Promise<TReply> {
    const correlationId = uuidv7();
    const timeoutMs = options.timeoutMs ?? this.requestTimeoutMs;
    const replyTo = `_reply.${this.clientId}`;
    const requestEnvelope = createEnvelope({
      topic,
      payload,
      key: options.key,
      producer: options.producer ?? this.clientId,
      correlationId,
      replyTo,
      kind: "request"
    });

    if (options.signal?.aborted) {
      return Promise.reject(new BrokerTimeoutError("Request was aborted before publishing."));
    }

    return new Promise<TReply>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(new BrokerTimeoutError(`Request timed out for topic '${topic}'.`));
      }, timeoutMs);

      const pending: PendingRequest = {
        resolve: (value) => {
          clearTimeout(timeout);
          pending.signalCleanup?.();
          resolve(value as TReply);
        },
        reject: (error) => {
          clearTimeout(timeout);
          pending.signalCleanup?.();
          reject(error);
        },
        timeout
      };

      if (options.signal) {
        const abortHandler = () => {
          this.pendingRequests.delete(correlationId);
          pending.reject(new BrokerTimeoutError("Request aborted."));
        };
        options.signal.addEventListener("abort", abortHandler, { once: true });
        pending.signalCleanup = () => {
          options.signal?.removeEventListener("abort", abortHandler);
        };
      }

      this.pendingRequests.set(correlationId, pending);
      void this.publishEnvelope(requestEnvelope).catch((error: unknown) => {
        this.pendingRequests.delete(correlationId);
        pending.reject(error instanceof Error ? error : new Error(String(error)));
      });
    });
  }

  respond(topicPattern: string, handler: RequestHandler, _options: RespondOptions = {}): Unsubscribe {
    validateTopicPattern(topicPattern);
    const id = uuidv7();
    this.responders.set(id, { id, pattern: topicPattern, handler });

    return () => {
      this.responders.delete(id);
    };
  }

  async *replay(topic: Topic, options: ReplayOptions = {}): AsyncIterable<Envelope> {
    const events = await this.replayBuffered(topic, options);
    for (const event of events) {
      yield event;
    }
  }

  stats(): Record<string, unknown> {
    return {
      mode: "memory",
      subscribers: this.subscribers.size,
      responders: this.responders.size,
      pendingRequests: this.pendingRequests.size,
      topicsInBuffer: this.topicBuffers.size
    };
  }

  close(): void {
    this.pendingRequests.forEach((pending) => {
      clearTimeout(pending.timeout);
      pending.signalCleanup?.();
      pending.reject(new BrokerTimeoutError("Memory broker shut down."));
    });
    this.pendingRequests.clear();
    this.subscribers.clear();
    this.responders.clear();
  }

  private async publishEnvelope(envelope: Envelope): Promise<Envelope> {
    const persisted = this.persist(envelope);

    if (persisted.kind === "reply" || persisted.kind === "error") {
      this.resolvePendingRequest(persisted);
    }

    this.subscribers.forEach(({ pattern, handler }) => {
      if (!matchTopicPattern(pattern, persisted.topic)) {
        return;
      }
      void Promise.resolve(handler(persisted));
    });

    if (persisted.kind === "request") {
      this.dispatchToResponders(persisted);
    }

    return persisted;
  }

  private persist(envelope: Envelope): Envelope {
    const nextOffset = (this.topicOffsets.get(envelope.topic) ?? 0) + 1;
    this.topicOffsets.set(envelope.topic, nextOffset);
    const persisted: Envelope = { ...envelope, offset: nextOffset };

    const current = this.topicBuffers.get(envelope.topic) ?? [];
    const next = [...current, persisted];
    if (next.length > this.ringBufferSize) {
      next.splice(0, next.length - this.ringBufferSize);
    }
    this.topicBuffers.set(envelope.topic, next);
    return persisted;
  }

  private dispatchToResponders(requestEnvelope: Envelope): void {
    const matchingResponders = [...this.responders.values()].filter((responder) =>
      matchTopicPattern(responder.pattern, requestEnvelope.topic)
    );

    matchingResponders.forEach((responder) => {
      void Promise.resolve(responder.handler(requestEnvelope.payload, requestEnvelope))
        .then((replyPayload) => {
          if (!requestEnvelope.replyTo || !requestEnvelope.correlationId) {
            return;
          }
          return this.publishEnvelope(
            createEnvelope({
              topic: requestEnvelope.replyTo,
              payload: replyPayload,
              kind: "reply",
              correlationId: requestEnvelope.correlationId,
              causationId: requestEnvelope.id,
              producer: this.clientId
            })
          );
        })
        .catch((error: unknown) => {
          if (!requestEnvelope.replyTo || !requestEnvelope.correlationId) {
            return;
          }
          return this.publishEnvelope(
            createEnvelope({
              topic: requestEnvelope.replyTo,
              payload: { message: error instanceof Error ? error.message : String(error) },
              kind: "error",
              correlationId: requestEnvelope.correlationId,
              causationId: requestEnvelope.id,
              producer: this.clientId
            })
          );
        });
    });
  }

  private resolvePendingRequest(envelope: Envelope): void {
    const correlationId = envelope.correlationId;
    if (!correlationId) {
      return;
    }

    const pending = this.pendingRequests.get(correlationId);
    if (!pending) {
      return;
    }

    this.pendingRequests.delete(correlationId);
    if (envelope.kind === "error") {
      const message =
        typeof envelope.payload === "object" &&
        envelope.payload !== null &&
        "message" in envelope.payload &&
        typeof (envelope.payload as { message?: unknown }).message === "string"
          ? (envelope.payload as { message: string }).message
          : "Request failed.";
      pending.reject(new BrokerResponseError(message, correlationId));
      return;
    }

    pending.resolve(envelope.payload);
  }

  private async replayBuffered(topicOrPattern: string, options: ReplayOptions = {}): Promise<Envelope[]> {
    const allBuffered = [...this.topicBuffers.values()].flat();
    const matching = allBuffered
      .filter((event) => matchTopicPattern(topicOrPattern, event.topic))
      .sort((a, b) => (a.ts === b.ts ? (a.offset ?? 0) - (b.offset ?? 0) : a.ts - b.ts));

    if (options.fromOffset !== undefined) {
      return matching.filter((event) => (event.offset ?? 0) >= options.fromOffset!);
    }

    if (options.lastN !== undefined) {
      return matching.slice(-options.lastN);
    }

    return matching;
  }
}

