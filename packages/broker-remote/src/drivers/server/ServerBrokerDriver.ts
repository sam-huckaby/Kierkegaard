import {
  createEnvelope,
  EnvelopeSchema,
  matchTopicPattern,
  ServerFrameSchema,
  validateTopicPattern,
  type Envelope,
  type Topic
} from "@federated-kafka/contracts";
import { v7 as uuidv7 } from "uuid";
import {
  BrokerResponseError,
  BrokerTimeoutError,
  BrokerUnavailableError,
  type BrokerDriver,
  type EventHandler,
  type PublishOptions,
  type ReplayOptions,
  type RespondOptions,
  type RequestHandler,
  type RequestOptions,
  type ServerDriverConfig,
  type SubscribeOptions,
  type Unsubscribe,
  type WebSocketFactory,
  type WebSocketLike
} from "../../client/types";

const WS_OPEN = 1;

type Subscriber = {
  id: string;
  pattern: string;
  handler: EventHandler;
};

type PendingAck = {
  resolve: (envelope: Envelope) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
};

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
  signalCleanup?: () => void;
};

export class ServerBrokerDriver implements BrokerDriver {
  private readonly wsUrl: string;

  private readonly httpUrl: string;

  private readonly clientId: string;

  private readonly requestTimeoutMs: number;

  private readonly ackTimeoutMs: number;

  private readonly reconnectMs: number;

  private readonly webSocketCtor: WebSocketFactory;

  private readonly fetchFn: typeof fetch;

  private ws: WebSocketLike | undefined;

  private connectPromise: Promise<void> | undefined;

  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  private readonly subscribers = new Map<string, Subscriber>();

  private readonly patternRefCount = new Map<string, number>();

  private readonly pendingAcks = new Map<string, PendingAck>();

  private readonly pendingRequests = new Map<string, PendingRequest>();

  private closed = false;

  constructor(config: ServerDriverConfig) {
    this.wsUrl = config.wsUrl;
    this.httpUrl = config.httpUrl;
    this.clientId = config.clientId ?? "server-client";
    this.requestTimeoutMs = config.requestTimeoutMs ?? 5000;
    this.ackTimeoutMs = config.ackTimeoutMs ?? 3000;
    this.reconnectMs = config.reconnectMs ?? 1000;
    this.webSocketCtor = config.webSocketCtor ?? this.resolveGlobalWebSocket();
    this.fetchFn = config.fetchFn ?? fetch;
  }

  async publish(topic: Topic, payload: unknown, options: PublishOptions = {}): Promise<Envelope> {
    const envelope = createEnvelope({
      topic,
      payload,
      key: options.key,
      producer: options.producer ?? this.clientId,
      correlationId: options.correlationId,
      causationId: options.causationId,
      replyTo: options.replyTo,
      kind: options.kind ?? "event",
      schemaVersion: options.schemaVersion
    });
    return this.sendPublish(envelope);
  }

  subscribe(topicPattern: string, handler: EventHandler, options: SubscribeOptions = {}): Unsubscribe {
    validateTopicPattern(topicPattern);
    const id = uuidv7();
    this.subscribers.set(id, { id, pattern: topicPattern, handler });

    const nextCount = (this.patternRefCount.get(topicPattern) ?? 0) + 1;
    this.patternRefCount.set(topicPattern, nextCount);
    if (nextCount === 1) {
      void this.sendFrame({ type: "SUBSCRIBE", pattern: topicPattern });
    }

    if (
      (options.replayFromOffset !== undefined || options.replayLastN !== undefined) &&
      !topicPattern.includes("*")
    ) {
      void (async () => {
        for await (const envelope of this.replay(topicPattern, {
          fromOffset: options.replayFromOffset,
          lastN: options.replayLastN
        })) {
          if (matchTopicPattern(topicPattern, envelope.topic)) {
            void Promise.resolve(handler(envelope));
          }
        }
      })();
    }

    return () => {
      this.subscribers.delete(id);
      const currentCount = this.patternRefCount.get(topicPattern) ?? 0;
      const updatedCount = Math.max(0, currentCount - 1);
      if (updatedCount === 0) {
        this.patternRefCount.delete(topicPattern);
        void this.sendFrame({ type: "UNSUBSCRIBE", pattern: topicPattern });
      } else {
        this.patternRefCount.set(topicPattern, updatedCount);
      }
    };
  }

  request<TReply = unknown>(topic: Topic, payload: unknown, options: RequestOptions = {}): Promise<TReply> {
    const correlationId = uuidv7();
    const timeoutMs = options.timeoutMs ?? this.requestTimeoutMs;
    const envelope = createEnvelope({
      topic,
      payload,
      key: options.key,
      producer: options.producer ?? this.clientId,
      kind: "request",
      replyTo: `_reply.${this.clientId}`,
      correlationId
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
      void this.sendPublish(envelope).catch((error: unknown) => {
        this.pendingRequests.delete(correlationId);
        pending.reject(error instanceof Error ? error : new Error(String(error)));
      });
    });
  }

  respond(topicPattern: string, handler: RequestHandler, _options: RespondOptions = {}): Unsubscribe {
    validateTopicPattern(topicPattern);
    return this.subscribe(topicPattern, (envelope) => {
      if (envelope.kind !== "request") {
        return;
      }

      void Promise.resolve()
        .then(() => handler(envelope.payload, envelope))
        .then((replyPayload) =>
          this.publish(envelope.replyTo ?? "_reply.unrouted", replyPayload, {
            kind: "reply",
            producer: this.clientId,
            correlationId: envelope.correlationId,
            causationId: envelope.id
          })
        )
        .catch((error: unknown) =>
          this.publish(envelope.replyTo ?? "_reply.unrouted", { message: String(error) }, {
            kind: "error",
            producer: this.clientId,
            correlationId: envelope.correlationId,
            causationId: envelope.id
          })
        );
    });
  }

  async *replay(topic: Topic, options: ReplayOptions = {}): AsyncIterable<Envelope> {
    const url = new URL("/events", this.httpUrl);
    url.searchParams.set("topic", topic);
    if (options.fromOffset !== undefined) {
      url.searchParams.set("fromOffset", String(options.fromOffset));
    }
    if (options.lastN !== undefined) {
      url.searchParams.set("lastN", String(options.lastN));
    }

    const response = await this.fetchFn(url.toString());
    if (!response.ok) {
      throw new BrokerUnavailableError(`Replay request failed with status ${response.status}.`);
    }

    const body = await response.json();
    if (!Array.isArray(body)) {
      throw new BrokerUnavailableError("Replay response must be an array.");
    }

    for (const rawEnvelope of body) {
      yield EnvelopeSchema.parse(rawEnvelope);
    }
  }

  stats(): Record<string, unknown> {
    return {
      mode: "server",
      subscribers: this.subscribers.size,
      pendingAcks: this.pendingAcks.size,
      pendingRequests: this.pendingRequests.size,
      connected: this.ws?.readyState === WS_OPEN
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }

    this.pendingAcks.forEach((pending) => {
      clearTimeout(pending.timeout);
      pending.reject(new BrokerUnavailableError("Broker connection closed."));
    });
    this.pendingAcks.clear();

    this.pendingRequests.forEach((pending) => {
      clearTimeout(pending.timeout);
      pending.signalCleanup?.();
      pending.reject(new BrokerUnavailableError("Broker connection closed."));
    });
    this.pendingRequests.clear();

    this.ws?.close();
    this.ws = undefined;
    this.connectPromise = undefined;
  }

  private resolveGlobalWebSocket(): WebSocketFactory {
    const ctor = (globalThis as { WebSocket?: WebSocketFactory }).WebSocket;
    if (!ctor) {
      throw new BrokerUnavailableError("No WebSocket implementation found. Provide webSocketCtor in config.");
    }
    return ctor;
  }

  private async sendPublish(envelope: Envelope): Promise<Envelope> {
    const requestId = uuidv7();
    await this.ensureConnected();

    return new Promise<Envelope>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingAcks.delete(requestId);
        reject(new BrokerTimeoutError("Broker ACK timed out."));
      }, this.ackTimeoutMs);

      this.pendingAcks.set(requestId, {
        resolve: (persisted) => {
          clearTimeout(timeout);
          resolve(persisted);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
        timeout
      });

      try {
        this.ws?.send(JSON.stringify({ type: "PUBLISH", requestId, envelope }));
      } catch (error: unknown) {
        this.pendingAcks.delete(requestId);
        clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private async sendFrame(frame: { type: "SUBSCRIBE" | "UNSUBSCRIBE"; pattern: string }): Promise<void> {
    await this.ensureConnected();
    this.ws?.send(JSON.stringify(frame));
  }

  private async ensureConnected(): Promise<void> {
    if (this.closed) {
      throw new BrokerUnavailableError("Broker driver is closed.");
    }

    if (this.ws?.readyState === WS_OPEN) {
      return;
    }

    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = new Promise<void>((resolve, reject) => {
      try {
        const ws = new this.webSocketCtor(this.wsUrl);
        this.ws = ws;

        ws.onopen = () => {
          this.connectPromise = undefined;
          this.replaySubscriptions();
          resolve();
        };

        ws.onmessage = (event) => {
          this.handleIncomingFrame(event.data);
        };

        ws.onerror = () => {
          reject(new BrokerUnavailableError("WebSocket connection failed."));
          this.connectPromise = undefined;
        };

        ws.onclose = () => {
          this.connectPromise = undefined;
          this.ws = undefined;
          if (!this.closed) {
            this.scheduleReconnect();
          }
        };
      } catch (error: unknown) {
        this.connectPromise = undefined;
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });

    return this.connectPromise;
  }

  private replaySubscriptions(): void {
    this.patternRefCount.forEach((_count, pattern) => {
      this.ws?.send(JSON.stringify({ type: "SUBSCRIBE", pattern }));
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.ensureConnected().catch(() => {
        this.scheduleReconnect();
      });
    }, this.reconnectMs);
  }

  private handleIncomingFrame(rawData: string | Buffer | ArrayBuffer): void {
    const asString =
      typeof rawData === "string"
        ? rawData
        : rawData instanceof ArrayBuffer
          ? new TextDecoder().decode(rawData)
          : rawData.toString("utf-8");

    let parsed: unknown;
    try {
      parsed = JSON.parse(asString);
    } catch {
      return;
    }

    const frameResult = ServerFrameSchema.safeParse(parsed);
    if (!frameResult.success) {
      return;
    }

    const frame = frameResult.data;
    if (frame.type === "ACK") {
      const pending = this.pendingAcks.get(frame.requestId);
      if (!pending) {
        return;
      }
      this.pendingAcks.delete(frame.requestId);
      pending.resolve(frame.envelope ?? createEnvelope({ topic: "_ack", payload: {}, kind: "event" }));
      return;
    }

    if (frame.type === "ERROR") {
      if (frame.requestId) {
        const pending = this.pendingAcks.get(frame.requestId);
        if (pending) {
          this.pendingAcks.delete(frame.requestId);
          pending.reject(new BrokerUnavailableError(frame.message));
        }
      }
      return;
    }

    this.resolvePendingRequest(frame.envelope);
    this.subscribers.forEach((subscriber) => {
      if (!matchTopicPattern(subscriber.pattern, frame.envelope.topic)) {
        return;
      }
      void Promise.resolve(subscriber.handler(frame.envelope));
    });
  }

  private resolvePendingRequest(envelope: Envelope): void {
    if (envelope.kind !== "reply" && envelope.kind !== "error") {
      return;
    }

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
}

