import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import { ClientFrameSchema, type Envelope } from "@kierkegaard/contracts";
import { Effect } from "effect";
import Fastify, { type FastifyInstance } from "fastify";
import type WebSocket from "ws";
import type { RawData } from "ws";
import { z } from "zod";
import { BrokerRouter } from "./broker/routing";
import { SqlitePersistence } from "./broker/persistence";

const ReplayQuerySchema = z
  .object({
    topic: z.string().min(1),
    fromOffset: z.coerce.number().int().nonnegative().optional(),
    lastN: z.coerce.number().int().positive().optional()
  })
  .refine((query) => !(query.fromOffset !== undefined && query.lastN !== undefined), {
    message: "Use either fromOffset or lastN, not both."
  });

export type BuildServerOptions = {
  dbPath: string;
  logger?: boolean;
};

type AckFrame = {
  type: "ACK";
  requestId: string;
  envelope: Envelope;
};

type ErrorFrame = {
  type: "ERROR";
  requestId?: string;
  message: string;
};

type LooseEnvelope = {
  id: string;
  topic: string;
  key?: string | undefined;
  ts: number;
  schemaVersion: number;
  producer?: string | undefined;
  correlationId?: string | undefined;
  causationId?: string | undefined;
  replyTo?: string | undefined;
  kind: Envelope["kind"];
  payload: unknown;
  offset?: number | undefined;
};

const send = (socket: WebSocket, frame: AckFrame | ErrorFrame): void => {
  if (socket.readyState !== 1) {
    return;
  }
  socket.send(JSON.stringify(frame));
};

const normalizeEnvelope = (envelope: LooseEnvelope): Envelope => {
  const normalized: Envelope = {
    id: envelope.id,
    topic: envelope.topic,
    ts: envelope.ts,
    schemaVersion: envelope.schemaVersion,
    kind: envelope.kind,
    payload: envelope.payload
  };

  if (envelope.key !== undefined) {
    normalized.key = envelope.key;
  }
  if (envelope.producer !== undefined) {
    normalized.producer = envelope.producer;
  }
  if (envelope.correlationId !== undefined) {
    normalized.correlationId = envelope.correlationId;
  }
  if (envelope.causationId !== undefined) {
    normalized.causationId = envelope.causationId;
  }
  if (envelope.replyTo !== undefined) {
    normalized.replyTo = envelope.replyTo;
  }
  if (envelope.offset !== undefined) {
    normalized.offset = envelope.offset;
  }

  return normalized;
};

export const buildServer = async (options: BuildServerOptions): Promise<FastifyInstance> => {
  const app = Fastify({ logger: options.logger ?? false });
  await app.register(cors, { origin: true });
  await app.register(websocket);

  const directPersistence = Effect.runSync(Effect.sync(() => new SqlitePersistence(options.dbPath)));
  const broker = Effect.runSync(Effect.sync(() => new BrokerRouter(directPersistence)));

  app.addHook("onClose", async () => {
    directPersistence.close();
  });

  app.get("/health", async () => ({ ok: true }));

  app.get("/stats", async () => broker.stats());

  app.get("/events", async (request, reply) => {
    const parsed = ReplayQuerySchema.safeParse(request.query);
    if (!parsed.success) {
      reply.status(400);
      return { message: parsed.error.flatten() };
    }

    const replayQuery: { fromOffset?: number; lastN?: number } = {};
    if (parsed.data.fromOffset !== undefined) {
      replayQuery.fromOffset = parsed.data.fromOffset;
    }
    if (parsed.data.lastN !== undefined) {
      replayQuery.lastN = parsed.data.lastN;
    }

    const events = directPersistence.replay(parsed.data.topic, replayQuery);
    return events;
  });

  app.get("/ws", { websocket: true }, (socket) => {
    const clientId = broker.register(socket);

    socket.on("message", (raw: RawData) => {
      let decoded: unknown;
      try {
        decoded = JSON.parse(raw.toString());
      } catch {
        send(socket, { type: "ERROR", message: "Invalid JSON frame." });
        return;
      }

      const parsed = ClientFrameSchema.safeParse(decoded);
      if (!parsed.success) {
        send(socket, { type: "ERROR", message: "Frame validation failed." });
        return;
      }

      const frame = parsed.data;
      try {
        if (frame.type === "SUBSCRIBE") {
          broker.subscribe(clientId, frame.pattern);
          return;
        }
        if (frame.type === "UNSUBSCRIBE") {
          broker.unsubscribe(clientId, frame.pattern);
          return;
        }

        const persisted = broker.publishFromClient(clientId, normalizeEnvelope(frame.envelope as LooseEnvelope));
        send(socket, {
          type: "ACK",
          requestId: frame.requestId,
          envelope: persisted
        });
      } catch (error: unknown) {
        const errorFrame: ErrorFrame = {
          type: "ERROR",
          message: error instanceof Error ? error.message : String(error)
        };
        if ("requestId" in frame) {
          errorFrame.requestId = frame.requestId;
        }
        send(socket, errorFrame);
      }
    });

    socket.on("close", () => {
      broker.unregister(clientId);
    });
  });

  return app;
};

