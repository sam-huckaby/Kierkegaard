import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import { ClientFrameSchema, type Envelope } from "@federated-kafka/contracts";
import { Effect } from "effect";
import Fastify, { type FastifyInstance } from "fastify";
import type WebSocket from "ws";
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

const send = (socket: WebSocket, frame: AckFrame | ErrorFrame): void => {
  if (socket.readyState !== 1) {
    return;
  }
  socket.send(JSON.stringify(frame));
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

    const events = directPersistence.replay(parsed.data.topic, {
      fromOffset: parsed.data.fromOffset,
      lastN: parsed.data.lastN
    });
    return events;
  });

  app.get("/ws", { websocket: true }, (connection) => {
    const clientId = broker.register(connection.socket);

    connection.socket.on("message", (raw) => {
      let decoded: unknown;
      try {
        decoded = JSON.parse(raw.toString());
      } catch {
        send(connection.socket, { type: "ERROR", message: "Invalid JSON frame." });
        return;
      }

      const parsed = ClientFrameSchema.safeParse(decoded);
      if (!parsed.success) {
        send(connection.socket, { type: "ERROR", message: "Frame validation failed." });
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

        const persisted = broker.publishFromClient(clientId, frame.envelope);
        send(connection.socket, {
          type: "ACK",
          requestId: frame.requestId,
          envelope: persisted
        });
      } catch (error: unknown) {
        send(connection.socket, {
          type: "ERROR",
          requestId: "requestId" in frame ? frame.requestId : undefined,
          message: error instanceof Error ? error.message : String(error)
        });
      }
    });

    connection.socket.on("close", () => {
      broker.unregister(clientId);
    });
  });

  return app;
};

