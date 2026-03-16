import { v7 as uuidv7 } from "uuid";
import { z } from "zod";

export type Topic = string;

export type EnvelopeKind = "event" | "request" | "reply" | "error";

export type Envelope<T = unknown> = {
  id: string;
  topic: Topic;
  key?: string;
  ts: number;
  schemaVersion: number;
  producer?: string;
  correlationId?: string;
  causationId?: string;
  replyTo?: Topic;
  kind: EnvelopeKind;
  payload: T;
  offset?: number;
};

export const EnvelopeSchema = z.object({
  id: z.string().min(1),
  topic: z.string().min(1),
  key: z.string().optional(),
  ts: z.number().int().nonnegative(),
  schemaVersion: z.number().int().positive(),
  producer: z.string().optional(),
  correlationId: z.string().optional(),
  causationId: z.string().optional(),
  replyTo: z.string().optional(),
  kind: z.enum(["event", "request", "reply", "error"]),
  payload: z.unknown(),
  offset: z.number().int().nonnegative().optional()
});

export type CreateEnvelopeInput<T> = {
  topic: Topic;
  payload: T;
  key?: string;
  producer?: string;
  correlationId?: string;
  causationId?: string;
  replyTo?: Topic;
  kind?: EnvelopeKind;
  schemaVersion?: number;
};

export type CreateEnvelopeOptions = {
  now?: () => number;
  id?: () => string;
};

export const createEnvelope = <T>(
  input: CreateEnvelopeInput<T>,
  options: CreateEnvelopeOptions = {}
): Envelope<T> => {
  const now = options.now ?? Date.now;
  const id = options.id ?? uuidv7;

  const envelope: Envelope<T> = {
    id: id(),
    topic: input.topic,
    ts: now(),
    schemaVersion: input.schemaVersion ?? 1,
    kind: input.kind ?? "event",
    payload: input.payload
  };

  if (input.key !== undefined) {
    envelope.key = input.key;
  }
  if (input.producer !== undefined) {
    envelope.producer = input.producer;
  }
  if (input.correlationId !== undefined) {
    envelope.correlationId = input.correlationId;
  }
  if (input.causationId !== undefined) {
    envelope.causationId = input.causationId;
  }
  if (input.replyTo !== undefined) {
    envelope.replyTo = input.replyTo;
  }

  return envelope;
};

