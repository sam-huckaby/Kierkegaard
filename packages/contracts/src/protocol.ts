import { z } from "zod";
import { EnvelopeSchema } from "./envelope";

export const SubscribeFrameSchema = z.object({
  type: z.literal("SUBSCRIBE"),
  pattern: z.string().min(1)
});

export const UnsubscribeFrameSchema = z.object({
  type: z.literal("UNSUBSCRIBE"),
  pattern: z.string().min(1)
});

export const PublishFrameSchema = z.object({
  type: z.literal("PUBLISH"),
  requestId: z.string().min(1),
  envelope: EnvelopeSchema
});

export const EventFrameSchema = z.object({
  type: z.literal("EVENT"),
  envelope: EnvelopeSchema
});

export const AckFrameSchema = z.object({
  type: z.literal("ACK"),
  requestId: z.string().min(1),
  envelope: EnvelopeSchema.optional()
});

export const ErrorFrameSchema = z.object({
  type: z.literal("ERROR"),
  requestId: z.string().optional(),
  message: z.string()
});

export const RequestEnvelopeFrameSchema = z.object({
  type: z.literal("REQUEST"),
  requestId: z.string().min(1),
  envelope: EnvelopeSchema
});

export const ReplyEnvelopeFrameSchema = z.object({
  type: z.literal("REPLY"),
  requestId: z.string().min(1),
  envelope: EnvelopeSchema
});

export const ClientFrameSchema = z.union([
  SubscribeFrameSchema,
  UnsubscribeFrameSchema,
  PublishFrameSchema,
  RequestEnvelopeFrameSchema,
  ReplyEnvelopeFrameSchema
]);

export const ServerFrameSchema = z.union([
  EventFrameSchema,
  AckFrameSchema,
  ErrorFrameSchema
]);

export type SubscribeFrame = z.infer<typeof SubscribeFrameSchema>;
export type UnsubscribeFrame = z.infer<typeof UnsubscribeFrameSchema>;
export type PublishFrame = z.infer<typeof PublishFrameSchema>;
export type EventFrame = z.infer<typeof EventFrameSchema>;
export type AckFrame = z.infer<typeof AckFrameSchema>;
export type ErrorFrame = z.infer<typeof ErrorFrameSchema>;
export type RequestEnvelopeFrame = z.infer<typeof RequestEnvelopeFrameSchema>;
export type ReplyEnvelopeFrame = z.infer<typeof ReplyEnvelopeFrameSchema>;

export type ClientFrame = z.infer<typeof ClientFrameSchema>;
export type ServerFrame = z.infer<typeof ServerFrameSchema>;

