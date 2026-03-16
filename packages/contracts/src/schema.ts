import { z } from "zod";
import { EnvelopeSchema } from "./envelope";

export const makePayloadSchema = <T extends z.ZodTypeAny>(schema: T) => schema;

export const makeEnvelopeSchema = <T extends z.ZodTypeAny>(payloadSchema: T) =>
  EnvelopeSchema.extend({
    payload: payloadSchema
  });

export const parseEnvelopeWithPayload = <T extends z.ZodTypeAny>(
  payloadSchema: T,
  value: unknown
): z.infer<ReturnType<typeof makeEnvelopeSchema<T>>> => makeEnvelopeSchema(payloadSchema).parse(value);

