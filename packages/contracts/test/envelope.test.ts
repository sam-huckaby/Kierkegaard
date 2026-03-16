import { describe, expect, it } from "vitest";
import { createEnvelope, EnvelopeSchema } from "../src/envelope";

describe("createEnvelope", () => {
  it("builds deterministic fields when injected", () => {
    const envelope = createEnvelope(
      {
        topic: "math.add",
        kind: "request",
        correlationId: "corr-1",
        replyTo: "reply.topic",
        payload: { a: 1, b: 2 }
      },
      {
        id: () => "evt-1",
        now: () => 1700000000000
      }
    );

    expect(envelope).toEqual({
      id: "evt-1",
      topic: "math.add",
      ts: 1700000000000,
      schemaVersion: 1,
      correlationId: "corr-1",
      replyTo: "reply.topic",
      kind: "request",
      payload: { a: 1, b: 2 }
    });
  });

  it("validates with runtime schema", () => {
    const envelope = createEnvelope({
      topic: "billing.invoice_paid",
      payload: { id: "inv-1" },
      kind: "event"
    });

    expect(() => EnvelopeSchema.parse(envelope)).not.toThrow();
  });
});

