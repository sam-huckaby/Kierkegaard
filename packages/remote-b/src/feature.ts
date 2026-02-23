import { respond, subscribe } from "broker/client";

export const startInvoiceSubscriber = (onInvoice: (invoiceId: string) => void): (() => void) =>
  subscribe("billing.*", (envelope) => {
    const payload = envelope.payload as { id?: string };
    if (payload.id) {
      onInvoice(payload.id);
    }
  });

export const startMathResponder = (): (() => void) =>
  respond("math.add", (payload) => {
    const input = payload as { a: number; b: number };
    return { result: input.a + input.b };
  });

