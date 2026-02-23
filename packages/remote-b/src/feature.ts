import { createFederatedBrokerSdk } from "@federated-kafka/sdk";

const broker = createFederatedBrokerSdk(() => import("broker/client"));
const billingTopic = broker.topic<{ id?: string }>("billing.*");
const mathAddTopic = broker.topic<never, { a: number; b: number }, { result: number }>("math.add");

export const ensureBrokerReady = (): Promise<void> => broker.ready();

export const startInvoiceSubscriber = (onInvoice: (invoiceId: string) => void): (() => void) =>
  billingTopic.subscribe((payload) => {
    if (payload.id) {
      onInvoice(payload.id);
    }
  });

export const startMathResponder = (): (() => void) =>
  mathAddTopic.respond((input) => {
    return { result: input.a + input.b };
  });

