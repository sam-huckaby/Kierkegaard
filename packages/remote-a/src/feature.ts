import { createFederatedBrokerSdk } from "@federated-kafka/sdk";

const broker = createFederatedBrokerSdk(() => import("broker/client"));
const billingTopic = broker.topic<{ id: string }>("billing.invoice_paid");
const mathAddTopic = broker.topic<never, { a: number; b: number }, { result: number }>("math.add");

export const publishInvoicePaid = async (invoiceId: string): Promise<void> => {
  await billingTopic.publish({ id: invoiceId });
};

export const requestMathAdd = async (a: number, b: number): Promise<number> => {
  const result = await mathAddTopic.request({ a, b }, { timeoutMs: 2000 });
  return result.result;
};

