import { publish, request } from "broker/client";

export const publishInvoicePaid = async (invoiceId: string): Promise<void> => {
  await publish("billing.invoice_paid", { id: invoiceId });
};

export const requestMathAdd = async (a: number, b: number): Promise<number> => {
  const result = await request<{ result: number }>("math.add", { a, b }, { timeoutMs: 2000 });
  return result.result;
};

