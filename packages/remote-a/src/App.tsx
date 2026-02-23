import { useState } from "react";
import { publishInvoicePaid, requestMathAdd } from "./feature";

export const App = () => {
  const [lastInvoice, setLastInvoice] = useState<string>("-");
  const [requestState, setRequestState] = useState<string>("idle");

  const onPublish = async (): Promise<void> => {
    const invoiceId = `inv-${Date.now()}`;
    await publishInvoicePaid(invoiceId);
    setLastInvoice(invoiceId);
  };

  const onRequest = async (): Promise<void> => {
    setRequestState("pending");
    try {
      const result = await requestMathAdd(13, 8);
      setRequestState(`13 + 8 = ${result}`);
    } catch (error: unknown) {
      setRequestState(`error: ${error instanceof Error ? error.message : String(error)}`);
    }
  };

  return (
    <section style={{ border: "1px solid #999", borderRadius: 6, padding: 12 }}>
      <h3>Remote A (Producer / Requester)</h3>
      <button type="button" onClick={onPublish} data-testid="publish-btn">
        Publish billing.invoice_paid
      </button>
      <button type="button" onClick={onRequest} style={{ marginLeft: 8 }} data-testid="request-btn">
        Request math.add
      </button>
      <p data-testid="last-invoice">Last invoice: {lastInvoice}</p>
      <p data-testid="request-state">Request: {requestState}</p>
    </section>
  );
};

export default App;

