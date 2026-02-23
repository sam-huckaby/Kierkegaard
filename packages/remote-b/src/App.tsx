import { useEffect, useState } from "react";
import { startInvoiceSubscriber, startMathResponder } from "./feature";

export const App = (): JSX.Element => {
  const [invoices, setInvoices] = useState<string[]>([]);
  const [responderReady, setResponderReady] = useState(false);

  useEffect(() => {
    const unsubscribe = startInvoiceSubscriber((invoiceId) => {
      setInvoices((current) => [invoiceId, ...current].slice(0, 20));
    });

    const stopResponder = startMathResponder();
    setResponderReady(true);

    return () => {
      unsubscribe();
      stopResponder();
      setResponderReady(false);
    };
  }, []);

  return (
    <section style={{ border: "1px solid #999", borderRadius: 6, padding: 12 }}>
      <h3>Remote B (Consumer / Responder)</h3>
      <p data-testid="responder-state">Responder: {responderReady ? "ready" : "stopped"}</p>
      <p data-testid="invoice-count">Received invoices: {invoices.length}</p>
      <ul data-testid="invoice-list">
        {invoices.map((invoiceId) => (
          <li key={invoiceId}>{invoiceId}</li>
        ))}
      </ul>
    </section>
  );
};

export default App;

