import { useEffect, useMemo, useState } from "react";
import type { Envelope } from "@federated-kafka/contracts";
import { stats, subscribe } from "../client";

const MAX_EVENTS = 200;

export const DevtoolsApp = () => {
  const [events, setEvents] = useState<Envelope[]>([]);
  const [snapshot, setSnapshot] = useState<Record<string, unknown>>({});

  useEffect(() => {
    const unsubscribe = subscribe("*", (event) => {
      setEvents((current) => {
        const next = [...current, event];
        if (next.length > MAX_EVENTS) {
          next.splice(0, next.length - MAX_EVENTS);
        }
        return next;
      });
      setSnapshot(stats());
    });

    setSnapshot(stats());
    return () => unsubscribe();
  }, []);

  const rows = useMemo(
    () =>
      events.map((event) => ({
        ...event,
        isoTs: new Date(event.ts).toISOString()
      })),
    [events]
  );

  return (
    <div style={{ fontFamily: "sans-serif", padding: 16 }}>
      <h2>Federated Kafka Devtools</h2>
      <pre data-testid="broker-stats" style={{ background: "#f5f5f5", padding: 8 }}>
        {JSON.stringify(snapshot, null, 2)}
      </pre>
      <div style={{ maxHeight: 380, overflow: "auto", border: "1px solid #ddd" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr>
              <th align="left">Offset</th>
              <th align="left">Topic</th>
              <th align="left">Kind</th>
              <th align="left">Correlation</th>
              <th align="left">Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((event) => (
              <tr key={event.id}>
                <td>{event.offset ?? "-"}</td>
                <td>{event.topic}</td>
                <td>{event.kind}</td>
                <td>{event.correlationId ?? "-"}</td>
                <td>{event.isoTs}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default DevtoolsApp;

