import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createEnvelope } from "@kierkegaard/contracts";
import { afterEach, describe, expect, it } from "vitest";
import { SqlitePersistence } from "../src/broker/persistence";

const tempPaths: string[] = [];

const makeDbPath = (): string => {
  const filePath = path.join(os.tmpdir(), `kierkegaard-${Date.now()}-${Math.random().toString(16).slice(2)}.sqlite`);
  tempPaths.push(filePath);
  return filePath;
};

afterEach(() => {
  tempPaths.splice(0).forEach((filePath) => {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  });
});

describe("SqlitePersistence", () => {
  it("assigns monotonic offsets per topic", () => {
    const persistence = new SqlitePersistence(makeDbPath());

    const first = persistence.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "1" } }));
    const second = persistence.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "2" } }));
    const third = persistence.insertEvent(createEnvelope({ topic: "orders.created", payload: { id: "3" } }));

    expect(first.offset).toBe(1);
    expect(second.offset).toBe(2);
    expect(third.offset).toBe(1);
    persistence.close();
  });

  it("replays by fromOffset and lastN", () => {
    const persistence = new SqlitePersistence(makeDbPath());
    persistence.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "1" } }));
    persistence.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "2" } }));
    persistence.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "3" } }));

    const fromOffset = persistence.replay("billing.invoice_paid", { fromOffset: 2 });
    const lastTwo = persistence.replay("billing.invoice_paid", { lastN: 2 });

    expect(fromOffset.map((e) => (e.payload as { id: string }).id)).toEqual(["2", "3"]);
    expect(lastTwo.map((e) => (e.payload as { id: string }).id)).toEqual(["2", "3"]);
    persistence.close();
  });

  it("keeps durable offsets across restart", () => {
    const dbPath = makeDbPath();
    const firstSession = new SqlitePersistence(dbPath);
    firstSession.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "1" } }));
    firstSession.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "2" } }));
    firstSession.close();

    const secondSession = new SqlitePersistence(dbPath);
    const persisted = secondSession.insertEvent(createEnvelope({ topic: "billing.invoice_paid", payload: { id: "3" } }));
    expect(persisted.offset).toBe(3);
    secondSession.close();
  });
});

