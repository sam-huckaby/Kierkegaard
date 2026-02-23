import { describe, expect, it, vi } from "vitest";
import { MemoryBrokerDriver } from "../src/drivers/memory/MemoryBrokerDriver";
import { BrokerResponseError, BrokerTimeoutError } from "../src/client/types";

describe("MemoryBrokerDriver", () => {
  it("publishes and fans out to matching subscribers", async () => {
    const driver = new MemoryBrokerDriver({ type: "memory" });
    const received: string[] = [];

    const unsubscribe = driver.subscribe("billing.*", (event) => {
      received.push(String((event.payload as { id: string }).id));
    });

    await driver.publish("billing.invoice_paid", { id: "inv-1" });
    await driver.publish("orders.invoice_paid", { id: "inv-2" });

    expect(received).toEqual(["inv-1"]);
    unsubscribe();
  });

  it("supports request/reply and returns responder payload", async () => {
    const driver = new MemoryBrokerDriver({ type: "memory" });

    driver.respond("math.add", (payload) => {
      const p = payload as { a: number; b: number };
      return { result: p.a + p.b };
    });

    const reply = await driver.request<{ result: number }>("math.add", { a: 2, b: 3 });
    expect(reply.result).toBe(5);
  });

  it("maps responder failures to error envelopes", async () => {
    const driver = new MemoryBrokerDriver({ type: "memory" });
    driver.respond("math.add", () => {
      throw new Error("bad input");
    });

    await expect(driver.request("math.add", { a: 1, b: 2 })).rejects.toBeInstanceOf(BrokerResponseError);
  });

  it("times out unresolved requests", async () => {
    vi.useFakeTimers();
    const driver = new MemoryBrokerDriver({ type: "memory", requestTimeoutMs: 50 });
    const result = driver.request("math.add", { a: 1, b: 1 });

    await vi.advanceTimersByTimeAsync(60);
    await expect(result).rejects.toBeInstanceOf(BrokerTimeoutError);
    vi.useRealTimers();
  });

  it("replays buffered events in order and evicts old entries", async () => {
    const driver = new MemoryBrokerDriver({ type: "memory", ringBufferSize: 2 });
    await driver.publish("billing.invoice_paid", { id: "inv-1" });
    await driver.publish("billing.invoice_paid", { id: "inv-2" });
    await driver.publish("billing.invoice_paid", { id: "inv-3" });

    const replayed: string[] = [];
    for await (const event of driver.replay("billing.invoice_paid", { lastN: 2 })) {
      replayed.push(String((event.payload as { id: string }).id));
    }

    expect(replayed).toEqual(["inv-2", "inv-3"]);
  });
});

