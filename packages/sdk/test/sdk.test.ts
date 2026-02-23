import { createEnvelope } from "@federated-kafka/contracts";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  configureFederatedBroker,
  createFederatedBrokerSdk,
  FederatedBrokerNotConfiguredError,
  publish,
  request,
  topic,
  unsafeResetFederatedBrokerForTests
} from "../src";
import type { BrokerClientModule } from "../src/types";

const makeMockBrokerModule = (): BrokerClientModule => {
  const replayIterable = {
    async *[Symbol.asyncIterator]() {
      yield createEnvelope({ topic: "billing.invoice_paid", payload: { id: "inv-1" } });
    }
  };

  return {
    publish: vi.fn(async (topicName: string, payload: unknown) =>
      createEnvelope({ topic: topicName, payload, kind: "event" })
    ),
    subscribe: vi.fn(() => () => undefined),
    request: vi.fn(async () => ({ result: 21 })),
    respond: vi.fn(() => () => undefined),
    replay: vi.fn(() => replayIterable),
    stats: vi.fn(() => ({ connected: true })),
    setDriver: vi.fn(async () => undefined)
  };
};

describe("@federated-kafka/sdk", () => {
  beforeEach(() => {
    unsafeResetFederatedBrokerForTests();
    vi.restoreAllMocks();
  });

  it("throws when global sdk is used without configuration", async () => {
    await expect(publish("billing.invoice_paid", { id: "inv-1" })).rejects.toBeInstanceOf(
      FederatedBrokerNotConfiguredError
    );
  });

  it("configures global loader and delegates publish/request calls", async () => {
    const brokerModule = makeMockBrokerModule();
    const loader = vi.fn(async () => brokerModule);
    configureFederatedBroker(loader);

    const published = await publish("billing.invoice_paid", { id: "inv-1" });
    const reply = await request<{ result: number }>("math.add", { a: 8, b: 13 });

    expect(loader).toHaveBeenCalledTimes(1);
    expect(brokerModule.publish).toHaveBeenCalledWith("billing.invoice_paid", { id: "inv-1" }, undefined);
    expect(published.topic).toBe("billing.invoice_paid");
    expect(reply.result).toBe(21);
  });

  it("creates isolated sdk instances and caches per-loader module", async () => {
    const brokerModule = makeMockBrokerModule();
    const loader = vi.fn(async () => brokerModule);
    const sdk = createFederatedBrokerSdk(loader);

    await sdk.publish("billing.invoice_paid", { id: "inv-1" });
    await sdk.request("math.add", { a: 1, b: 2 });
    await sdk.setMemoryDriver({ ringBufferSize: 200 });

    expect(loader).toHaveBeenCalledTimes(1);
    expect(brokerModule.setDriver).toHaveBeenCalledWith({ type: "memory", ringBufferSize: 200 });
  });

  it("creates typed topic helpers for publish/request/subscribe/respond/replay", async () => {
    const brokerModule = makeMockBrokerModule();
    const loader = vi.fn(async () => brokerModule);
    const sdk = createFederatedBrokerSdk(loader);

    const billing = sdk.topic<{ id: string }, { a: number; b: number }, { result: number }>("math.add");
    await billing.publish({ id: "inv-9" });
    const reply = await billing.request({ a: 2, b: 3 });
    const unsubscribeSub = billing.subscribe(() => undefined);
    const unsubscribeRes = billing.respond(() => ({ result: 12 }));

    const replayed: string[] = [];
    for await (const event of billing.replay()) {
      replayed.push((event.payload as { id: string }).id);
    }

    expect(reply.result).toBe(21);
    expect(replayed).toEqual(["inv-1"]);
    expect(typeof unsubscribeSub).toBe("function");
    expect(typeof unsubscribeRes).toBe("function");
  });

  it("supports global topic helper after configuration", async () => {
    const brokerModule = makeMockBrokerModule();
    configureFederatedBroker(async () => brokerModule);

    const mathTopic = topic<never, { a: number; b: number }, { result: number }>("math.add");
    const response = await mathTopic.request({ a: 5, b: 7 });
    expect(response.result).toBe(21);
  });
});

