import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import { afterEach, describe, expect, it } from "vitest";
import type { Envelope } from "@federated-kafka/contracts";
import { buildServer } from "@federated-kafka/server/app";
import { ServerBrokerDriver } from "../src/drivers/server/ServerBrokerDriver";
import { BrokerTimeoutError, type ServerDriverConfig, type WebSocketFactory } from "../src/client/types";

const cleanups: Array<() => Promise<void> | void> = [];
const tmpFiles: string[] = [];

const waitFor = async (
  predicate: () => boolean,
  timeoutMs = 3000,
  intervalMs = 20
): Promise<void> => {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("Timed out waiting for condition.");
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
};

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
  for (const tmp of tmpFiles.splice(0)) {
    if (fs.existsSync(tmp)) {
      fs.unlinkSync(tmp);
    }
  }
});

describe("ServerBrokerDriver", () => {
  it("supports publish/subscribe, request/reply, replay and timeout", async () => {
    const dbPath = path.join(os.tmpdir(), `broker-driver-${Date.now()}-${Math.random().toString(16).slice(2)}.sqlite`);
    tmpFiles.push(dbPath);

    const app = await buildServer({ dbPath });
    await app.listen({ host: "127.0.0.1", port: 0 });
    cleanups.push(() => app.close());

    const port = (app.server.address() as AddressInfo).port;
    const baseConfig: Omit<ServerDriverConfig, "type"> = {
      wsUrl: `ws://127.0.0.1:${port}/ws`,
      httpUrl: `http://127.0.0.1:${port}`,
      webSocketCtor: WebSocket as unknown as WebSocketFactory
    };

    const publisher = new ServerBrokerDriver({ type: "server", ...baseConfig, clientId: "publisher" });
    const subscriber = new ServerBrokerDriver({ type: "server", ...baseConfig, clientId: "subscriber" });
    const requester = new ServerBrokerDriver({
      type: "server",
      ...baseConfig,
      clientId: "requester",
      requestTimeoutMs: 80
    });
    const responder = new ServerBrokerDriver({ type: "server", ...baseConfig, clientId: "responder" });

    cleanups.push(() => publisher.close());
    cleanups.push(() => subscriber.close());
    cleanups.push(() => requester.close());
    cleanups.push(() => responder.close());

    const seenEvents: Envelope[] = [];
    const unsubscribe = subscriber.subscribe("billing.*", (event) => {
      seenEvents.push(event);
    });

    const persistedEvent = await publisher.publish("billing.invoice_paid", { id: "inv-1" });
    expect(persistedEvent.offset).toBe(1);

    await waitFor(() => seenEvents.length === 1);
    const firstSeen = seenEvents.at(0);
    expect(firstSeen).toBeDefined();
    expect((firstSeen!.payload as { id: string }).id).toBe("inv-1");

    responder.respond("math.add", (payload) => {
      const p = payload as { a: number; b: number };
      return { result: p.a + p.b };
    });

    const reply = await requester.request<{ result: number }>("math.add", { a: 8, b: 13 });
    expect(reply.result).toBe(21);

    await expect(requester.request("math.subtract", { a: 8, b: 13 }, { timeoutMs: 40 })).rejects.toBeInstanceOf(
      BrokerTimeoutError
    );

    const replayed: string[] = [];
    for await (const envelope of requester.replay("billing.invoice_paid", { fromOffset: 1 })) {
      replayed.push((envelope.payload as { id: string }).id);
    }
    expect(replayed).toEqual(["inv-1"]);

    unsubscribe();
  });
});

