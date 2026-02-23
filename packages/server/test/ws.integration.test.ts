import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import { afterEach, describe, expect, it } from "vitest";
import { createEnvelope, type Envelope } from "@kierkegaard/contracts";
import { buildServer } from "../src/app";

type Frame =
  | { type: "EVENT"; envelope: Envelope }
  | { type: "ACK"; requestId: string; envelope: Envelope }
  | { type: "ERROR"; requestId?: string; message: string };

const resources: Array<{ dbPath: string; close: () => Promise<void> }> = [];

const makeDbPath = (): string => {
  const filePath = path.join(os.tmpdir(), `kierkegaard-ws-${Date.now()}-${Math.random().toString(16).slice(2)}.sqlite`);
  return filePath;
};

const waitForOpen = (socket: WebSocket): Promise<void> =>
  new Promise((resolve, reject) => {
    socket.once("open", () => resolve());
    socket.once("error", (error) => reject(error));
  });

const waitForFrame = (socket: WebSocket, predicate: (frame: Frame) => boolean, timeoutMs = 3000): Promise<Frame> =>
  new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.off("message", onMessage);
      reject(new Error("Timed out waiting for frame."));
    }, timeoutMs);

    const onMessage = (raw: WebSocket.RawData) => {
      const frame = JSON.parse(raw.toString()) as Frame;
      if (!predicate(frame)) {
        return;
      }
      clearTimeout(timeout);
      socket.off("message", onMessage);
      resolve(frame);
    };

    socket.on("message", onMessage);
  });

afterEach(async () => {
  for (const resource of resources.splice(0)) {
    await resource.close();
    if (fs.existsSync(resource.dbPath)) {
      fs.unlinkSync(resource.dbPath);
    }
  }
});

describe("WebSocket broker integration", () => {
  it("stores published events and broadcasts to subscribers", async () => {
    const dbPath = makeDbPath();
    const app = await buildServer({ dbPath });
    await app.listen({ host: "127.0.0.1", port: 0 });
    resources.push({ dbPath, close: async () => app.close() });

    const port = (app.server.address() as AddressInfo).port;
    const wsUrl = `ws://127.0.0.1:${port}/ws`;

    const publisher = new WebSocket(wsUrl);
    const subscriber = new WebSocket(wsUrl);
    await Promise.all([waitForOpen(publisher), waitForOpen(subscriber)]);

    subscriber.send(JSON.stringify({ type: "SUBSCRIBE", pattern: "billing.*" }));

    const eventPromise = waitForFrame(subscriber, (frame) => frame.type === "EVENT");
    publisher.send(
      JSON.stringify({
        type: "PUBLISH",
        requestId: "pub-1",
        envelope: createEnvelope({ topic: "billing.invoice_paid", payload: { id: "inv-1" } })
      })
    );

    const ack = await waitForFrame(publisher, (frame) => frame.type === "ACK" && frame.requestId === "pub-1");
    const event = await eventPromise;

    expect(ack.type).toBe("ACK");
    expect(ack.envelope.offset).toBe(1);
    expect(event.type).toBe("EVENT");
    expect((event.envelope.payload as { id: string }).id).toBe("inv-1");

    const replayResponse = await fetch(`http://127.0.0.1:${port}/events?topic=billing.invoice_paid&fromOffset=1`);
    const replayPayload = (await replayResponse.json()) as Envelope[];
    expect(replayPayload).toHaveLength(1);
    expect((replayPayload[0].payload as { id: string }).id).toBe("inv-1");

    publisher.close();
    subscriber.close();
  });

  it("routes request/reply by correlation id", async () => {
    const dbPath = makeDbPath();
    const app = await buildServer({ dbPath });
    await app.listen({ host: "127.0.0.1", port: 0 });
    resources.push({ dbPath, close: async () => app.close() });

    const port = (app.server.address() as AddressInfo).port;
    const wsUrl = `ws://127.0.0.1:${port}/ws`;

    const requester = new WebSocket(wsUrl);
    const responder = new WebSocket(wsUrl);
    await Promise.all([waitForOpen(requester), waitForOpen(responder)]);

    responder.send(JSON.stringify({ type: "SUBSCRIBE", pattern: "math.add" }));
    responder.on("message", (raw) => {
      const frame = JSON.parse(raw.toString()) as Frame;
      if (frame.type !== "EVENT" || frame.envelope.kind !== "request") {
        return;
      }

      const input = frame.envelope.payload as { a: number; b: number };
      responder.send(
        JSON.stringify({
          type: "PUBLISH",
          requestId: "reply-1",
          envelope: createEnvelope({
            topic: frame.envelope.replyTo ?? "_reply.unrouted",
            kind: "reply",
            correlationId: frame.envelope.correlationId,
            causationId: frame.envelope.id,
            payload: { result: input.a + input.b }
          })
        })
      );
    });

    const correlationId = "corr-123";
    requester.send(
      JSON.stringify({
        type: "PUBLISH",
        requestId: "req-1",
        envelope: createEnvelope({
          topic: "math.add",
          kind: "request",
          correlationId,
          replyTo: "_reply.requester",
          payload: { a: 7, b: 8 }
        })
      })
    );

    const requestAck = await waitForFrame(requester, (frame) => frame.type === "ACK" && frame.requestId === "req-1");
    const replyEvent = await waitForFrame(
      requester,
      (frame) =>
        frame.type === "EVENT" && frame.envelope.kind === "reply" && frame.envelope.correlationId === correlationId
    );

    expect(requestAck.type).toBe("ACK");
    expect(replyEvent.type).toBe("EVENT");
    expect((replyEvent.envelope.payload as { result: number }).result).toBe(15);

    requester.close();
    responder.close();
  });
});

