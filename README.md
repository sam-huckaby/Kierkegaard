# Federated Kafka

Kafka-like durable pub/sub + request/reply backbone for Module Federation micro-frontends.

This monorepo provides:

- **`@federated-kafka/contracts`**: shared envelope/topic/protocol contracts
- **`@federated-kafka/broker-remote`**: Module Federation remote exposing:
  - `broker/client`
  - `broker/contracts`
  - `broker/devtools`
- **`@federated-kafka/server`**: Fastify + WebSocket + durable SQLite-backed broker server
- **`@federated-kafka/host-shell`**, **`remote-a`**, **`remote-b`**: demo MF host/remotes

The primary value is first-class **request/reply** for federated apps, with a unified client API that can run in:

- `memory` mode (single-page runtime)
- `server` mode (durable + replayable)

## Why this design

- Topic strings + payload schemas are the coupling boundary.
- Broker API is imported as a **federated remote** (not shared source import), so remotes are loosely coupled.
- Broker singleton is guarded with `globalThis[Symbol.for("federated-kafka/broker")]` to prevent accidental duplication.
- Server ack guarantees durability: once a publish ACK is returned, the event has been persisted.

## Tech Stack

- Vite + React + TypeScript
- Module Federation plugin: **`@originjs/vite-plugin-federation`**
- Vitest + Testing Library
- Playwright (e2e smoke)
- Fastify + `@fastify/websocket`
- SQLite engine (`sql.js`) persisted to `.sqlite` file on disk
- Effect.ts for service/layer orchestration in broker/client and server bootstrap paths

## Monorepo Structure

```text
packages/
  contracts/
  broker-remote/
  server/
  host-shell/
  remote-a/
  remote-b/
```

## Quick Start

```bash
pnpm install
pnpm dev
```

`pnpm dev` runs all dev servers in parallel:

- server (Fastify): `http://127.0.0.1:7777`
- broker remote: `http://127.0.0.1:4173`
- host shell: `http://127.0.0.1:4170`
- remote-a: `http://127.0.0.1:4171`
- remote-b: `http://127.0.0.1:4172`

## Configuration

See `.env.example`.

Important variables:

- `BROKER_DRIVER=memory|server`
- `BROKER_WS_URL=ws://127.0.0.1:7777/ws`
- `BROKER_HTTP_URL=http://127.0.0.1:7777`
- `BROKER_DB_PATH=./federated-kafka.sqlite`
- `VITE_BROKER_DRIVER=memory|server` (host runtime selection)

## Public Broker Client API

Imported from `broker/client`:

- `publish(topic, payload, options?) -> Promise<Envelope>`
- `subscribe(topicPattern, handler, options?) -> Unsubscribe`
- `request(topic, payload, options?) -> Promise<ReplyPayload>`
- `respond(topicPattern, handler, options?) -> Unsubscribe`
- `replay(topic, { fromOffset | lastN }) -> AsyncIterable<Envelope>`
- `stats()`
- `setDriver({ type: "memory" | "server", ... })`

### Envelope shape

```ts
type Envelope<T = unknown> = {
  id: string;
  topic: string;
  key?: string;
  ts: number;
  schemaVersion: number;
  producer?: string;
  correlationId?: string;
  causationId?: string;
  replyTo?: string;
  kind: "event" | "request" | "reply" | "error";
  payload: T;
  offset?: number;
};
```

### Topic patterns

- Exact: `billing.invoice_paid`
- Suffix wildcard: `billing.*`
- Global wildcard: `*`

## Durable Server API

- `GET /health`
- `GET /stats`
- `GET /events?topic=<topic>&fromOffset=<n>`
- `GET /events?topic=<topic>&lastN=<n>`
- `WS /ws` for publish/subscribe/request/reply frames

Server persistence stores:

- envelope metadata + payload
- per-topic monotonically increasing offset
- correlation metadata for request/reply

## Module Federation Integration Guide (for a new MFE)

1. Configure remote in your host/remotes:

```ts
// vite.config.ts
federation({
  remotes: {
    broker: "http://127.0.0.1:4173/assets/remoteEntry.js"
  }
});
```

2. Import broker API from federation:

```ts
import { publish, subscribe, request, respond, setDriver } from "broker/client";
```

3. Choose driver at app bootstrap:

```ts
await setDriver({
  type: "server",
  wsUrl: "ws://127.0.0.1:7777/ws",
  httpUrl: "http://127.0.0.1:7777"
});
```

4. Use request/reply:

```ts
respond("math.add", ({ a, b }) => ({ result: a + b }));
const reply = await request<{ result: number }>("math.add", { a: 1, b: 2 });
```

## Tests

Run all unit/integration tests:

```bash
pnpm test
pnpm typecheck
pnpm build
```

Run e2e smoke test:

```bash
pnpm -F @federated-kafka/host-shell e2e
```

Current automated coverage includes:

- topic matching + pattern validation
- envelope generation/schema validation
- memory driver pub/sub + request/reply + timeout + replay ring buffer
- server durability + per-topic offsets + replay
- websocket publish/subscribe integration
- server-driver request/reply correlation + timeout + replay
- host+two-remotes+broker e2e smoke (publish + request/reply)

