import { matchTopicPattern, validateTopicPattern, type Envelope } from "@kierkegaard/contracts";
import { v7 as uuidv7 } from "uuid";
import type WebSocket from "ws";
import { SqlitePersistence } from "./persistence";

type ConnectionState = {
  id: string;
  socket: WebSocket;
  subscriptions: Set<string>;
};

export type BrokerStats = {
  connections: number;
  pendingRequests: number;
  subscriptions: number;
};

export class BrokerRouter {
  private readonly connections = new Map<string, ConnectionState>();

  private readonly pendingRequests = new Map<string, string>();

  constructor(private readonly persistence: SqlitePersistence) {}

  register(socket: WebSocket): string {
    const id = uuidv7();
    this.connections.set(id, { id, socket, subscriptions: new Set() });
    return id;
  }

  unregister(clientId: string): void {
    this.connections.delete(clientId);
    for (const [correlationId, requesterId] of this.pendingRequests.entries()) {
      if (requesterId === clientId) {
        this.pendingRequests.delete(correlationId);
      }
    }
  }

  subscribe(clientId: string, pattern: string): void {
    validateTopicPattern(pattern);
    const client = this.connections.get(clientId);
    if (!client) {
      return;
    }
    client.subscriptions.add(pattern);
  }

  unsubscribe(clientId: string, pattern: string): void {
    const client = this.connections.get(clientId);
    if (!client) {
      return;
    }
    client.subscriptions.delete(pattern);
  }

  publishFromClient(clientId: string, envelope: Envelope): Envelope {
    const persisted = this.persistence.insertEvent(envelope);
    this.routePersistedEnvelope(clientId, persisted);
    return persisted;
  }

  stats(): BrokerStats {
    const subscriptions = [...this.connections.values()].reduce((acc, connection) => acc + connection.subscriptions.size, 0);
    return {
      connections: this.connections.size,
      pendingRequests: this.pendingRequests.size,
      subscriptions
    };
  }

  private routePersistedEnvelope(publisherId: string, envelope: Envelope): void {
    if (envelope.kind === "request" && envelope.correlationId) {
      this.pendingRequests.set(envelope.correlationId, publisherId);
    }

    if ((envelope.kind === "reply" || envelope.kind === "error") && envelope.correlationId) {
      const requesterId = this.pendingRequests.get(envelope.correlationId);
      if (requesterId) {
        this.pendingRequests.delete(envelope.correlationId);
        this.sendEventToClient(requesterId, envelope);
      }
    }

    this.broadcastToSubscribers(envelope);
  }

  private broadcastToSubscribers(envelope: Envelope): void {
    this.connections.forEach((client) => {
      for (const pattern of client.subscriptions) {
        if (matchTopicPattern(pattern, envelope.topic)) {
          this.sendEventToClient(client.id, envelope);
          break;
        }
      }
    });
  }

  private sendEventToClient(clientId: string, envelope: Envelope): void {
    const client = this.connections.get(clientId);
    if (!client || client.socket.readyState !== 1) {
      return;
    }
    client.socket.send(JSON.stringify({ type: "EVENT", envelope }));
  }
}

