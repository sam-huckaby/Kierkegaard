import Database from "better-sqlite3";
import { EnvelopeSchema, type Envelope, type Topic } from "@federated-kafka/contracts";
import path from "node:path";
import { runMigrations } from "../db/migrations";

export type ReplayQuery = {
  fromOffset?: number;
  lastN?: number;
};

type EventRow = {
  id: string;
  topic: string;
  ts: number;
  kind: Envelope["kind"];
  correlation_id: string | null;
  causation_id: string | null;
  reply_to: string | null;
  schema_version: number;
  producer: string | null;
  event_key: string | null;
  payload_json: string;
  offset: number;
};

type OffsetRow = {
  next_offset: number;
};

export class SqlitePersistence {
  private readonly db: Database.Database;

  private readonly selectOffsetStmt;

  private readonly insertOffsetStmt;

  private readonly updateOffsetStmt;

  private readonly insertEventStmt;

  private readonly selectByTopicFromStmt;

  private readonly selectByTopicLastNStmt;

  private readonly selectByTopicStmt;

  constructor(dbPath: string) {
    const resolvedDbPath = path.resolve(dbPath);
    this.db = new Database(resolvedDbPath);
    this.db.pragma("journal_mode = WAL");
    runMigrations(this.db);

    this.selectOffsetStmt = this.db.prepare("SELECT next_offset FROM topic_offsets WHERE topic = ?");
    this.insertOffsetStmt = this.db.prepare("INSERT INTO topic_offsets(topic, next_offset) VALUES (?, ?)");
    this.updateOffsetStmt = this.db.prepare("UPDATE topic_offsets SET next_offset = ? WHERE topic = ?");
    this.insertEventStmt = this.db.prepare(`
      INSERT INTO events(
        id, topic, ts, kind, correlation_id, causation_id, reply_to,
        schema_version, producer, event_key, payload_json, offset
      )
      VALUES (
        @id, @topic, @ts, @kind, @correlation_id, @causation_id, @reply_to,
        @schema_version, @producer, @event_key, @payload_json, @offset
      );
    `);
    this.selectByTopicFromStmt = this.db.prepare(
      "SELECT * FROM events WHERE topic = ? AND offset >= ? ORDER BY offset ASC"
    );
    this.selectByTopicLastNStmt = this.db.prepare(
      "SELECT * FROM events WHERE topic = ? ORDER BY offset DESC LIMIT ?"
    );
    this.selectByTopicStmt = this.db.prepare("SELECT * FROM events WHERE topic = ? ORDER BY offset ASC");
  }

  insertEvent(envelope: Envelope): Envelope {
    const persisted = this.db.transaction((incoming: Envelope): Envelope => {
      const validated = EnvelopeSchema.parse(incoming);
      const currentOffset = this.selectOffsetStmt.get(validated.topic) as OffsetRow | undefined;
      const nextOffset = (currentOffset?.next_offset ?? 0) + 1;
      if (currentOffset) {
        this.updateOffsetStmt.run(nextOffset, validated.topic);
      } else {
        this.insertOffsetStmt.run(validated.topic, nextOffset);
      }

      this.insertEventStmt.run({
        id: validated.id,
        topic: validated.topic,
        ts: validated.ts,
        kind: validated.kind,
        correlation_id: validated.correlationId ?? null,
        causation_id: validated.causationId ?? null,
        reply_to: validated.replyTo ?? null,
        schema_version: validated.schemaVersion,
        producer: validated.producer ?? null,
        event_key: validated.key ?? null,
        payload_json: JSON.stringify(validated.payload),
        offset: nextOffset
      });

      return {
        ...validated,
        offset: nextOffset
      };
    })(envelope);

    return persisted;
  }

  replay(topic: Topic, query: ReplayQuery = {}): Envelope[] {
    let rows: EventRow[];
    if (query.fromOffset !== undefined) {
      rows = this.selectByTopicFromStmt.all(topic, query.fromOffset) as EventRow[];
    } else if (query.lastN !== undefined) {
      rows = this.selectByTopicLastNStmt.all(topic, query.lastN) as EventRow[];
      rows = rows.reverse();
    } else {
      rows = this.selectByTopicStmt.all(topic) as EventRow[];
    }

    return rows.map((row) => {
      const envelope: Envelope = {
        id: row.id,
        topic: row.topic,
        ts: row.ts,
        kind: row.kind,
        correlationId: row.correlation_id ?? undefined,
        causationId: row.causation_id ?? undefined,
        replyTo: row.reply_to ?? undefined,
        schemaVersion: row.schema_version,
        producer: row.producer ?? undefined,
        key: row.event_key ?? undefined,
        payload: JSON.parse(row.payload_json),
        offset: row.offset
      };
      return EnvelopeSchema.parse(envelope);
    });
  }

  close(): void {
    this.db.close();
  }
}

