import { EnvelopeSchema, type Envelope, type Topic } from "@federated-kafka/contracts";
import fs from "node:fs";
import path from "node:path";
import initSqlJs from "sql.js";
import { runMigrations } from "../db/migrations";

export type ReplayQuery = {
  fromOffset?: number;
  lastN?: number;
};

const SQL = await initSqlJs();

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

type OffsetRow = { next_offset: number };
type SqlParamValue = string | number | null;
type SqlParams = Record<string, SqlParamValue>;

export class SqlitePersistence {
  private readonly dbPath: string;

  private readonly db: any;

  constructor(dbPath: string) {
    this.dbPath = path.resolve(dbPath);
    fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
    const existingBytes = fs.existsSync(this.dbPath) ? fs.readFileSync(this.dbPath) : undefined;
    this.db = new SQL.Database(existingBytes);
    runMigrations(this.db);
    this.flushToDisk();
  }

  insertEvent(envelope: Envelope): Envelope {
    const validated = EnvelopeSchema.parse(envelope);
    this.db.run("BEGIN IMMEDIATE");
    try {
      const currentOffset = this.queryRows<OffsetRow>("SELECT next_offset FROM topic_offsets WHERE topic = $topic", {
        $topic: validated.topic
      })[0];
      const nextOffset = (currentOffset?.next_offset ?? 0) + 1;
      if (currentOffset) {
        this.db.run("UPDATE topic_offsets SET next_offset = $nextOffset WHERE topic = $topic", {
          $nextOffset: nextOffset,
          $topic: validated.topic
        });
      } else {
        this.db.run("INSERT INTO topic_offsets(topic, next_offset) VALUES ($topic, $nextOffset)", {
          $topic: validated.topic,
          $nextOffset: nextOffset
        });
      }

      this.db.run(
        `INSERT INTO events(
          id, topic, ts, kind, correlation_id, causation_id, reply_to,
          schema_version, producer, event_key, payload_json, offset
        )
        VALUES (
          $id, $topic, $ts, $kind, $correlationId, $causationId, $replyTo,
          $schemaVersion, $producer, $eventKey, $payloadJson, $offset
        )`,
        {
          $id: validated.id,
          $topic: validated.topic,
          $ts: validated.ts,
          $kind: validated.kind,
          $correlationId: validated.correlationId ?? null,
          $causationId: validated.causationId ?? null,
          $replyTo: validated.replyTo ?? null,
          $schemaVersion: validated.schemaVersion,
          $producer: validated.producer ?? null,
          $eventKey: validated.key ?? null,
          $payloadJson: JSON.stringify(validated.payload),
          $offset: nextOffset
        }
      );

      this.db.run("COMMIT");
      this.flushToDisk();
      const persisted: Envelope = {
        id: validated.id,
        topic: validated.topic,
        ts: validated.ts,
        schemaVersion: validated.schemaVersion,
        kind: validated.kind,
        payload: validated.payload,
        offset: nextOffset
      };
      if (validated.key !== undefined) {
        persisted.key = validated.key;
      }
      if (validated.producer !== undefined) {
        persisted.producer = validated.producer;
      }
      if (validated.correlationId !== undefined) {
        persisted.correlationId = validated.correlationId;
      }
      if (validated.causationId !== undefined) {
        persisted.causationId = validated.causationId;
      }
      if (validated.replyTo !== undefined) {
        persisted.replyTo = validated.replyTo;
      }

      return persisted;
    } catch (error: unknown) {
      this.db.run("ROLLBACK");
      throw error;
    }
  }

  replay(topic: Topic, query: ReplayQuery = {}): Envelope[] {
    let sql = "SELECT * FROM events WHERE topic = $topic ORDER BY offset ASC";
    let params: SqlParams = { $topic: topic };

    if (query.fromOffset !== undefined) {
      sql = "SELECT * FROM events WHERE topic = $topic AND offset >= $fromOffset ORDER BY offset ASC";
      params = { $topic: topic, $fromOffset: query.fromOffset };
    } else if (query.lastN !== undefined) {
      sql = `SELECT * FROM (
        SELECT * FROM events WHERE topic = $topic ORDER BY offset DESC LIMIT $lastN
      ) ORDER BY offset ASC`;
      params = { $topic: topic, $lastN: query.lastN };
    }

    const rows = this.queryRows<EventRow>(sql, params);
    return rows.map((row) => {
      const envelope: Envelope = {
        id: row.id,
        topic: row.topic,
        ts: row.ts,
        schemaVersion: row.schema_version,
        kind: row.kind,
        payload: JSON.parse(row.payload_json),
        offset: row.offset
      };
      if (row.correlation_id !== null) {
        envelope.correlationId = row.correlation_id;
      }
      if (row.causation_id !== null) {
        envelope.causationId = row.causation_id;
      }
      if (row.reply_to !== null) {
        envelope.replyTo = row.reply_to;
      }
      if (row.producer !== null) {
        envelope.producer = row.producer;
      }
      if (row.event_key !== null) {
        envelope.key = row.event_key;
      }

      EnvelopeSchema.parse(envelope);
      return envelope;
    });
  }

  close(): void {
    this.flushToDisk();
    this.db.close();
  }

  private queryRows<T>(sql: string, params?: SqlParams): T[] {
    const result = this.db.exec(sql, params);
    if (result.length === 0) {
      return [];
    }

    const first = (result as Array<{ columns: string[]; values: unknown[][] }>)[0];
    if (!first) {
      return [];
    }
    return first.values.map((row: unknown[]) => {
      const objectRow: Record<string, unknown> = {};
      first.columns.forEach((column: string, index: number) => {
        objectRow[column] = row[index];
      });
      return objectRow as T;
    });
  }

  private flushToDisk(): void {
    const exported = this.db.export();
    fs.writeFileSync(this.dbPath, Buffer.from(exported));
  }
}

