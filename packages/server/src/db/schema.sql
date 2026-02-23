CREATE TABLE IF NOT EXISTS migrations (
  name TEXT PRIMARY KEY,
  applied_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS topic_offsets (
  topic TEXT PRIMARY KEY,
  next_offset INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  ts INTEGER NOT NULL,
  kind TEXT NOT NULL,
  correlation_id TEXT,
  causation_id TEXT,
  reply_to TEXT,
  schema_version INTEGER NOT NULL,
  producer TEXT,
  event_key TEXT,
  payload_json TEXT NOT NULL,
  offset INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_topic_offset ON events (topic, offset);
CREATE INDEX IF NOT EXISTS idx_events_topic ON events (topic);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events (correlation_id);
