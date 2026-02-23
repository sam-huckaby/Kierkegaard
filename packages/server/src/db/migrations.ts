import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Database } from "better-sqlite3";

export type MigrationRunnerOptions = {
  migrationsDir?: string;
};

type MigrationRow = {
  name: string;
};

export const runMigrations = (db: Database, options: MigrationRunnerOptions = {}): void => {
  const defaultDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "migrations");
  const migrationsDir = options.migrationsDir ?? defaultDir;
  const files = fs
    .readdirSync(migrationsDir)
    .filter((entry) => entry.endsWith(".sql"))
    .sort((a, b) => a.localeCompare(b));

  db.exec(`
    CREATE TABLE IF NOT EXISTS migrations (
      name TEXT PRIMARY KEY,
      applied_ts INTEGER NOT NULL
    );
  `);

  const selectApplied = db.prepare("SELECT name FROM migrations WHERE name = ?");
  const insertApplied = db.prepare("INSERT INTO migrations(name, applied_ts) VALUES (?, ?)");

  files.forEach((fileName) => {
    const alreadyApplied = selectApplied.get(fileName) as MigrationRow | undefined;
    if (alreadyApplied) {
      return;
    }

    const sql = fs.readFileSync(path.join(migrationsDir, fileName), "utf-8");
    const tx = db.transaction(() => {
      db.exec(sql);
      insertApplied.run(fileName, Date.now());
    });
    tx();
  });
};

