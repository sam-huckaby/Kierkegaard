import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export type MigrationRunnerOptions = {
  migrationsDir?: string;
};

type SqlJsDatabase = {
  exec: (sql: string, params?: Record<string, string | number | null>) => Array<{ values: unknown[][] }>;
  run: (sql: string, params?: Record<string, string | number | null>) => unknown;
};

const hasMigration = (db: SqlJsDatabase, migrationName: string): boolean => {
  const result = db.exec("SELECT name FROM migrations WHERE name = $name", { $name: migrationName });
  return result.length > 0 && (result[0]?.values.length ?? 0) > 0;
};

export const runMigrations = (db: SqlJsDatabase, options: MigrationRunnerOptions = {}): void => {
  const defaultDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "migrations");
  const migrationsDir = options.migrationsDir ?? defaultDir;
  const files = fs
    .readdirSync(migrationsDir)
    .filter((entry) => entry.endsWith(".sql"))
    .sort((a, b) => a.localeCompare(b));

  db.run(`
    CREATE TABLE IF NOT EXISTS migrations (
      name TEXT PRIMARY KEY,
      applied_ts INTEGER NOT NULL
    );
  `);

  files.forEach((fileName) => {
    if (hasMigration(db, fileName)) {
      return;
    }

    const sql = fs.readFileSync(path.join(migrationsDir, fileName), "utf-8");
    db.run("BEGIN IMMEDIATE");
    try {
      db.exec(sql);
      db.run("INSERT INTO migrations(name, applied_ts) VALUES ($name, $appliedTs)", {
        $name: fileName,
        $appliedTs: Date.now()
      });
      db.run("COMMIT");
    } catch (error: unknown) {
      db.run("ROLLBACK");
      throw error;
    }
  });
};

