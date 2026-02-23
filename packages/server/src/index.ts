import path from "node:path";
import { buildServer } from "./app";

const port = Number(process.env.PORT ?? "7777");
const host = process.env.HOST ?? "0.0.0.0";
const dbPath = process.env.BROKER_DB_PATH ?? path.resolve(process.cwd(), "federated-kafka.sqlite");

const start = async (): Promise<void> => {
  const app = await buildServer({ dbPath, logger: true });
  await app.listen({ host, port });
};

start().catch((error) => {
  console.error(error);
  process.exit(1);
});

