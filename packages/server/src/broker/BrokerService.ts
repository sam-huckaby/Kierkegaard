import { Context, Layer } from "effect";
import { BrokerRouter } from "./routing";
import { SqlitePersistence } from "./persistence";

export class PersistenceService extends Context.Tag("kierkegaard/server/PersistenceService")<
  PersistenceService,
  SqlitePersistence
>() {}

export class RouterService extends Context.Tag("kierkegaard/server/RouterService")<
  RouterService,
  BrokerRouter
>() {}

export const makePersistenceLayer = (dbPath: string) =>
  Layer.succeed(PersistenceService, new SqlitePersistence(dbPath));

export const makeRouterLayer = (persistence: SqlitePersistence) =>
  Layer.succeed(RouterService, new BrokerRouter(persistence));

