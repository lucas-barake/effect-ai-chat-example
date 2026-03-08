import * as BunServices from "@effect/platform-bun/BunServices";
import * as PgMigrator from "@effect/sql-pg/PgMigrator";
import * as Layer from "effect/Layer";
import * as Migrator from "effect/unstable/sql/Migrator";
import { PgLive } from "./pg-live.js";

export const MigrationLayer: Layer.Layer<never> = PgMigrator.layer({
  loader: Migrator.fromFileSystem(new URL("./migrations", import.meta.url).pathname),
}).pipe(
  Layer.provide(PgLive),
  Layer.provide(BunServices.layer),
  Layer.orDie,
);
