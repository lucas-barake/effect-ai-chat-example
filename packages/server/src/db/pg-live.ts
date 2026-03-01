import * as PgClient from "@effect/sql-pg/PgClient";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Str from "effect/String";

export const PgLive = Layer.unwrap(
  Effect.gen(function*() {
    const databaseUrl = yield* Config.redacted("DATABASE_URL");
    return PgClient.layer({
      url: databaseUrl,
      transformQueryNames: Str.camelToSnake,
      transformResultNames: Str.snakeToCamel,
      transformJson: true,
    });
  }),
).pipe(Layer.orDie);
