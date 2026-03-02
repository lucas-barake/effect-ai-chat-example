import { UsersRpc } from "@app/domain/api/users-rpc";
import { CurrentUser } from "@app/domain/auth";
import { Layer } from "effect";
import * as Effect from "effect/Effect";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { PgLive } from "../db/pg-live";

export const UsersRpcLive: Layer.Layer<Rpc.Handler<"GetMe">> = UsersRpc.toLayer(
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient;
    return UsersRpc.of({
      GetMe: () =>
        Effect.gen(function*() {
          yield* sql`SELECT 1`.pipe(
            Effect.catchTag("SqlError", (e) => Effect.die(e)),
          );
          return yield* CurrentUser;
        }),
    });
  }),
).pipe(Layer.provide(PgLive));
