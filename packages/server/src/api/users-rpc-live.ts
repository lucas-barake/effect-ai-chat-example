import { CurrentUser, UsersRpc } from "@app/domain";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const UsersRpcLive = UsersRpc.toLayer(Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient;
  return UsersRpc.of({
    GetMe: () =>
      Effect.gen(function*() {
        yield* sql`SELECT 1`.pipe(Effect.orDie);
        return yield* CurrentUser;
      }),
  });
}));
