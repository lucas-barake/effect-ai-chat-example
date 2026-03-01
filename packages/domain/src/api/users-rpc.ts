import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware";
import { CurrentUser, CurrentUserSchema } from "../CurrentUser.js";

export class AuthMiddleware extends RpcMiddleware.Service<AuthMiddleware, {
  provides: CurrentUser;
}>()(
  "AuthMiddleware",
  { error: Schema.Never, requiredForClient: false },
) {}

export class GetMe extends Rpc.make("GetMe", {
  success: CurrentUserSchema,
}) {}

export class UsersRpc extends RpcGroup.make(GetMe).middleware(AuthMiddleware) {}
