import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import { AuthMiddleware, CurrentUserSchema } from "../auth.js";

export class GetMe extends Rpc.make("GetMe", {
  success: CurrentUserSchema,
}) {}

export class UsersRpc extends RpcGroup.make(GetMe).middleware(AuthMiddleware) {}
