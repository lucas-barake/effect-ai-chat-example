import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { UsersRpc } from "@app/domain";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { createServer } from "node:http";
import { AuthMiddlewareLive } from "./api/auth-middleware-live.js";
import { UsersRpcLive } from "./api/users-rpc-live.js";
import { PgLive } from "./db/pg-live.js";

const RpcRouter = RpcServer.layerHttp({
  group: UsersRpc,
  path: "/rpc",
});

const AllRoutes = Layer.mergeAll(RpcRouter).pipe(
  Layer.provide(Layer.mergeAll(UsersRpcLive, AuthMiddlewareLive)),
  Layer.provide(RpcSerialization.layerNdjson),
  Layer.provide(
    HttpRouter.cors({
      allowedOrigins: ["*"],
      allowedMethods: ["GET", "POST", "PUT", "DELETE", "PATCH"],
      allowedHeaders: ["Content-Type"],
    }),
  ),
);

const ServerLayer = HttpRouter.serve(AllRoutes).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3000 })),
  Layer.provide(PgLive),
);

NodeRuntime.runMain(Layer.launch(ServerLayer));
