import { AppRpc } from "@app/domain/api/app-rpc";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { createServer } from "node:http";
import { AuthMiddlewareLive } from "./api/auth-middleware-live.js";
import { ChatRpcLive } from "./api/chat/chat-rpc-live.js";
import { UsersRpcLive } from "./api/users-rpc-live.js";
import { TracerLive } from "./lib/tracer.js";

const RpcRouter = RpcServer.layerHttp({
  group: AppRpc,
  path: "/rpc",
});

const AllRoutes = Layer.mergeAll(RpcRouter).pipe(
  Layer.provide(Layer.mergeAll(UsersRpcLive, ChatRpcLive, AuthMiddlewareLive)),
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
  Layer.provide(TracerLive),
  Layer.orDie,
);

NodeRuntime.runMain(Layer.launch(ServerLayer));
