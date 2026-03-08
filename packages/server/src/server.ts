import { AppRpc } from "@app/domain/api/app-rpc";
import * as BunHttpServer from "@effect/platform-bun/BunHttpServer";
import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { AuthMiddlewareLive } from "./api/auth-middleware-live.js";
import { ChatRpcLive } from "./api/chat/chat-rpc-live.js";
import { UsersRpcLive } from "./api/users-rpc-live.js";
import { MigrationLayer } from "./db/migrator.js";
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
  Layer.provide(BunHttpServer.layer({ port: 3000 })),
  Layer.provide(TracerLive),
  Layer.provide(MigrationLayer),
  Layer.orDie,
);

BunRuntime.runMain(Layer.launch(ServerLayer));
