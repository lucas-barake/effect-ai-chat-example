import { UsersRpc } from "@app/domain";
import * as Layer from "effect/Layer";
import * as ServiceMap from "effect/ServiceMap";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import type * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";

export class DomainRpcClient extends ServiceMap.Service<
  DomainRpcClient,
  RpcClient.RpcClient<RpcGroup.Rpcs<typeof UsersRpc>, RpcClientError.RpcClientError>
>()("DomainRpcClient") {
  static layer = Layer.effect(DomainRpcClient)(
    RpcClient.make(UsersRpc),
  ).pipe(
    Layer.provide(RpcClient.layerProtocolHttp({ url: "http://localhost:3000/rpc" })),
    Layer.provide(RpcSerialization.layerNdjson),
    Layer.provide(FetchHttpClient.layer),
  );
}
