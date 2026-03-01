import * as Effect from "effect/Effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import { DomainRpcClient } from "../services/rpc-client.js";

const runtime = Atom.runtime(DomainRpcClient.layer);

export const currentUserAtom = runtime.atom(
  Effect.gen(function*() {
    const client = yield* DomainRpcClient;
    return yield* client.GetMe();
  }),
);
