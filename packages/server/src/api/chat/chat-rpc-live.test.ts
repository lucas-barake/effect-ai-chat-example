import { AuthMiddlewareLive } from "@/api/auth-middleware-live.js";
import { AiModels } from "@/lib/ai-models.js";
import * as Chat from "@app/domain/api/chat-rpc";
import { describe, expect, it } from "@effect/vitest";
import { withLanguageModel } from "@test/utils/with-language-model.js";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { RpcTest } from "effect/unstable/rpc";
import { ChatRpcHandler } from "./chat-rpc-live.js";

const MockAiModels = Layer.mock(AiModels)({
  use: (_model) => (effect) =>
    withLanguageModel(effect, {
      streamText: [{ type: "text-delta" as const, id: "t1", delta: "Hello from AI" }],
    }),
});

const TestLayer = Layer.mergeAll(
  ChatRpcHandler.pipe(Layer.provide(MockAiModels)),
  AuthMiddlewareLive,
);

describe("ChatRpc", () => {
  it.effect("chat_ask streams MessageEvent Chunk events through the full pipeline", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc);

      const events = yield* client.chat_ask({
        messages: [{ role: "user", content: "Say hello" }],
        model: "haiku-4.5",
      }).pipe(Stream.runCollect);

      expect(events.some((e) => e._tag === "Chunk" && "delta" in e && e.delta === "Hello from AI"))
        .toBe(true);
    }).pipe(Effect.provide(TestLayer)));
});
