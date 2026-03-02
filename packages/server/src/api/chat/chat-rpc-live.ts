import { AiModels } from "@/lib/ai-models.js";
import * as Chat from "@app/domain/api/chat-rpc";
import type { Done } from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Rpc from "effect/unstable/rpc/Rpc";
import { ChatProcessor } from "./chat-processor.js";
import { ChatToolkitLive } from "./chat-toolkit-live.js";
import { ChatMailbox } from "./chat-toolkit.js";

export const ChatRpcHandler = Chat.ChatRpc.toLayer(
  Effect.gen(function*() {
    const aiModels = yield* AiModels;

    return Chat.ChatRpc.of({
      chat_ask: (payload) =>
        Effect.gen(function*() {
          const mailbox = yield* Queue.make<Chat.MessageEvent, Done>();

          yield* Effect.gen(function*() {
            const processor = yield* ChatProcessor;
            yield* processor.run(payload.messages);
          }).pipe(
            aiModels.use(payload.model),
            Effect.catchTags({
              AiError: (e) => Effect.die(e),
            }),
            Effect.ensuring(Queue.end(mailbox)),
            Effect.provide(ChatProcessor.layer),
            Effect.provide(ChatToolkitLive),
            Effect.provideService(ChatMailbox, mailbox),
            Effect.forkScoped,
          );

          return mailbox;
        }),
    });
  }),
);

export const ChatRpcLive: Layer.Layer<Rpc.Handler<"chat_ask">> = ChatRpcHandler.pipe(
  Layer.provide(AiModels.layer),
);
