import type { ChatModel } from "@/db/chat-model.js";
import { ChatRepo } from "@/db/chat-repo.js";
import { AiModels } from "@/lib/ai-models.js";
import * as Chat from "@app/domain/api/chat-rpc";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HashMap from "effect/HashMap";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as RcMap from "effect/RcMap";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as ServiceMap from "effect/ServiceMap";
import * as Stream from "effect/Stream";
import { ChatProcessor } from "./chat-processor.js";
import { ChatToolkitLive } from "./chat-toolkit-live.js";
import { ChatMailbox } from "./chat-toolkit.js";

export class ChatRunManager extends ServiceMap.Service<
  ChatRunManager,
  {
    readonly subscribe: (chatId: Chat.ChatId) => Stream.Stream<Chat.ChatEvent>;
    readonly startGeneration: (args: {
      readonly chatId: Chat.ChatId;
      readonly chat: typeof ChatModel.Type;
      readonly message: string;
      readonly reconciliationId: Chat.ReconciliationId;
    }) => Effect.Effect<void, Chat.GenerationInProgressError>;
    readonly interrupt: (chatId: Chat.ChatId) => Effect.Effect<void>;
  }
>()("ChatRunManager", {
  make: Effect.gen(function*() {
    const aiModels = yield* AiModels;
    const processor = yield* ChatProcessor;
    const chatRepo = yield* ChatRepo;

    const eventChannels = yield* RcMap.make({
      lookup: (_chatId: Chat.ChatId) => PubSub.unbounded<Chat.ChatEvent>({ replay: Infinity }),
      idleTimeToLive: "2 minutes",
    });

    const activeRuns = yield* Ref.make(HashMap.empty<Chat.ChatId, Scope.Closeable>());

    return {
      subscribe: (chatId) =>
        Stream.unwrap(
          RcMap.get(eventChannels, chatId).pipe(
            Effect.map((pubsub) => Stream.fromPubSub(pubsub)),
          ),
        ),

      startGeneration: Effect.fnUntraced(function*(args) {
        const generationScope = yield* Scope.make();

        const pubsub = yield* RcMap.get(eventChannels, args.chatId).pipe(
          Scope.provide(generationScope),
        );

        const reserved = yield* Ref.modify(activeRuns, (map) => {
          if (HashMap.has(map, args.chatId)) return [false, map] as const;
          return [
            true,
            HashMap.set(map, args.chatId, generationScope),
          ] as const;
        });
        if (!reserved) {
          yield* Scope.close(generationScope, Exit.void);
          return yield* new Chat.GenerationInProgressError({
            chatId: args.chatId,
          });
        }

        yield* Effect.uninterruptible(Effect.gen(function*() {
          yield* PubSub.publish(pubsub, {
            _tag: "GenerationStarted",
            reconciliationId: args.reconciliationId,
          });

          yield* Effect.gen(function*() {
            const aiMessages = yield* processor.run(args.chat, args.message);
            yield* chatRepo.updateMessages({
              chatId: args.chat.id,
              userId: args.chat.userId,
              messages: [
                ...args.chat.messages,
                { role: "user" as const, content: args.message },
                ...aiMessages,
              ],
            });
          }).pipe(
            aiModels.use(args.chat.model),
            // interruption is encoded in Failure.cause to keep one failure terminal shape
            Effect.onExit((exit) =>
              Exit.isSuccess(exit)
                ? PubSub.publish(pubsub, { _tag: "Done" })
                : PubSub.publish(pubsub, { _tag: "Failure", cause: exit.cause })
            ),
            Effect.ensuring(
              Effect.gen(function*() {
                yield* Ref.update(activeRuns, HashMap.remove(args.chatId));
                yield* Scope.close(generationScope, Exit.void);
              }),
            ),
            Effect.provide(ChatProcessor.layer),
            Effect.provide(ChatToolkitLive),
            Effect.provideService(ChatMailbox, pubsub),
            Effect.asVoid,
            Effect.forkIn(generationScope, { startImmediately: true }),
          );
        }));
      }),

      interrupt: Effect.fnUntraced(function*(chatId) {
        const map = yield* Ref.get(activeRuns);
        const entry = HashMap.get(map, chatId);
        if (Option.isSome(entry)) {
          yield* Scope.close(entry.value, Exit.void);
        }
      }),
    };
  }),
}) {
  static layer: Layer.Layer<ChatRunManager> = Layer.effect(
    this,
    this.make,
  ).pipe(
    Layer.provide(AiModels.layer),
    Layer.provide(ChatRepo.layer),
    Layer.provide(ChatProcessor.layer),
  );
}
