import { AiModels } from "@/lib/ai-models.js"
import { ChatRepo } from "@/db/chat-repo.js"
import type { ChatModel } from "@/db/chat-model.js"
import * as Chat from "@app/domain/api/chat-rpc"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Ref from "effect/Ref"
import * as Scope from "effect/Scope"
import * as ServiceMap from "effect/ServiceMap"
import * as Stream from "effect/Stream"
import { ChatProcessor } from "./chat-processor.js"
import { ChatToolkitLive } from "./chat-toolkit-live.js"
import { ChatMailbox } from "./chat-toolkit.js"

export class ChatRunManager extends ServiceMap.Service<ChatRunManager, {
  readonly subscribe: (
    chatId: Chat.ChatId,
  ) => Effect.Effect<Stream.Stream<Chat.ChatEvent>>
  readonly startGeneration: (
    chatId: Chat.ChatId,
    chat: typeof ChatModel.Type,
    message: string,
    reconciliationId: Chat.ReconciliationId,
  ) => Effect.Effect<void, Chat.GenerationInProgressError>
  readonly interrupt: (
    chatId: Chat.ChatId,
  ) => Effect.Effect<void>
}>()("ChatRunManager", {
  make: Effect.gen(function*() {
    const aiModels = yield* AiModels
    const chatRepo = yield* ChatRepo
    const managerScope = yield* Effect.scope

    const eventChannels = yield* RcMap.make({
      lookup: (_chatId: Chat.ChatId) =>
        PubSub.unbounded<Chat.ChatEvent>({ replay: 50000 }),
      idleTimeToLive: "2 minutes",
    })

    type RunEntry = {
      pubsub: PubSub.PubSub<Chat.ChatEvent>
      interrupt: Deferred.Deferred<void>
    }
    const activeRuns = yield* Ref.make(
      HashMap.empty<Chat.ChatId, RunEntry | "starting">(),
    )

    return {
      subscribe: Effect.fnUntraced(function*(chatId: Chat.ChatId) {
        return Stream.unwrap(
          RcMap.get(eventChannels, chatId).pipe(
            Effect.map((pubsub) => Stream.fromPubSub(pubsub)),
          ),
        )
      }),

      startGeneration: Effect.fnUntraced(function*(
        chatId: Chat.ChatId,
        chat: typeof ChatModel.Type,
        message: string,
        reconciliationId: Chat.ReconciliationId,
      ) {
        const reserved = yield* Ref.modify(activeRuns, (map) => {
          if (HashMap.has(map, chatId)) return [false, map] as const
          return [true, HashMap.set(map, chatId, "starting" as const)] as const
        })
        if (!reserved) return yield* new Chat.GenerationInProgressError({ chatId })

        const generationScope = yield* Scope.make()

        const pubsub = yield* RcMap.get(eventChannels, chatId).pipe(
          Scope.provide(generationScope),
        )

        yield* PubSub.publish(pubsub, { _tag: "GenerationStarted", reconciliationId })

        const interrupt = yield* Deferred.make<void>()

        yield* Effect.gen(function*() {
          const processor = yield* ChatProcessor
          const aiMessages = yield* processor.run(chat, message)
          yield* chatRepo.updateMessages({
            chatId: chat.id,
            userId: chat.userId,
            messages: [...chat.messages, { role: "user" as const, content: message }, ...aiMessages],
          })
          yield* PubSub.publish(pubsub, { _tag: "Done" })
        }).pipe(
          aiModels.use(chat.model),
          Effect.raceFirst(
            Deferred.await(interrupt).pipe(
              Effect.andThen(PubSub.publish(pubsub, { _tag: "Interrupted" })),
            ),
          ),
          Effect.catchTags({
            AiError: (e) =>
              PubSub.publish(pubsub, { _tag: "Error", message: e.message }).pipe(
                Effect.andThen(PubSub.publish(pubsub, { _tag: "Done" })),
              ),
          }),
          Effect.ensuring(Effect.gen(function*() {
            yield* Ref.update(activeRuns, HashMap.remove(chatId))
            yield* Scope.close(generationScope, Exit.void)
          })),
          Effect.provide(ChatProcessor.layer),
          Effect.provide(ChatToolkitLive),
          Effect.provideService(ChatMailbox, pubsub),
          Effect.asVoid,
          Effect.forkIn(managerScope),
        )

        yield* Ref.update(activeRuns, (map) => HashMap.set(map, chatId, { pubsub, interrupt }))
      }),

      interrupt: Effect.fnUntraced(function*(chatId: Chat.ChatId) {
        const map = yield* Ref.get(activeRuns)
        const entry = HashMap.get(map, chatId)
        if (Option.isSome(entry) && entry.value !== "starting") {
          yield* Deferred.succeed(entry.value.interrupt, void 0 as void)
        }
      }),
    }
  }),
}) {
  static layer: Layer.Layer<ChatRunManager> = Layer.effect(this, this.make).pipe(
    Layer.provide(AiModels.layer),
    Layer.provide(ChatRepo.layer),
  )
}
