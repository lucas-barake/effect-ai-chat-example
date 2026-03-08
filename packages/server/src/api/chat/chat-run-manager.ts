import type { ChatModel } from "@/db/chat-model.js";
import { ChatRepo } from "@/db/chat-repo.js";
import { AiModels } from "@/lib/ai-models.js";
import * as Chat from "@app/domain/api/chat-rpc";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as HashMap from "effect/HashMap";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as RcMap from "effect/RcMap";
import * as Ref from "effect/Ref";
import * as ServiceMap from "effect/ServiceMap";
import * as Stream from "effect/Stream";
import type * as Take from "effect/Take";
import { ChatProcessor } from "./chat-processor.js";
import { ChatToolkitLive } from "./chat-toolkit-live.js";
import { ChatMailbox } from "./chat-toolkit.js";

export class ChatRunManager extends ServiceMap.Service<
  ChatRunManager,
  {
    readonly watch: (chatId: Chat.ChatId) => Stream.Stream<Chat.ChatWatchEvent>;
    readonly subscribe: (
      runId: Chat.RunId,
      userId: string,
    ) => Stream.Stream<Chat.ChatEvent, typeof Chat.ChatRunError.Type>;
    readonly startGeneration: (args: {
      readonly chatId: Chat.ChatId;
      readonly chat: typeof ChatModel.Type;
      readonly message: string;
    }) => Effect.Effect<{ readonly runId: Chat.RunId; }, Chat.GenerationInProgressError>;
    readonly interrupt: (chatId: Chat.ChatId) => Effect.Effect<void>;
  }
>()("ChatRunManager", {
  make: Effect.gen(function*() {
    const aiModels = yield* AiModels;
    const processor = yield* ChatProcessor;
    const chatRepo = yield* ChatRepo;

    const runOwners = yield* Ref.make(
      HashMap.empty<Chat.RunId, { readonly chatId: Chat.ChatId; readonly userId: string; }>(),
    );
    const watchChannels = yield* RcMap.make({
      lookup: (_chatId: Chat.ChatId) => PubSub.unbounded<Chat.ChatWatchEvent>({ replay: Infinity }),
      idleTimeToLive: "2 minutes",
    });
    const eventChannels = yield* RcMap.make({
      lookup: (runId: Chat.RunId) =>
        Effect.gen(function*() {
          yield* Ref.get(runOwners).pipe(
            Effect.flatMap((map) =>
              Option.match(HashMap.get(map, runId), {
                onNone: () => Effect.fail(new Chat.ChatRunNotFoundError({ runId })),
                onSome: Effect.succeed,
              })
            ),
          );
          return yield* Effect.acquireRelease(
            PubSub.unbounded<Take.Take<Chat.ChatEvent, typeof Chat.ChatRunTerminalError.Type>>({
              replay: Infinity,
            }),
            () => Ref.update(runOwners, HashMap.remove(runId)),
          );
        }),
      idleTimeToLive: Duration.infinity,
    });

    const activeRuns = yield* FiberMap.make<Chat.RunId>();
    const activeChats = yield* Ref.make(HashMap.empty<Chat.ChatId, Chat.RunId>());

    return {
      watch: (chatId) =>
        Stream.unwrap(
          RcMap.get(watchChannels, chatId).pipe(
            Effect.map((pubsub) => Stream.fromPubSub(pubsub)),
          ),
        ),

      subscribe: (runId, userId) =>
        Stream.unwrap(
          Effect.gen(function*() {
            const run = yield* Ref.get(runOwners).pipe(
              Effect.flatMap((map) =>
                Option.match(HashMap.get(map, runId), {
                  onNone: () => Effect.fail(new Chat.ChatRunNotFoundError({ runId })),
                  onSome: Effect.succeed,
                })
              ),
            );
            if (run.userId !== userId) {
              return yield* new Chat.ChatRunNotFoundError({ runId });
            }
            const pubsub = yield* RcMap.get(eventChannels, runId);
            return Stream.fromPubSubTake(pubsub);
          }),
        ),

      startGeneration: Effect.fnUntraced(function*(args) {
        const runId = Chat.RunId.makeUnsafe(crypto.randomUUID());
        const userMessage: typeof Chat.Message.Type = { role: "user", content: args.message };
        const baseMessages = [...args.chat.messages, userMessage] as const;
        return yield* Effect.uninterruptible(
          Effect.gen(function*() {
            const reserved = yield* Ref.modify(activeChats, (map) => {
              if (HashMap.has(map, args.chatId)) return [false, map] as const;
              return [
                true,
                HashMap.set(map, args.chatId, runId),
              ] as const;
            });
            if (!reserved) {
              return yield* new Chat.GenerationInProgressError({
                chatId: args.chatId,
              });
            }

            yield* Ref.update(
              runOwners,
              HashMap.set(runId, { chatId: args.chatId, userId: args.chat.userId }),
            );

            const started = yield* chatRepo.startRun({
              chatId: args.chat.id,
              userId: args.chat.userId,
              runId,
              messages: baseMessages,
            });
            if (!started) {
              return yield* new Chat.GenerationInProgressError({ chatId: args.chatId });
            }

            yield* Effect.scoped(
              RcMap.get(watchChannels, args.chatId).pipe(
                Effect.flatMap((watch) => PubSub.publish(watch, { _tag: "RunChanged", runId })),
                Effect.asVoid,
              ),
            );

            const ready = yield* Deferred.make<void, Chat.ChatRunNotFoundError>();

            yield* FiberMap.run(
              activeRuns,
              runId,
              Effect.scoped(
                Effect.gen(function*() {
                  const mailboxExit = yield* Effect.exit(RcMap.get(eventChannels, runId));
                  if (Exit.isFailure(mailboxExit)) {
                    yield* Deferred.done(ready, mailboxExit);
                    return yield* Effect.failCause(mailboxExit.cause);
                  }

                  const mailbox = mailboxExit.value;
                  yield* Deferred.succeed(ready, void 0);

                  yield* Effect.gen(function*() {
                    const aiMessages = yield* processor.run(args.chat, args.message);
                    yield* chatRepo.finishRun({
                      chatId: args.chat.id,
                      userId: args.chat.userId,
                      runId,
                      messages: [...baseMessages, ...aiMessages],
                    });
                  }).pipe(
                    aiModels.use(args.chat.model),
                    Effect.onExit((exit) =>
                      Exit.isSuccess(exit)
                        ? PubSub.publish(mailbox, Exit.void).pipe(Effect.asVoid)
                        : PubSub.publish(mailbox, Exit.failCause(exit.cause)).pipe(Effect.asVoid)
                    ),
                    Effect.ensuring(
                      Effect.gen(function*() {
                        const watch = yield* RcMap.get(watchChannels, args.chatId);
                        yield* chatRepo.clearActiveRun({
                          chatId: args.chat.id,
                          userId: args.chat.userId,
                          runId,
                        });
                        yield* PubSub.publish(watch, { _tag: "RunChanged", runId: null });
                        yield* RcMap.invalidate(eventChannels, runId);
                      }),
                    ),
                    Effect.provide(ChatProcessor.layer),
                    Effect.provide(ChatToolkitLive),
                    Effect.provideService(ChatMailbox, mailbox),
                    Effect.asVoid,
                  );
                }).pipe(
                  Effect.ensuring(Ref.update(activeChats, HashMap.remove(args.chatId))),
                ),
              ),
              { startImmediately: true },
            );

            yield* Deferred.await(ready).pipe(Effect.orDie);
            return { runId };
          }).pipe(
            Effect.catchCause((cause) =>
              Ref.update(activeChats, HashMap.remove(args.chatId)).pipe(
                Effect.andThen(Ref.update(runOwners, HashMap.remove(runId))),
                Effect.andThen(chatRepo.clearActiveRun({
                  chatId: args.chat.id,
                  userId: args.chat.userId,
                  runId,
                })),
                Effect.andThen(
                  Effect.scoped(
                    RcMap.get(watchChannels, args.chatId).pipe(
                      Effect.flatMap((watch) =>
                        PubSub.publish(watch, { _tag: "RunChanged", runId: null })
                      ),
                      Effect.asVoid,
                    ),
                  ),
                ),
                Effect.andThen(RcMap.invalidate(eventChannels, runId)),
                Effect.andThen(Effect.failCause(cause)),
              )
            ),
          ),
        );
      }),

      interrupt: Effect.fnUntraced(function*(chatId) {
        const activeRun = yield* Ref.get(activeChats).pipe(
          Effect.map((map) => HashMap.get(map, chatId)),
        );
        if (Option.isSome(activeRun)) {
          yield* FiberMap.remove(activeRuns, activeRun.value);
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
