import type { ChatModel } from "@/db/chat-model.js";
import { ChatRepo } from "@/db/chat-repo.js";
import { AiModels } from "@/lib/ai-models.js";
import * as Chat from "@app/domain/api/chat-rpc";
import { describe, expect, it } from "@effect/vitest";
import { withLanguageModel } from "@test/utils/with-language-model.js";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as AiError from "effect/unstable/ai/AiError";
import { ChatProcessor } from "./chat-processor.js";
import { ChatRunManager } from "./chat-run-manager.js";

const mockChat = (overrides?: Partial<typeof ChatModel.Type>): typeof ChatModel.Type => ({
  id: Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001"),
  userId: "user-1",
  title: "Test Chat",
  model: "haiku-4.5",
  messages: [],
  activeRunId: null,
  createdAt: DateTime.nowUnsafe(),
  updatedAt: DateTime.nowUnsafe(),
  ...overrides,
});

const MockAiModels = Layer.mock(AiModels)({
  use: (_model) => (effect) =>
    withLanguageModel(effect, {
      streamText: [{ type: "text-delta" as const, id: "t1", delta: "Hello" }],
    }),
});

const FailingAiModels = Layer.mock(AiModels)({
  use: (_model) => (_effect) =>
    Effect.fail(
      new AiError.AiError({
        module: "test",
        method: "streamText",
        reason: new AiError.RateLimitError({}),
      }),
    ) as any,
});

const makeMockChatRepo = (updatedMessagesRef?: Ref.Ref<ReadonlyArray<typeof Chat.Message.Type>>) =>
  Layer.mock(ChatRepo)({
    create: () => Effect.succeed(mockChat()),
    findById: (chatId) => Effect.succeed(mockChat({ id: chatId })),
    listByUser: () => Effect.succeed({ items: [], hasMore: false }),
    delete: () => Effect.void,
    updateMessages: ({ messages }) =>
      updatedMessagesRef
        ? Ref.set(updatedMessagesRef, messages)
        : Effect.void,
    startRun: () => Effect.succeed(true),
    finishRun: () => Effect.void,
    clearActiveRun: () => Effect.void,
  });

const makeTestLayer = (
  aiLayer: Layer.Layer<AiModels> = MockAiModels,
  repoLayer?: Layer.Layer<ChatRepo>,
) => {
  const repo = repoLayer ?? makeMockChatRepo();
  return Layer.effect(ChatRunManager, ChatRunManager.make).pipe(
    Layer.provide(aiLayer),
    Layer.provide(repo),
    Layer.provide(ChatProcessor.layer),
  );
};

describe("ChatRunManager", () => {
  it.live(
    "startGeneration streams events while the run is active",
    () => {
      const slowAi = Layer.mock(AiModels)({
        use: (_model) => (effect) =>
          withLanguageModel(effect, {
            streamText: () =>
              Stream.make({ type: "text-delta" as const, id: "t1", delta: "Hello" }).pipe(
                Stream.tap(() => Effect.sleep("100 millis")),
              ),
          }),
      });
      return Effect.gen(function*() {
        const mgr = yield* ChatRunManager;
        const chat = mockChat();

        const { runId } = yield* mgr.startGeneration({
          chatId: chat.id,
          chat,
          message: "Hello",
        });

        const events = yield* mgr.subscribe(runId, chat.userId).pipe(Stream.runCollect);
        expect(events.some((event) => event._tag === "Chunk")).toBe(true);
      }).pipe(Effect.provide(makeTestLayer(slowAi)));
    },
    { timeout: 5000 },
  );

  it.live("startGeneration fails with GenerationInProgressError when already running", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () =>
            Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
              Stream.tap(() => Effect.sleep("2 seconds")),
            ),
        }),
    });
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "First",
      });
      yield* Effect.sleep("50 millis");

      const exit = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Second",
      }).pipe(Effect.exit);

      expect(exit._tag).toBe("Failure");
    }).pipe(Effect.provide(makeTestLayer(slowAi)));
  }, { timeout: 5000 });

  it.effect("subscribe fails with ChatRunNotFoundError when run is missing", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const runId = Chat.RunId.makeUnsafe("00000000-0000-4000-8000-000000000099");

      const exit = yield* mgr.subscribe(runId, "user-1").pipe(Stream.runDrain, Effect.exit);
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(
          exit.cause.reasons.some((reason) =>
            reason._tag === "Fail" && reason.error._tag === "ChatRunNotFoundError"
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(makeTestLayer())));

  it.live("failed generation fails stream with AiError", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      const { runId } = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });

      const exit = yield* mgr.subscribe(runId, chat.userId).pipe(Stream.runDrain, Effect.exit);
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(
          exit.cause.reasons.some((reason) =>
            reason._tag === "Fail" && reason.error._tag === "AiError"
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(makeTestLayer(FailingAiModels))), { timeout: 5000 });

  it.live("interrupt fails active stream with interrupt-only cause", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () =>
            Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
              Stream.tap(() => Effect.sleep("10 seconds")),
            ),
        }),
    });
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      const { runId } = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });
      const exitFiber = yield* mgr.subscribe(runId, chat.userId).pipe(
        Stream.runDrain,
        Effect.exit,
        Effect.forkChild,
      );

      yield* Effect.sleep("100 millis");
      yield* mgr.interrupt(chat.id);

      const exit = yield* Fiber.join(exitFiber);
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      }
    }).pipe(Effect.provide(makeTestLayer(slowAi)));
  }, { timeout: 5000 });

  it.live("completed run invalidates the run stream entry", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      const { runId } = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });
      yield* Effect.sleep("200 millis");

      const exit = yield* mgr.subscribe(runId, chat.userId).pipe(Stream.runDrain, Effect.exit);
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(
          exit.cause.reasons.some((reason) =>
            reason._tag === "Fail" && reason.error._tag === "ChatRunNotFoundError"
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 });

  it.live("interrupted run invalidates the run stream entry", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () =>
            Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
              Stream.tap(() => Effect.sleep("10 seconds")),
            ),
        }),
    });
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      const { runId } = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });
      yield* Effect.sleep("100 millis");

      yield* mgr.interrupt(chat.id);
      yield* Effect.sleep("200 millis");

      const exit = yield* mgr.subscribe(runId, chat.userId).pipe(Stream.runDrain, Effect.exit);
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(
          exit.cause.reasons.some((reason) =>
            reason._tag === "Fail" && reason.error._tag === "ChatRunNotFoundError"
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(makeTestLayer(slowAi)));
  }, { timeout: 5000 });

  it.live("completed generation releases the active chat lock", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager;
      const chat = mockChat();

      yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });
      yield* Effect.sleep("200 millis");

      const second = yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Again",
      });

      expect(second.runId).toBeDefined();
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 });

  it.live("closing manager scope interrupts an active generation", () => {
    let finalized = false;
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () =>
            Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
              Stream.tap(() => Effect.never),
              Stream.ensuring(
                Effect.sync(() => {
                  finalized = true;
                }),
              ),
            ),
        }),
    });
    return Effect.gen(function*() {
      const scope = yield* Scope.make();
      const mgr = yield* ChatRunManager.make.pipe(
        Effect.provide(slowAi),
        Effect.provide(makeMockChatRepo()),
        Effect.provide(ChatProcessor.layer),
        Scope.provide(scope),
      );
      const chat = mockChat();

      yield* mgr.startGeneration({
        chatId: chat.id,
        chat,
        message: "Hello",
      });
      yield* Effect.sleep("100 millis");

      yield* Scope.close(scope, Exit.void);
      yield* Effect.sleep("100 millis");

      expect(finalized).toBe(true);
    });
  }, { timeout: 5000 });
});
