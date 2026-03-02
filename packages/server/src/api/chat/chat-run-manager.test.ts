import { AiModels } from "@/lib/ai-models.js"
import { ChatRepo } from "@/db/chat-repo.js"
import type { ChatModel } from "@/db/chat-model.js"
import * as Chat from "@app/domain/api/chat-rpc"
import { describe, expect, it } from "@effect/vitest"
import { withLanguageModel } from "@test/utils/with-language-model.js"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import * as AiError from "effect/unstable/ai/AiError"
import { ChatRunManager } from "./chat-run-manager.js"

const mockChat = (overrides?: Partial<typeof ChatModel.Type>): typeof ChatModel.Type => ({
  id: Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001"),
  userId: "user-1",
  title: "Test Chat",
  model: "haiku-4.5",
  messages: [],
  createdAt: DateTime.nowUnsafe(),
  updatedAt: DateTime.nowUnsafe(),
  ...overrides,
})

const MockAiModels = Layer.mock(AiModels)({
  use: (_model) => (effect) =>
    withLanguageModel(effect, {
      streamText: [{ type: "text-delta" as const, id: "t1", delta: "Hello" }],
    }),
})

const FailingAiModels = Layer.mock(AiModels)({
  use: (_model) => (_effect) =>
    Effect.fail(
      new AiError.AiError({
        module: "test",
        method: "streamText",
        reason: new AiError.RateLimitError({}),
      }),
    ) as any,
})

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
  })

const makeTestLayer = (aiLayer: Layer.Layer<AiModels> = MockAiModels, repoLayer?: Layer.Layer<ChatRepo>) => {
  const repo = repoLayer ?? makeMockChatRepo()
  return Layer.effect(ChatRunManager, ChatRunManager.make).pipe(
    Layer.provide(aiLayer),
    Layer.provide(repo),
  )
}

describe("ChatRunManager", () => {
  it.live("subscribe returns a stream that receives events", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const eventsFiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.take(3),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)

      yield* Effect.sleep("200 millis")

      const events = yield* Fiber.join(eventsFiber)
      expect(events).toHaveLength(3)
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 })

  it.live("startGeneration publishes GenerationStarted with reconciliationId", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-42")

      const eventsFiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.take(1),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)

      yield* Effect.sleep("200 millis")

      const events = yield* Fiber.join(eventsFiber)
      expect(events[0]!._tag).toBe("GenerationStarted")
      expect((events[0] as any).reconciliationId).toBe(reconciliationId)
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 })

  it.live("startGeneration fails with GenerationInProgressError when already running", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () => Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
            Stream.tap(() => Effect.sleep("2 seconds")),
          ),
        }),
    })
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId1 = Chat.ReconciliationId.makeUnsafe("recon-1")
      const reconciliationId2 = Chat.ReconciliationId.makeUnsafe("recon-2")

      yield* mgr.startGeneration(chatId, mockChat(), "First", reconciliationId1)
      yield* Effect.sleep("50 millis")

      const exit = yield* mgr.startGeneration(chatId, mockChat(), "Second", reconciliationId2).pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(makeTestLayer(slowAi)))
  }, { timeout: 5000 })

  it.live("generation publishes Done on completion", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const eventsFiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((e) => e._tag === "Done"),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)

      yield* Effect.sleep("500 millis")

      const events = yield* Fiber.join(eventsFiber)
      const lastEvent = events[events.length - 1]
      expect(lastEvent!._tag).toBe("Done")
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 })

  it.live("generation publishes Error then Done on AiError", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const eventsFiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((e) => e._tag === "Done"),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)

      yield* Effect.sleep("500 millis")

      const events = yield* Fiber.join(eventsFiber)
      expect(events.some((e) => e._tag === "Error")).toBe(true)
      expect(events[events.length - 1]!._tag).toBe("Done")
    }).pipe(Effect.provide(makeTestLayer(FailingAiModels))), { timeout: 5000 })

  it.live("interrupt publishes Interrupted", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () => Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
            Stream.tap(() => Effect.sleep("10 seconds")),
          ),
        }),
    })
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const eventsFiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((e) => e._tag === "Interrupted"),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)
      yield* Effect.sleep("100 millis")

      yield* mgr.interrupt(chatId)

      yield* Effect.sleep("200 millis")

      const events = yield* Fiber.join(eventsFiber)
      expect(events.some((e) => e._tag === "Interrupted")).toBe(true)
    }).pipe(Effect.provide(makeTestLayer(slowAi)))
  }, { timeout: 5000 })

  it.effect("interrupt is no-op when no generation running", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      yield* mgr.interrupt(chatId)
    }).pipe(Effect.provide(makeTestLayer())))

  it.live("late subscriber receives replayed events including Done", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)
      yield* Effect.sleep("500 millis")

      const stream = yield* mgr.subscribe(chatId)
      const events = yield* stream.pipe(
        Stream.takeUntil((e) => e._tag === "Done"),
        Stream.runCollect,
      )
      expect(events.some((e) => e._tag === "GenerationStarted")).toBe(true)
      expect(events[events.length - 1]!._tag).toBe("Done")
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 })

  it.live("interrupt observed by all subscribers", () => {
    const slowAi = Layer.mock(AiModels)({
      use: (_model) => (effect) =>
        withLanguageModel(effect, {
          streamText: () => Stream.make({ type: "text-delta" as const, id: "t1", delta: "slow" }).pipe(
            Stream.tap(() => Effect.sleep("10 seconds")),
          ),
        }),
    })
    return Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const sub1Fiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((e) => e._tag === "Interrupted"),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )
      const sub2Fiber = yield* mgr.subscribe(chatId).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            Stream.takeUntil((e) => e._tag === "Interrupted"),
            Stream.runCollect,
          )
        ),
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)
      yield* Effect.sleep("100 millis")

      yield* mgr.interrupt(chatId)
      yield* Effect.sleep("200 millis")

      const events1 = yield* Fiber.join(sub1Fiber)
      const events2 = yield* Fiber.join(sub2Fiber)
      expect(events1.some((e) => e._tag === "Interrupted")).toBe(true)
      expect(events2.some((e) => e._tag === "Interrupted")).toBe(true)
    }).pipe(Effect.provide(makeTestLayer(slowAi)))
  }, { timeout: 5000 })

  it.live("concurrent starts for same chatId: one succeeds, one fails", () =>
    Effect.gen(function*() {
      const mgr = yield* ChatRunManager
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const recon1 = Chat.ReconciliationId.makeUnsafe("recon-1")
      const recon2 = Chat.ReconciliationId.makeUnsafe("recon-2")

      const [exit1, exit2] = yield* Effect.all([
        mgr.startGeneration(chatId, mockChat(), "First", recon1).pipe(Effect.exit),
        mgr.startGeneration(chatId, mockChat(), "Second", recon2).pipe(Effect.exit),
      ])

      const oneSucceeded = (exit1._tag === "Success" && exit2._tag === "Failure")
        || (exit1._tag === "Failure" && exit2._tag === "Success")
      expect(oneSucceeded).toBe(true)
    }).pipe(Effect.provide(makeTestLayer())), { timeout: 5000 })

  it.live("generation persists messages on completion", () =>
    Effect.gen(function*() {
      const updatedRef = yield* Ref.make<ReadonlyArray<typeof Chat.Message.Type>>([])
      const repo = makeMockChatRepo(updatedRef)

      yield* Effect.gen(function*() {
        const mgr = yield* ChatRunManager
        const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
        const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

        yield* mgr.startGeneration(chatId, mockChat(), "Hello", reconciliationId)
        yield* Effect.sleep("500 millis")

        const messages = yield* Ref.get(updatedRef)
        expect(messages.length).toBeGreaterThan(0)
        expect(messages[0]!.role).toBe("user")
        expect(messages[0]!.content).toBe("Hello")
      }).pipe(Effect.provide(makeTestLayer(MockAiModels, repo)))
    }), { timeout: 5000 })
})
