import { AuthMiddlewareLive } from "@/api/auth-middleware-live.js"
import { AiModels } from "@/lib/ai-models.js"
import { ChatRepo } from "@/db/chat-repo.js"
import type { ChatModel } from "@/db/chat-model.js"
import * as Chat from "@app/domain/api/chat-rpc"
import { describe, expect, it } from "@effect/vitest"
import { withLanguageModel } from "@test/utils/with-language-model.js"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Fiber from "effect/Fiber"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import { RpcTest } from "effect/unstable/rpc"
import { ChatRunManager } from "./chat-run-manager.js"
import { ChatRpcHandler } from "./chat-rpc-live.js"

const mockChat = (overrides?: Partial<typeof ChatModel.Type>): typeof ChatModel.Type => ({
  id: Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001"),
  userId: "00000000-0000-4000-8000-000000000001",
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
      streamText: [{ type: "text-delta" as const, id: "t1", delta: "Hello from AI" }],
    }),
})

const MockChatRepo = Layer.mock(ChatRepo)({
  create: ({ userId, title, model }) =>
    Effect.succeed(mockChat({ userId, title, model })),
  findById: (chatId, _userId) =>
    Effect.succeed(mockChat({ id: chatId })),
  listByUser: () =>
    Effect.succeed({ items: [mockChat()], hasMore: false }),
  delete: () => Effect.void,
  updateMessages: () => Effect.void,
})

const NotFoundChatRepo = Layer.mock(ChatRepo)({
  create: () => Effect.die("not called"),
  findById: (chatId) => Effect.fail(new Chat.ChatNotFoundError({ id: chatId })),
  listByUser: () => Effect.die("not called"),
  delete: (chatId) => Effect.fail(new Chat.ChatNotFoundError({ id: chatId })),
  updateMessages: () => Effect.die("not called"),
})

const makeRunManagerLayer = (repoLayer: Layer.Layer<ChatRepo>) =>
  Layer.effect(ChatRunManager, ChatRunManager.make).pipe(
    Layer.provide(MockAiModels),
    Layer.provide(repoLayer),
  )

const TestLayer = Layer.mergeAll(
  ChatRpcHandler.pipe(
    Layer.provide(makeRunManagerLayer(MockChatRepo)),
    Layer.provide(MockChatRepo),
  ),
  AuthMiddlewareLive,
)

const NotFoundLayer = Layer.mergeAll(
  ChatRpcHandler.pipe(
    Layer.provide(makeRunManagerLayer(NotFoundChatRepo)),
    Layer.provide(NotFoundChatRepo),
  ),
  AuthMiddlewareLive,
)

describe("ChatRpc", () => {
  it.effect("chat_create returns a Chat", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const result = yield* client.chat_create({ title: "New Chat", model: "haiku-4.5" })
      expect(result.title).toBe("New Chat")
      expect(result.model).toBe("haiku-4.5")
    }).pipe(Effect.provide(TestLayer)))

  it.effect("chat_list returns items and hasMore", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const result = yield* client.chat_list({ cursor: null })
      expect(result.items).toHaveLength(1)
      expect(result.hasMore).toBe(false)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("chat_get returns Chat.WithMessages", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const result = yield* client.chat_get({ chatId })
      expect(result.id).toBe(chatId)
    }).pipe(Effect.provide(TestLayer)))

  it.effect("chat_delete succeeds", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      yield* client.chat_delete({ chatId })
    }).pipe(Effect.provide(TestLayer)))

  it.effect("chat_delete fails with ChatNotFoundError when not found", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000099")
      const exit = yield* client.chat_delete({ chatId }).pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(NotFoundLayer)))

  it.effect("chat_ask fails with ChatNotFoundError for invalid chatId", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000099")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")
      const exit = yield* client.chat_ask({ chatId, message: "Hello", reconciliationId }).pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
    }).pipe(Effect.provide(NotFoundLayer)))

  it.live("chat_ask saves user message before starting generation", () => {
    const updatedRef = Ref.makeUnsafe<ReadonlyArray<typeof Chat.Message.Type>>([])
    const TrackingRepo = Layer.mock(ChatRepo)({
      create: ({ userId, title, model }) =>
        Effect.succeed(mockChat({ userId, title, model })),
      findById: (chatId, _userId) =>
        Effect.succeed(mockChat({ id: chatId })),
      listByUser: () =>
        Effect.succeed({ items: [mockChat()], hasMore: false }),
      delete: () => Effect.void,
      updateMessages: ({ messages }) => Ref.set(updatedRef, messages),
    })
    const TrackingLayer = Layer.mergeAll(
      ChatRpcHandler.pipe(
        Layer.provide(makeRunManagerLayer(TrackingRepo)),
        Layer.provide(TrackingRepo),
      ),
      AuthMiddlewareLive,
    )
    return Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      yield* client.chat_ask({ chatId, message: "Hello world", reconciliationId })
      yield* Effect.sleep("200 millis")

      const messages = yield* Ref.get(updatedRef)
      expect(messages.length).toBeGreaterThanOrEqual(1)
      expect(messages[0]!.role).toBe("user")
      expect(messages[0]!.content).toBe("Hello world")
    }).pipe(Effect.provide(TrackingLayer))
  }, { timeout: 5000 })

  it.live("chat_ask starts generation and events flow through chat_events", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      const reconciliationId = Chat.ReconciliationId.makeUnsafe("recon-1")

      const eventsFiber = yield* client.chat_events({ chatId }).pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      )

      yield* Effect.sleep("50 millis")

      yield* client.chat_ask({ chatId, message: "Hello", reconciliationId })

      yield* Effect.sleep("100 millis")

      const events = yield* Fiber.join(eventsFiber)
      expect(events.some((e) => e._tag === "GenerationStarted")).toBe(true)
      expect(events.some((e) => e._tag === "Chunk")).toBe(true)
      expect(events.some((e) => e._tag === "Done")).toBe(true)
    }).pipe(Effect.provide(TestLayer)), { timeout: 5000 })

  it.effect("chat_interrupt is no-op when no generation running", () =>
    Effect.gen(function*() {
      const client = yield* RpcTest.makeClient(Chat.ChatRpc)
      const chatId = Chat.ChatId.makeUnsafe("00000000-0000-4000-8000-000000000001")
      yield* client.chat_interrupt({ chatId })
    }).pipe(Effect.provide(TestLayer)))
})
