import { ChatId, RunId } from "@app/domain/api/chat-rpc";
import type { ChatEvent } from "@app/domain/api/chat-rpc";
import { addEqualityTesters, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry";
import { ChatApi } from "./chat-api.js";
import {
  clearMessagesAtom,
  generatingAtom,
  messagesAtom,
  runtime,
  sendMessageAtom,
} from "./chat-atoms.js";
import type { UIMessage } from "./chat-types.js";

addEqualityTesters();

const TEST_CHAT_ID = ChatId.makeUnsafe("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
const TEST_RUN_ID = RunId.makeUnsafe("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12");

const makeApiLayer = (options: {
  events?: readonly ChatEvent[];
  streamFactory?: (runId: RunId) => Stream.Stream<ChatEvent, unknown>;
}) => {
  const calls = {
    chatAsk: [] as Array<{ chatId: ChatId; message: string; }>,
    chatEvents: [] as Array<RunId>,
  };

  const layer = Layer.mock(ChatApi)({
    chatAsk: (args) => {
      calls.chatAsk.push(args);
      return Effect.succeed({ runId: TEST_RUN_ID });
    },
    chatEvents: (runId) => {
      calls.chatEvents.push(runId);
      if (options.streamFactory) {
        return options.streamFactory(runId) as never;
      }
      return Stream.fromIterable(options.events ?? []) as never;
    },
    chatWatch: () => Stream.never as never,
    chatList: () => Effect.succeed({ items: [], hasMore: false }),
    chatGet: (chatId) =>
      Effect.succeed({
        id: chatId,
        title: "Test Chat",
        model: "haiku-4.5" as const,
        createdAt: new Date() as never,
        updatedAt: new Date() as never,
        messages: [],
        activeRunId: null,
      }),
    chatCreate: () => Effect.die("not mocked"),
    chatDelete: () => Effect.die("not mocked"),
    chatInterrupt: () => Effect.void,
  });
  return { layer, calls };
};

const makeRegistry = (options?: {
  initialMessages?: ReadonlyArray<UIMessage>;
  events?: readonly ChatEvent[];
  streamFactory?: (runId: RunId) => Stream.Stream<ChatEvent, unknown>;
}) => {
  const apiOptions: Parameters<typeof makeApiLayer>[0] = {};
  if (options?.events) apiOptions.events = options.events;
  if (options?.streamFactory) apiOptions.streamFactory = options.streamFactory;
  const { layer, calls } = makeApiLayer(apiOptions);
  const registry = AtomRegistry.make({
    initialValues: [
      Atom.initialValue(runtime.layer, layer),
      Atom.initialValue(messagesAtom, options?.initialMessages ?? []),
    ],
  });
  registry.mount(messagesAtom);
  registry.mount(generatingAtom);
  return { registry, calls };
};

const flush = async () => {
  for (let i = 0; i < 10; i++) {
    await Effect.runPromise(Effect.yieldNow);
    await new Promise<void>((r) => setTimeout(r, 0));
  }
};

describe("sendMessageAtom", () => {
  it("creates optimistic messages", async () => {
    const { registry } = makeRegistry({
      events: [],
    });
    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "hello" });
    await flush();

    const msgs = registry.get(messagesAtom);
    expect(msgs.length).toBeGreaterThanOrEqual(2);
    expect(msgs[0]!.role).toBe("user");
    expect(msgs[0]!.content).toBe("hello");
    expect(msgs[1]!.role).toBe("assistant");
  });

  it("sets generatingAtom to true during streaming", async () => {
    const { registry } = makeRegistry({
      streamFactory: () => Stream.never as Stream.Stream<ChatEvent>,
    });
    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "hello" });
    await flush();

    expect(registry.get(generatingAtom)).toBe(true);

    registry.set(sendMessageAtom, Atom.Interrupt);
    await flush();
  });

  it("accumulates stream events on assistant message", async () => {
    const { registry } = makeRegistry({
      events: [
        { _tag: "Chunk", delta: "Hello" },
        { _tag: "Chunk", delta: " world" },
      ],
    });

    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "hi" });
    await flush();

    const msgs = registry.get(messagesAtom);
    const assistant = msgs.find((m) => m.role === "assistant");
    expect(assistant).toBeDefined();
    expect(assistant!.content).toBe("Hello world");
    expect(assistant!.contentBlocks).toHaveLength(1);
    expect(assistant!.contentBlocks[0]!._tag).toBe("text");
  });

  it("sets generatingAtom to false on completion", async () => {
    const { registry } = makeRegistry({
      events: [
        { _tag: "Chunk", delta: "hi" },
      ],
    });

    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "test" });
    await flush();

    expect(registry.get(generatingAtom)).toBe(false);
  });

  it("marks interrupted on Atom.Interrupt", async () => {
    const { registry } = makeRegistry({
      streamFactory: () => Stream.never as Stream.Stream<ChatEvent>,
    });

    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "test" });
    await flush();

    registry.set(sendMessageAtom, Atom.Interrupt);
    await flush();

    expect(registry.get(generatingAtom)).toBe(false);
    const msgs = registry.get(messagesAtom);
    const assistant = msgs.find((m) => m.role === "assistant");
    expect(assistant).toBeDefined();
    expect(assistant!.content).toBe("(interrupted)");
  });

  it("stores error on stream failure", async () => {
    const { registry } = makeRegistry({
      streamFactory: () => Stream.fail("stream-error") as Stream.Stream<ChatEvent, unknown>,
    });

    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "test" });
    await flush();

    expect(registry.get(generatingAtom)).toBe(false);
    const msgs = registry.get(messagesAtom);
    const assistant = msgs.find((m) => m.role === "assistant");
    expect(assistant).toBeDefined();
    expect(assistant!.error).not.toBeNull();
  });

  it("calls chatAsk with correct payload", async () => {
    const { registry, calls } = makeRegistry({
      events: [],
    });

    registry.mount(sendMessageAtom);
    await flush();
    registry.set(sendMessageAtom, { chatId: TEST_CHAT_ID, message: "hello" });
    await flush();

    expect(calls.chatAsk).toHaveLength(1);
    expect(calls.chatAsk[0]!.chatId).toBe(TEST_CHAT_ID);
    expect(calls.chatAsk[0]!.message).toBe("hello");
    expect(calls.chatEvents).toEqual([TEST_RUN_ID]);
  });
});

describe("clearMessagesAtom", () => {
  it("resets messages to empty", () => {
    const initialMessages: UIMessage[] = [
      {
        id: "1",
        role: "user",
        content: "hi",
        contentBlocks: [],
        error: null,
      },
    ];
    const { registry } = makeRegistry({ initialMessages });
    expect(registry.get(messagesAtom)).toHaveLength(1);

    registry.set(clearMessagesAtom, null);
    expect(registry.get(messagesAtom)).toHaveLength(0);
  });
});
