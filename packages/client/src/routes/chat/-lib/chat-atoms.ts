import { ModelFamily } from "@app/domain/ai-models";
import { ChatId, RunId } from "@app/domain/api/chat-rpc";
import type { ChatEvent, Message } from "@app/domain/api/chat-rpc";
import * as BrowserKeyValueStore from "@effect/platform-browser/BrowserKeyValueStore";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Pull from "effect/Pull";
import type { Json } from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry";
import { accumulateEvent, extractText } from "./chat-accumulator.js";
import { ChatApi } from "./chat-api.js";
import type { ContentBlock, StreamState, ToolStatus, UIMessage } from "./chat-types.js";

export const runtime = Atom.runtime(ChatApi.layer);

export const messagesAtom = Atom.make<readonly UIMessage[]>([]);
export const inputAtom = Atom.make("");
export const generatingAtom = Atom.make(false);
export const attachedRunIdAtom = Atom.make<RunId | null>(null);

export const selectedModelAtom = Atom.kvs({
  runtime: Atom.runtime(BrowserKeyValueStore.layerLocalStorage),
  key: "@app/chat/selected-model",
  schema: ModelFamily,
  defaultValue: () => "sonnet-4.6" as const,
});

export const clearMessagesAtom = Atom.writable(
  () => null,
  (ctx) => {
    ctx.set(messagesAtom, []);
  },
);

const convertPersistedMessages = (messages: ReadonlyArray<Message>): readonly UIMessage[] => {
  const result: UIMessage[] = [];
  let i = 0;

  while (i < messages.length) {
    const msg = messages[i]!;

    if (msg.role === "user") {
      const content = typeof msg.content === "string"
        ? msg.content
        : msg.content
          .filter((p): p is typeof p & { type: "text"; } => p.type === "text")
          .map((p) => p.text)
          .join("");
      result.push({
        id: crypto.randomUUID(),
        role: "user",
        content,
        contentBlocks: [],
        error: null,
      });
      i++;
      continue;
    }

    const contentBlocks: ContentBlock[] = [];

    while (i < messages.length && messages[i]!.role !== "user") {
      const current = messages[i]!;

      if (current.role === "assistant") {
        const toolResults = new Map<
          string,
          { readonly result: Json; readonly isFailure: boolean; }
        >();
        const nextMsg = messages[i + 1];
        if (nextMsg && nextMsg.role === "tool" && typeof nextMsg.content !== "string") {
          for (const part of nextMsg.content) {
            if (part.type === "tool-result") {
              toolResults.set(part.id, { result: part.result, isFailure: part.isFailure });
            }
          }
        }

        if (typeof current.content === "string") {
          if (current.content) {
            contentBlocks.push({ _tag: "text", content: current.content });
          }
        } else {
          for (const part of current.content) {
            if (part.type === "text") {
              const last = contentBlocks[contentBlocks.length - 1];
              if (last && last._tag === "text") {
                contentBlocks[contentBlocks.length - 1] = {
                  _tag: "text",
                  content: last.content + part.text,
                };
              } else if (part.text) {
                contentBlocks.push({ _tag: "text", content: part.text });
              }
            } else if (part.type === "tool-call") {
              const tr = toolResults.get(part.id);
              const tool: ToolStatus = {
                id: part.id,
                toolName: part.name,
                status: tr ? (tr.isFailure ? "failure" : "success") : "start",
                input: typeof part.params === "string"
                  ? part.params
                  : JSON.stringify(part.params, null, 2),
                output: tr
                  ? (typeof tr.result === "string" ? tr.result : JSON.stringify(tr.result, null, 2))
                  : null,
              };
              const last = contentBlocks[contentBlocks.length - 1];
              if (last && last._tag === "tool_group") {
                contentBlocks[contentBlocks.length - 1] = {
                  _tag: "tool_group",
                  tools: [...last.tools, tool],
                };
              } else {
                contentBlocks.push({ _tag: "tool_group", tools: [tool] });
              }
            }
          }
        }
      }

      i++;
    }

    result.push({
      id: crypto.randomUUID(),
      role: "assistant",
      content: extractText(contentBlocks),
      contentBlocks,
      error: null,
    });
  }

  return result;
};

const applyChatSnapshot = (
  registry: AtomRegistry.AtomRegistry,
  chat: { readonly messages: ReadonlyArray<Message>; readonly activeRunId: RunId | null; },
) => {
  registry.set(messagesAtom, convertPersistedMessages(chat.messages));
  registry.set(generatingAtom, chat.activeRunId !== null);
};

const appendAssistantPlaceholder = (registry: AtomRegistry.AtomRegistry) => {
  const assistantMsgId = crypto.randomUUID();
  const currentMessages = registry.get(messagesAtom);
  registry.set(messagesAtom, [...currentMessages, {
    id: assistantMsgId,
    role: "assistant",
    content: "",
    contentBlocks: [],
    error: null,
  }]);
  return assistantMsgId;
};

const runStream = (
  chatId: ChatId,
  runId: RunId,
  assistantMsgId: string,
  reloadOnSuccess: boolean,
) =>
  Stream.unwrap(Effect.gen(function*() {
    const api = yield* ChatApi;
    const registry = yield* AtomRegistry.AtomRegistry;

    const onStreamDone = Effect.gen(function*() {
      registry.set(attachedRunIdAtom, null);
      if (reloadOnSuccess) {
        const chatExit = yield* Effect.exit(api.chatGet(chatId));
        if (chatExit._tag === "Success") {
          applyChatSnapshot(registry, chatExit.value);
        } else {
          registry.set(generatingAtom, false);
        }
      } else {
        registry.set(generatingAtom, false);
      }
    });

    yield* Effect.addFinalizer(Exit.match({
      onSuccess: () => onStreamDone,
      onFailure: (cause) => {
        if (Pull.isDoneCause(cause)) {
          return onStreamDone;
        }
        return Effect.sync(() => {
          registry.set(attachedRunIdAtom, null);
          registry.set(generatingAtom, false);
          const messages = registry.get(messagesAtom);
          if (Cause.hasInterruptsOnly(cause)) {
            registry.set(
              messagesAtom,
              messages.map((message) =>
                message.id === assistantMsgId
                  ? { ...message, content: message.content || "(interrupted)" }
                  : message
              ),
            );
            return;
          }
          registry.set(
            messagesAtom,
            messages.map((message) =>
              message.id === assistantMsgId ? { ...message, error: cause } : message
            ),
          );
        });
      },
    }));

    return api.chatEvents(runId).pipe(
      Stream.mapAccumEffect(
        (): StreamState => ({ contentBlocks: [] }),
        (state, event: ChatEvent) =>
          Effect.gen(function*() {
            const reg = yield* AtomRegistry.AtomRegistry;
            const nextState = accumulateEvent(state, event);
            const textContent = extractText(nextState.contentBlocks);
            const messages = reg.get(messagesAtom);
            reg.set(
              messagesAtom,
              messages.map((message) =>
                message.id === assistantMsgId
                  ? { ...message, content: textContent, contentBlocks: nextState.contentBlocks }
                  : message
              ),
            );
            return [nextState, [nextState]] as const;
          }),
      ),
      Stream.catchTag("RpcClientError", (error) => Stream.fromEffect(Effect.die(error))),
    );
  }));

export const attachRunAtom = runtime.fn(
  ({
    chatId,
    runId,
    assistantMsgId,
    reloadOnSuccess,
  }: {
    chatId: ChatId;
    runId: RunId;
    assistantMsgId: string;
    reloadOnSuccess: boolean;
  }) => runStream(chatId, runId, assistantMsgId, reloadOnSuccess),
);

export const watchChatAtom = runtime.fn(
  ({ chatId, activeRunId }: { chatId: ChatId; activeRunId: RunId | null; }) =>
    Effect.gen(function*() {
      const api = yield* ChatApi;
      const registry = yield* AtomRegistry.AtomRegistry;

      const attach = (runId: RunId) =>
        Effect.gen(function*() {
          if (registry.get(attachedRunIdAtom) === runId) {
            return;
          }

          const chat = yield* api.chatGet(chatId);
          applyChatSnapshot(registry, chat);
          if (chat.activeRunId !== runId) {
            return;
          }

          const assistantMsgId = appendAssistantPlaceholder(registry);
          registry.mount(attachRunAtom);
          registry.set(attachedRunIdAtom, runId);
          registry.set(attachRunAtom, { chatId, runId, assistantMsgId, reloadOnSuccess: true });
        });

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          registry.set(attachRunAtom, Atom.Interrupt);
          registry.set(attachedRunIdAtom, null);
          registry.set(generatingAtom, false);
        })
      );

      if (activeRunId !== null) {
        yield* attach(activeRunId);
      }

      yield* api.chatWatch(chatId).pipe(
        Stream.runForEach((event) => event.runId === null ? Effect.void : attach(event.runId)),
        Effect.forkScoped,
      );

      yield* Effect.never;
    }),
);

export const chatListAtom = runtime.atom(
  Effect.gen(function*() {
    const api = yield* ChatApi;
    return yield* api.chatList(null);
  }),
);

export const chatAtom = runtime.fn((chatId: ChatId) =>
  Effect.gen(function*() {
    const api = yield* ChatApi;
    const registry = yield* AtomRegistry.AtomRegistry;
    registry.set(sendMessageAtom, Atom.Interrupt);
    registry.set(attachRunAtom, Atom.Interrupt);
    registry.set(attachedRunIdAtom, null);
    const chat = yield* api.chatGet(chatId);
    applyChatSnapshot(registry, chat);
    return chat;
  })
);

export const sendMessageAtom = runtime.fn(
  ({ chatId, message }: { chatId: ChatId; message: string; }) =>
    Stream.unwrap(Effect.gen(function*() {
      const api = yield* ChatApi;
      const registry = yield* AtomRegistry.AtomRegistry;

      const assistantMsgId = crypto.randomUUID();

      const userMsg: UIMessage = {
        id: crypto.randomUUID(),
        role: "user",
        content: message,
        contentBlocks: [],
        error: null,
      };
      const assistantMsg: UIMessage = {
        id: assistantMsgId,
        role: "assistant",
        content: "",
        contentBlocks: [],
        error: null,
      };
      const currentMessages = registry.get(messagesAtom);
      registry.set(messagesAtom, [...currentMessages, userMsg, assistantMsg]);
      registry.set(generatingAtom, true);

      const { runId } = yield* api.chatAsk({ chatId, message });

      registry.set(attachedRunIdAtom, runId);
      return runStream(chatId, runId, assistantMsgId, true);
    })),
);

export const createChatAtom = runtime.fn(
  ({ title, model }: { title: string; model: typeof ModelFamily.Type; }) =>
    Effect.gen(function*() {
      const api = yield* ChatApi;
      return yield* api.chatCreate({ title, model });
    }),
);

export const deleteChatAtom = runtime.fn((chatId: ChatId) =>
  Effect.gen(function*() {
    const api = yield* ChatApi;
    yield* api.chatDelete(chatId);
  })
);

export const interruptAtom = runtime.fn((chatId: ChatId) =>
  Effect.gen(function*() {
    const api = yield* ChatApi;
    yield* api.chatInterrupt(chatId);
  })
);
