import { ModelFamily } from "@app/domain/ai-models";
import { ChatId, RunId } from "@app/domain/api/chat-rpc";
import type { ChatEvent, Message } from "@app/domain/api/chat-rpc";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { AsyncResult } from "effect/unstable/reactivity";
import * as Atom from "effect/unstable/reactivity/Atom";
import { accumulateEvent, extractText } from "./chat-accumulator.js";
import { ChatApi } from "./chat-api.js";
import { ChatPreferences } from "./chat-preferences.js";
import type { ContentBlock, StreamState, ToolStatus, UIMessage } from "./chat-types.js";

export const chatRuntime = Atom.runtime(ChatApi.layer);
export const preferencesRuntime = Atom.runtime(ChatPreferences.layer);

type LocalTranscript =
  | { readonly _tag: "None"; }
  | { readonly _tag: "Deleted"; }
  | {
    readonly _tag: "Sending";
    readonly assistantMsgId: string;
    readonly messages: readonly UIMessage[];
  }
  | {
    readonly _tag: "Streaming";
    readonly runId: RunId;
    readonly assistantMsgId: string;
    readonly messages: readonly UIMessage[];
  }
  | {
    readonly _tag: "Overlay";
    readonly reason: "interrupted" | "failure" | "completion-race";
    readonly runId: RunId | null;
    readonly assistantMsgId: string | null;
    readonly messages: readonly UIMessage[];
  };

const localNone: LocalTranscript = { _tag: "None" };
const localDeleted: LocalTranscript = { _tag: "Deleted" };

const selectedModelDataAtom = preferencesRuntime.atom(
  Effect.gen(function*() {
    const preferences = yield* ChatPreferences;
    return yield* preferences.getSelectedModel().pipe(
      Effect.catch(() => Effect.succeed("sonnet-4.6" as const)),
    );
  }),
);

const setSelectedModelAtom = preferencesRuntime.fn<ModelFamily>()(
  Effect.fnUntraced(function*(model) {
    const preferences = yield* ChatPreferences;
    yield* preferences.setSelectedModel(model).pipe(
      Effect.catch(() => Effect.void),
    );
    return model;
  }),
);

export const selectedModelAtom = Atom.writable(
  (get) => {
    get.mount(setSelectedModelAtom);
    get.subscribe(selectedModelDataAtom, (result) => {
      if (AsyncResult.isSuccess(result)) {
        get.setSelf(result.value);
      }
    }, { immediate: true });
    return Option.getOrElse(get.self<typeof ModelFamily.Type>(), () => "sonnet-4.6" as const);
  },
  (ctx, value: typeof ModelFamily.Type) => {
    ctx.set(setSelectedModelAtom, value);
    ctx.setSelf(value);
  },
);

export const convertPersistedMessages = (
  messages: ReadonlyArray<Message>,
): readonly UIMessage[] => {
  const result: UIMessage[] = [];
  let i = 0;

  while (i < messages.length) {
    const msg = messages[i]!;

    if (msg.role === "user") {
      const content = typeof msg.content === "string"
        ? msg.content
        : msg.content
          .filter((part): part is typeof part & { type: "text"; } => part.type === "text")
          .map((part) => part.text)
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
          { readonly result: Schema.Json; readonly isFailure: boolean; }
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
              const toolResult = toolResults.get(part.id);
              const tool: ToolStatus = {
                id: part.id,
                toolName: part.name,
                status: toolResult ? (toolResult.isFailure ? "failure" : "success") : "start",
                input: typeof part.params === "string"
                  ? part.params
                  : JSON.stringify(part.params, null, 2),
                output: toolResult
                  ? (typeof toolResult.result === "string"
                    ? toolResult.result
                    : JSON.stringify(toolResult.result, null, 2))
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

const localTranscriptFamily = Atom.family((_chatId: ChatId) =>
  Atom.make<LocalTranscript>(localNone).pipe(Atom.setIdleTTL("1 minute"))
);

export const inputFamily = Atom.family((_chatId: ChatId) =>
  Atom.make("").pipe(Atom.setIdleTTL("1 day"))
);

export const chatDataFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.atom(
    Effect.gen(function*() {
      const api = yield* ChatApi;
      return yield* api.chatGet(chatId);
    }),
  )
);

const authoritativeMessagesFamily = Atom.family((chatId: ChatId) =>
  Atom.readable((get) => {
    const chatResult = get(chatDataFamily(chatId));
    return AsyncResult.isSuccess(chatResult)
      ? convertPersistedMessages(chatResult.value.messages)
      : [];
  })
);

export const messagesFamily = Atom.family((chatId: ChatId) =>
  Atom.readable((get) => {
    const local = get(localTranscriptFamily(chatId));
    if (local._tag === "Deleted") {
      return [];
    }
    return local._tag === "None"
      ? get(authoritativeMessagesFamily(chatId))
      : local.messages;
  })
);

export const pendingSendFamily = Atom.family((chatId: ChatId) =>
  Atom.readable((get) => get(localTranscriptFamily(chatId))._tag === "Sending")
);

export const attachedRunIdFamily = Atom.family((chatId: ChatId) =>
  Atom.readable((get) => {
    const local = get(localTranscriptFamily(chatId));
    return local._tag === "Streaming" ? local.runId : null;
  })
);

export const generatingFamily = Atom.family((chatId: ChatId) =>
  Atom.readable((get) => {
    const local = get(localTranscriptFamily(chatId));
    if (local._tag === "Sending" || local._tag === "Streaming") {
      return true;
    }
    if (local._tag === "Overlay" || local._tag === "Deleted") {
      return false;
    }
    const chatResult = get(chatDataFamily(chatId));
    return AsyncResult.isSuccess(chatResult) && chatResult.value.activeRunId !== null;
  })
);

const appendAssistantPlaceholder = ({
  messages,
}: {
  readonly messages: readonly UIMessage[];
}) => {
  const assistantMsgId = crypto.randomUUID();
  const assistant: UIMessage = {
    id: assistantMsgId,
    role: "assistant",
    content: "",
    contentBlocks: [],
    error: null,
  };
  return {
    assistantMsgId,
    messages: [...messages, assistant],
  };
};

const updateAssistantMessage = ({
  messages,
  assistantMsgId,
  updater,
}: {
  readonly messages: readonly UIMessage[];
  readonly assistantMsgId: string;
  readonly updater: (message: UIMessage) => UIMessage;
}) => messages.map((message) => message.id === assistantMsgId ? updater(message) : message);

const refreshChat = Effect.fnUntraced(function*({
  get,
  chatId,
}: {
  readonly get: Atom.FnContext;
  readonly chatId: ChatId;
}) {
  get.refresh(chatDataFamily(chatId));
  return yield* get.result(chatDataFamily(chatId), { suspendOnWaiting: true });
});

const loadAuthoritativeMessages = Effect.fnUntraced(function*({
  get,
  chatId,
  forceRefresh,
}: {
  readonly get: Atom.FnContext;
  readonly chatId: ChatId;
  readonly forceRefresh: boolean;
}) {
  if (!forceRefresh) {
    const chatResult = get(chatDataFamily(chatId));
    if (AsyncResult.isSuccess(chatResult)) {
      return convertPersistedMessages(chatResult.value.messages);
    }
  }
  const chat = yield* refreshChat({ get, chatId });
  return convertPersistedMessages(chat.messages);
});

const runStream = Effect.fnUntraced(function*({
  get,
  chatId,
  runId,
}: {
  readonly get: Atom.FnContext;
  readonly chatId: ChatId;
  readonly runId: RunId;
}) {
  const api = yield* ChatApi;

  let state: StreamState = { contentBlocks: [] };

  yield* api.chatEvents(runId).pipe(
    Stream.runForEach((event: ChatEvent) =>
      Effect.sync(() => {
        const local = get(localTranscriptFamily(chatId));
        if (local._tag !== "Streaming" || local.runId !== runId) {
          return;
        }

        state = accumulateEvent(state, event);
        const messages = updateAssistantMessage({
          messages: local.messages,
          assistantMsgId: local.assistantMsgId,
          updater: (message) => ({
            ...message,
            content: extractText(state.contentBlocks),
            contentBlocks: state.contentBlocks,
          }),
        });

        get.set(localTranscriptFamily(chatId), {
          ...local,
          messages,
        });
      })
    ),
    Effect.onExit(Exit.match({
      onSuccess: () =>
        Effect.gen(function*() {
          const local = get(localTranscriptFamily(chatId));
          if (local._tag !== "Streaming" || local.runId !== runId) {
            return;
          }

          const refreshExit = yield* Effect.exit(refreshChat({ get, chatId }));
          if (refreshExit._tag === "Failure") {
            get.set(localTranscriptFamily(chatId), {
              _tag: "Overlay",
              reason: "completion-race",
              runId,
              assistantMsgId: local.assistantMsgId,
              messages: local.messages,
            });
            return;
          }

          if (refreshExit.value.activeRunId === runId) {
            get.set(localTranscriptFamily(chatId), {
              _tag: "Overlay",
              reason: "completion-race",
              runId,
              assistantMsgId: local.assistantMsgId,
              messages: local.messages,
            });
            return;
          }

          if (refreshExit.value.activeRunId !== null) {
            const next = appendAssistantPlaceholder({
              messages: convertPersistedMessages(refreshExit.value.messages),
            });
            get.set(localTranscriptFamily(chatId), {
              _tag: "Streaming",
              runId: refreshExit.value.activeRunId,
              assistantMsgId: next.assistantMsgId,
              messages: next.messages,
            });
            get.mount(attachRunFamily(chatId));
            get.set(attachRunFamily(chatId), { runId: refreshExit.value.activeRunId });
            return;
          }

          get.set(localTranscriptFamily(chatId), localNone);
        }),
      onFailure: (cause) =>
        Effect.sync(() => {
          const local = get(localTranscriptFamily(chatId));
          if (local._tag !== "Streaming" || local.runId !== runId) {
            return;
          }

          const nextMessages = Cause.hasInterruptsOnly(cause)
            ? updateAssistantMessage({
              messages: local.messages,
              assistantMsgId: local.assistantMsgId,
              updater: (message) => ({
                ...message,
                content: message.content || "(interrupted)",
              }),
            })
            : updateAssistantMessage({
              messages: local.messages,
              assistantMsgId: local.assistantMsgId,
              updater: (message) => ({ ...message, error: cause }),
            });

          get.set(localTranscriptFamily(chatId), {
            _tag: "Overlay",
            reason: Cause.hasInterruptsOnly(cause) ? "interrupted" : "failure",
            runId,
            assistantMsgId: local.assistantMsgId,
            messages: nextMessages,
          });
          get.refresh(chatDataFamily(chatId));
        }),
    })),
  );
});

const attachRunFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.fn<{ runId: RunId; }>()(
    Effect.fnUntraced(function*({ runId }, get) {
      yield* runStream({ get, chatId, runId });
    }),
  ).pipe(Atom.setIdleTTL("1 minute"))
);

const prepareSend = Effect.fnUntraced(function*({
  get,
  chatId,
  message,
}: {
  readonly get: Atom.FnContext;
  readonly chatId: ChatId;
  readonly message: string;
}) {
  const local = get(localTranscriptFamily(chatId));
  if (local._tag === "Sending" || local._tag === "Streaming" || local._tag === "Deleted") {
    return Option.none<RunId>();
  }

  const baseMessages = yield* loadAuthoritativeMessages({
    get,
    chatId,
    forceRefresh: local._tag === "Overlay",
  });

  const api = yield* ChatApi;
  const userMsg: UIMessage = {
    id: crypto.randomUUID(),
    role: "user",
    content: message,
    contentBlocks: [],
    error: null,
  };
  const optimistic = appendAssistantPlaceholder({ messages: [...baseMessages, userMsg] });

  get.set(localTranscriptFamily(chatId), {
    _tag: "Sending",
    assistantMsgId: optimistic.assistantMsgId,
    messages: optimistic.messages,
  });

  const askExit = yield* Effect.exit(api.chatAsk({ chatId, message }));
  if (askExit._tag === "Failure") {
    get.set(localTranscriptFamily(chatId), localNone);
    return yield* Effect.failCause(askExit.cause);
  }

  const latestLocal = get(localTranscriptFamily(chatId));
  if (latestLocal._tag !== "Sending" || latestLocal.assistantMsgId !== optimistic.assistantMsgId) {
    if (latestLocal._tag !== "Deleted") {
      yield* api.chatInterrupt(chatId).pipe(Effect.ignore);
    }
    return Option.none<RunId>();
  }

  get.set(localTranscriptFamily(chatId), {
    _tag: "Streaming",
    runId: askExit.value.runId,
    assistantMsgId: optimistic.assistantMsgId,
    messages: optimistic.messages,
  });
  get.set(inputFamily(chatId), "");

  return Option.some(askExit.value.runId);
});

export const sendMessageFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.fn<{ message: string; }>()(
    Effect.fnUntraced(function*({ message }, get) {
      const runId = yield* prepareSend({ get, chatId, message });
      if (Option.isNone(runId)) {
        return;
      }
      yield* runStream({ get, chatId, runId: runId.value });
    }),
    { concurrent: true },
  ).pipe(Atom.setIdleTTL("1 minute"))
);

export const watchChatFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.fn<{ activeRunId: RunId | null; }>()(
    Effect.fnUntraced(function*({ activeRunId }, get) {
      const api = yield* ChatApi;

      get.mount(attachRunFamily(chatId));
      get.addFinalizer(() => {
        get.set(attachRunFamily(chatId), Atom.Interrupt);
      });

      const attach = Effect.fnUntraced(function*({ runId }: { readonly runId: RunId; }) {
        const localBefore = get(localTranscriptFamily(chatId));
        if (localBefore._tag === "Sending" || localBefore._tag === "Streaming") {
          return;
        }
        if (
          localBefore._tag === "Overlay" && localBefore.runId === runId
          && localBefore.assistantMsgId !== null
        ) {
          get.set(localTranscriptFamily(chatId), {
            _tag: "Streaming",
            runId,
            assistantMsgId: localBefore.assistantMsgId,
            messages: localBefore.messages,
          });
          get.set(attachRunFamily(chatId), { runId });
          return;
        }

        const chat = yield* api.chatGet(chatId);
        const localAfter = get(localTranscriptFamily(chatId));
        if (localAfter._tag === "Sending" || localAfter._tag === "Streaming") {
          return;
        }
        if (chat.activeRunId !== runId) {
          return;
        }

        const next = appendAssistantPlaceholder({
          messages: convertPersistedMessages(chat.messages),
        });
        get.set(localTranscriptFamily(chatId), {
          _tag: "Streaming",
          runId,
          assistantMsgId: next.assistantMsgId,
          messages: next.messages,
        });
        get.set(attachRunFamily(chatId), { runId });
      });

      if (activeRunId !== null) {
        yield* attach({ runId: activeRunId });
      }

      yield* api.chatWatch(chatId).pipe(
        Stream.runForEach((event) =>
          event.runId === null
            ? Effect.gen(function*() {
              const refreshExit = yield* Effect.exit(refreshChat({ get, chatId }));
              if (refreshExit._tag === "Failure") {
                return;
              }
              if (refreshExit.value.activeRunId !== null) {
                return;
              }

              const local = get(localTranscriptFamily(chatId));
              if (local._tag === "Overlay" && local.reason === "completion-race") {
                get.set(localTranscriptFamily(chatId), localNone);
              }
            })
            : attach({ runId: event.runId })
        ),
      );
    }),
  ).pipe(Atom.setIdleTTL("1 minute"))
);

export const chatListAtom = chatRuntime.atom(
  Effect.gen(function*() {
    const api = yield* ChatApi;
    return yield* api.chatList(null);
  }),
);

export const createChatAtom = chatRuntime.fn(
  Effect.fnUntraced(function*({
    title,
    model,
  }: {
    readonly title: string;
    readonly model: typeof ModelFamily.Type;
  }) {
    const api = yield* ChatApi;
    return yield* api.chatCreate({ title, model });
  }),
);

export const deleteChatFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.fn<void>()(
    Effect.fnUntraced(function*(_, get) {
      const api = yield* ChatApi;
      yield* api.chatDelete(chatId);
      get.set(attachRunFamily(chatId), Atom.Interrupt);
      get.set(watchChatFamily(chatId), Atom.Interrupt);
      get.set(inputFamily(chatId), "");
      get.set(localTranscriptFamily(chatId), localDeleted);
    }),
  ).pipe(Atom.setIdleTTL("1 minute"))
);

export const interruptFamily = Atom.family((chatId: ChatId) =>
  chatRuntime.fn<void>()(
    Effect.fnUntraced(function*(_, get) {
      get.set(attachRunFamily(chatId), Atom.Interrupt);

      const local = get(localTranscriptFamily(chatId));
      if (local._tag === "Sending") {
        get.set(localTranscriptFamily(chatId), localNone);
        return;
      }

      if (local._tag === "Streaming") {
        get.set(localTranscriptFamily(chatId), {
          _tag: "Overlay",
          reason: "interrupted",
          runId: local.runId,
          assistantMsgId: local.assistantMsgId,
          messages: updateAssistantMessage({
            messages: local.messages,
            assistantMsgId: local.assistantMsgId,
            updater: (message) => ({
              ...message,
              content: message.content || "(interrupted)",
            }),
          }),
        });
      }

      const api = yield* ChatApi;
      yield* api.chatInterrupt(chatId);
    }),
  )
);
