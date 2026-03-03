import type { ChatModel } from "@/db/chat-model.js";
import * as Chat from "@app/domain/api/chat-rpc";
import * as Array from "effect/Array";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as ServiceMap from "effect/ServiceMap";
import * as Stream from "effect/Stream";
import * as AiChat from "effect/unstable/ai/Chat";
import * as Prompt from "effect/unstable/ai/Prompt";
import type * as Response from "effect/unstable/ai/Response";
import { ChatMailbox, ChatToolkit } from "./chat-toolkit.js";

export class ChatProcessor extends ServiceMap.Service<ChatProcessor>()("ChatProcessor", {
  make: Effect.succeed({
    run: Effect.fnUntraced(function*(chat: typeof ChatModel.Type, message: string) {
      const mailbox = yield* ChatMailbox;
      const toolkit = yield* ChatToolkit;

      const userMsg: typeof Chat.Message.Type = { role: "user", content: message };
      const prompt = makePrompt([...chat.messages, userMsg]);
      const aichat = yield* AiChat.fromPrompt(prompt);

      const newMessages: Array<typeof Chat.Message.Type> = [];

      let cont = true;
      while (cont) {
        const result = yield* aichat.streamText({ prompt: Prompt.empty, toolkit }).pipe(
          Stream.runFoldEffect(
            () => ({
              finish: Option.none<Response.FinishReason>(),
              textSoFar: "",
              toolCalls: Array.empty<{
                type: "tool-call";
                id: string;
                name: Chat.ToolName;
                params: typeof Schema.Json.Type;
              }>(),
              toolResults: Array.empty<{
                type: "tool-result";
                id: string;
                name: Chat.ToolName;
                result: typeof Schema.Json.Type;
                isFailure: boolean;
              }>(),
            }),
            Effect.fnUntraced(function*(acc, part) {
              if (part.type === "text-delta") {
                yield* PubSub.publish(mailbox, { _tag: "Chunk" as const, delta: part.delta });
                return { ...acc, textSoFar: acc.textSoFar + part.delta };
              } else if (part.type === "reasoning-delta") {
                yield* PubSub.publish(mailbox, {
                  _tag: "ReasoningChunk" as const,
                  delta: part.delta,
                });
                return acc;
              } else if (part.type === "tool-call") {
                return {
                  ...acc,
                  toolCalls: [
                    ...acc.toolCalls,
                    {
                      type: "tool-call" as const,
                      id: part.id,
                      name: part.name,
                      params: part.params as typeof Schema.Json.Type,
                    },
                  ],
                };
              } else if (part.type === "tool-result") {
                return {
                  ...acc,
                  toolResults: [
                    ...acc.toolResults,
                    {
                      type: "tool-result" as const,
                      id: part.id,
                      name: part.name,
                      result: part.result as typeof Schema.Json.Type,
                      isFailure: part.isFailure,
                    },
                  ],
                };
              } else if (part.type === "finish") {
                return { ...acc, finish: Option.some(part.reason) };
              }
              return acc;
            }),
          ),
        );

        const assistantParts: Array<Chat.MessagePart> = [];
        if (result.textSoFar) {
          assistantParts.push({ type: "text", text: result.textSoFar });
        }
        assistantParts.push(...result.toolCalls);

        if (assistantParts.length > 0) {
          newMessages.push({
            role: "assistant",
            content: assistantParts.length === 1 && assistantParts[0]!.type === "text"
              ? result.textSoFar
              : assistantParts,
          });
        }

        if (result.toolResults.length > 0) {
          newMessages.push({ role: "tool", content: result.toolResults });
        }

        cont = result.finish.pipe(
          Option.map((f) => f === "tool-calls"),
          Option.getOrElse(() => false),
        );
      }

      return newMessages as ReadonlyArray<typeof Chat.Message.Type>;
    }),
  }),
}) {
  static layer: Layer.Layer<ChatProcessor> = Layer.effect(this, this.make);
}

export const makePrompt = (
  messages: readonly (typeof Chat.Message.Type)[],
): Array<Prompt.MessageEncoded> => {
  const result: Array<Prompt.MessageEncoded> = [
    {
      role: "system",
      content:
        "You are a helpful assistant. You have access to tools for getting the current date/time, weather information, and random jokes. Use them when appropriate.",
    },
  ];
  for (const msg of messages) {
    if (msg.role === "user" && typeof msg.content === "string") {
      result.push({ role: "user", content: msg.content });
    } else if (msg.role === "user" && Array.isArray(msg.content)) {
      const parts: Array<Prompt.UserMessagePartEncoded> = [];
      for (const part of msg.content) {
        if (typeof part === "string") continue;
        if (part.type === "text") {
          parts.push({ type: "text", text: part.text });
        }
      }
      if (parts.length > 0) {
        result.push({ role: "user", content: parts });
      }
    } else if (msg.role === "assistant" && typeof msg.content === "string") {
      result.push({ role: "assistant", content: [{ type: "text" as const, text: msg.content }] });
    } else if (msg.role === "assistant" && Array.isArray(msg.content)) {
      const parts: Array<Prompt.AssistantMessagePartEncoded> = [];
      for (const part of msg.content) {
        if (typeof part === "string") continue;
        if (part.type === "text") {
          parts.push({ type: "text", text: part.text });
        } else if (part.type === "tool-call") {
          parts.push({ type: "tool-call", id: part.id, name: part.name, params: part.params });
        }
      }
      result.push({ role: "assistant", content: parts });
    } else if (msg.role === "tool" && Array.isArray(msg.content)) {
      const parts: Array<Prompt.ToolMessagePartEncoded> = [];
      for (const part of msg.content) {
        if (typeof part === "string") continue;
        if (part.type === "tool-result") {
          parts.push({
            type: "tool-result",
            id: part.id,
            name: part.name,
            result: part.result,
            isFailure: part.isFailure,
          });
        }
      }
      result.push({ role: "tool", content: parts });
    }
  }
  return result;
};
