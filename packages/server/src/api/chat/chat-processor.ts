import type * as Chat from "@app/domain/api/chat-rpc";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as ServiceMap from "effect/ServiceMap";
import * as Stream from "effect/Stream";
import * as AiChat from "effect/unstable/ai/Chat";
import * as Prompt from "effect/unstable/ai/Prompt";
import type * as Response from "effect/unstable/ai/Response";
import { ChatMailbox, ChatToolkit } from "./chat-toolkit.js";

export class ChatProcessor extends ServiceMap.Service<ChatProcessor>()("ChatProcessor", {
  make: Effect.succeed({
    run: Effect.fnUntraced(function*(messages: readonly Chat.Message[]) {
      const mailbox = yield* ChatMailbox;
      const toolkit = yield* ChatToolkit;
      const prompt = makePrompt(messages);
      const chat = yield* AiChat.fromPrompt(prompt);

      let cont = true;
      while (cont) {
        const result = yield* chat.streamText({ prompt: Prompt.empty, toolkit }).pipe(
          Stream.runFoldEffect(
            () => ({ finish: Option.none<Response.FinishReason>() }),
            Effect.fnUntraced(function*(acc, part) {
              if (part.type === "text-delta") {
                yield* Queue.offer(mailbox, { _tag: "Chunk" as const, delta: part.delta });
                return acc;
              } else if (part.type === "reasoning-delta") {
                yield* Queue.offer(mailbox, {
                  _tag: "ReasoningChunk" as const,
                  delta: part.delta,
                });
                return acc;
              } else if (part.type === "finish") {
                return { ...acc, finish: Option.some(part.reason) };
              }
              return acc;
            }),
          ),
        );

        cont = result.finish.pipe(
          Option.map((f) => f === "tool-calls"),
          Option.getOrElse(() => false),
        );
      }
    }),
  }),
}) {
  static layer: Layer.Layer<ChatProcessor> = Layer.effect(this, this.make);
}

export const makePrompt = (messages: readonly Chat.Message[]): Array<Prompt.MessageEncoded> => {
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
