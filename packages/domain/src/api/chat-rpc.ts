import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import { type ModelFamily } from "../ai-models.js";
import { AuthMiddleware } from "../auth.js";

export const ToolName = Schema.Literals(["getCurrentDateTime", "getWeather", "fetchRandomJoke"]);
export type ToolName = typeof ToolName.Type;

export const ToolEvent = Schema.Union([
  Schema.TaggedStruct("ToolStart", { toolName: ToolName, input: Schema.String }),
  Schema.TaggedStruct("ToolFailure", { toolName: ToolName }),
  Schema.TaggedStruct("ToolSuccess", { toolName: ToolName, output: Schema.String }),
]);
export type ToolEvent = typeof ToolEvent.Type;

const TextPart = Schema.Struct({ type: Schema.Literal("text"), text: Schema.String });
const ToolCallPart = Schema.Struct({
  type: Schema.Literal("tool-call"),
  id: Schema.String,
  name: ToolName,
  params: Schema.Json,
});
const ToolResultPart = Schema.Struct({
  type: Schema.Literal("tool-result"),
  id: Schema.String,
  name: ToolName,
  result: Schema.Json,
  isFailure: Schema.Boolean,
});
const MessagePart = Schema.Union([TextPart, ToolCallPart, ToolResultPart]);
const MessageContent = Schema.Union([Schema.String, Schema.Array(MessagePart)]);

export interface Message {
  readonly role: "user" | "assistant" | "tool";
  readonly content: typeof MessageContent.Type;
}

const MessageSchema: Schema.Codec<Message> = Schema.Struct({
  role: Schema.Literals(["user", "assistant", "tool"]),
  content: MessageContent,
});

export const Message = Schema.Opaque<Message>()(MessageSchema);

const MessageEvent = Schema.Union([
  Schema.TaggedStruct("Chunk", { delta: Schema.String }),
  Schema.TaggedStruct("ReasoningChunk", { delta: Schema.String }),
  Schema.TaggedStruct("ToolStart", { toolName: ToolName, input: Schema.String }),
  Schema.TaggedStruct("ToolFailure", { toolName: ToolName }),
  Schema.TaggedStruct("ToolSuccess", { toolName: ToolName, output: Schema.String }),
]);
export type MessageEvent = typeof MessageEvent.Type;

export class ChatAsk extends Rpc.make("chat_ask", {
  stream: true,
  payload: {
    messages: Schema.Array(Message),
    model: Schema.Literals(["sonnet-4.6", "haiku-4.5"]) as Schema.Codec<ModelFamily>,
  },
  success: MessageEvent,
}) {}

export class ChatRpc extends RpcGroup.make(ChatAsk).middleware(AuthMiddleware) {}
