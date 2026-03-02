import type * as Chat from "@app/domain/api/chat-rpc";
import type { Done } from "effect/Cause";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as ServiceMap from "effect/ServiceMap";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

export class ChatMailbox extends ServiceMap.Service<
  ChatMailbox,
  Queue.Queue<Chat.MessageEvent, Done>
>()("ChatMailbox") {}

export const getCurrentDateTime = Tool.make("getCurrentDateTime", {
  description: "Get the current date and time",
  success: Schema.String,
  dependencies: [ChatMailbox],
});

export const getWeather = Tool.make("getWeather", {
  description: "Get current weather for a location",
  parameters: Schema.Struct({ latitude: Schema.Number, longitude: Schema.Number }),
  success: Schema.String,
  failure: Schema.String,
  failureMode: "return",
  dependencies: [ChatMailbox],
});

export const fetchRandomJoke = Tool.make("fetchRandomJoke", {
  description: "Fetch a random dad joke",
  success: Schema.String,
  failure: Schema.String,
  failureMode: "return",
  dependencies: [ChatMailbox],
});

export const ChatToolkit = Toolkit.make(getCurrentDateTime, getWeather, fetchRandomJoke);
