import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { ChatMailbox, ChatToolkit } from "./chat-toolkit.js";
import { JokeApi } from "./joke-api.js";
import { WeatherApi } from "./weather-api.js";

export const HandlersLive = ChatToolkit.toLayer(
  Effect.gen(function*() {
    const weatherApi = yield* WeatherApi;
    const jokeApi = yield* JokeApi;

    return {
      getCurrentDateTime: Effect.fnUntraced(function*() {
        const mailbox = yield* ChatMailbox;

        yield* Queue.offer(mailbox, {
          _tag: "ToolStart",
          toolName: "getCurrentDateTime",
          input: "{}",
        });

        const now = yield* DateTime.now;
        const formatted = DateTime.formatIso(now);

        yield* Queue.offer(mailbox, {
          _tag: "ToolSuccess",
          toolName: "getCurrentDateTime",
          output: formatted,
        });

        return formatted;
      }),

      getWeather: Effect.fnUntraced(function*(params) {
        const mailbox = yield* ChatMailbox;

        yield* Queue.offer(mailbox, {
          _tag: "ToolStart",
          toolName: "getWeather",
          input: JSON.stringify(params),
        });

        const result = yield* weatherApi.getForecast(params).pipe(
          Effect.tapError(() =>
            Queue.offer(mailbox, { _tag: "ToolFailure", toolName: "getWeather" })
          ),
        );

        yield* Queue.offer(mailbox, {
          _tag: "ToolSuccess",
          toolName: "getWeather",
          output: result,
        });

        return result;
      }),

      fetchRandomJoke: Effect.fnUntraced(function*() {
        const mailbox = yield* ChatMailbox;

        yield* Queue.offer(mailbox, {
          _tag: "ToolStart",
          toolName: "fetchRandomJoke",
          input: "{}",
        });

        const joke = yield* jokeApi.fetchRandom().pipe(
          Effect.tapError(() =>
            Queue.offer(mailbox, { _tag: "ToolFailure", toolName: "fetchRandomJoke" })
          ),
        );

        yield* Queue.offer(mailbox, {
          _tag: "ToolSuccess",
          toolName: "fetchRandomJoke",
          output: joke,
        });

        return joke;
      }),
    };
  }),
);

export const ChatToolkitLive = HandlersLive.pipe(
  Layer.provide(WeatherApi.layer),
  Layer.provide(JokeApi.layer),
);
