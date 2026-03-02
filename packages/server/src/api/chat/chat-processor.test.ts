import type * as Chat from "@app/domain/api/chat-rpc";
import { describe, expect, it } from "@effect/vitest";
import { withLanguageModel } from "@test/utils/with-language-model.js";
import type { Done } from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { ChatProcessor } from "./chat-processor.js";
import { HandlersLive } from "./chat-toolkit-live.js";
import { ChatMailbox } from "./chat-toolkit.js";
import { JokeApi } from "./joke-api.js";
import { WeatherApi } from "./weather-api.js";

const makeMailbox = Effect.gen(function*() {
  const mailbox = yield* Queue.make<Chat.MessageEvent, Done>();
  return { mailbox, events: () => Queue.takeAll(mailbox) };
});

const MockWeatherApi = Layer.mock(WeatherApi)({
  getForecast: () => Effect.succeed("Sunny, 22°C"),
});

const MockJokeApi = Layer.mock(JokeApi)({
  fetchRandom: () => Effect.succeed("Why did the chicken cross the road?"),
});

const TestHandlers = HandlersLive.pipe(
  Layer.provide(MockWeatherApi),
  Layer.provide(MockJokeApi),
);

const userMessage = (content: string): Chat.Message => ({ role: "user", content });

describe("ChatProcessor", () => {
  it.effect("text-delta parts become Chunk mailbox events", () =>
    Effect.gen(function*() {
      const { mailbox, events } = yield* makeMailbox;
      const processor = yield* ChatProcessor;

      yield* processor.run([userMessage("Hello")]).pipe(
        withLanguageModel({
          streamText: [{ type: "text-delta", id: "t1", delta: "Hello!" }],
        }),
        Effect.provideService(ChatMailbox, mailbox),
        Effect.provide(TestHandlers),
      );

      const evts = yield* events();
      expect(evts).toHaveLength(1);
      expect(evts[0]).toEqual({ _tag: "Chunk", delta: "Hello!" });
    }).pipe(Effect.provide(ChatProcessor.layer)));

  it.effect("reasoning-delta parts become ReasoningChunk mailbox events", () =>
    Effect.gen(function*() {
      const { mailbox, events } = yield* makeMailbox;
      const processor = yield* ChatProcessor;

      yield* processor.run([userMessage("Think carefully")]).pipe(
        withLanguageModel({
          streamText: [{ type: "reasoning-delta", id: "r1", delta: "Thinking..." }],
        }),
        Effect.provideService(ChatMailbox, mailbox),
        Effect.provide(TestHandlers),
      );

      const evts = yield* events();
      expect(evts).toHaveLength(1);
      expect(evts[0]).toEqual({ _tag: "ReasoningChunk", delta: "Thinking..." });
    }).pipe(Effect.provide(ChatProcessor.layer)));

  it.effect("loop continues on tool-calls finish reason and stops on stop", () =>
    Effect.gen(function*() {
      const { mailbox, events } = yield* makeMailbox;
      const processor = yield* ChatProcessor;
      let calls = 0;

      yield* processor.run([userMessage("What time is it?")]).pipe(
        withLanguageModel({
          streamText: () => {
            calls += 1;
            if (calls === 1) {
              return [{
                type: "finish" as const,
                reason: "tool-calls" as const,
                usage: {
                  inputTokens: {
                    uncached: undefined,
                    total: undefined,
                    cacheRead: undefined,
                    cacheWrite: undefined,
                  },
                  outputTokens: { total: undefined, text: undefined, reasoning: undefined },
                },
                response: undefined,
              }];
            }
            return [{ type: "text-delta" as const, id: "t1", delta: "It is noon." }];
          },
        }),
        Effect.provideService(ChatMailbox, mailbox),
        Effect.provide(TestHandlers),
      );

      expect(calls).toBe(2);
      const evts = yield* events();
      expect(evts.some((e) => e._tag === "Chunk" && "delta" in e && e.delta === "It is noon."))
        .toBe(true);
    }).pipe(Effect.provide(ChatProcessor.layer)));
});
