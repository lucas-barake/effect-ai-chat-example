import type * as Chat from "@app/domain/api/chat-rpc";
import { describe, expect, it } from "@effect/vitest";
import { withLanguageModel } from "@test/utils/with-language-model.js";
import type { Done } from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { ChatProcessor, makePrompt } from "./chat-processor.js";
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

describe("makePrompt", () => {
  it("empty messages returns only system message", () => {
    const result = makePrompt([]);
    expect(result).toHaveLength(1);
    expect(result[0]!.role).toBe("system");
  });

  it("user string message forwarded verbatim", () => {
    const result = makePrompt([{ role: "user", content: "hello" }]);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual({ role: "user", content: "hello" });
  });

  it("user array message maps text parts", () => {
    const result = makePrompt([{ role: "user", content: [{ type: "text", text: "hello" }] }]);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual({ role: "user", content: [{ type: "text", text: "hello" }] });
  });

  it("assistant string message wrapped in text array", () => {
    const result = makePrompt([{ role: "assistant", content: "reply" }]);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual({ role: "assistant", content: [{ type: "text", text: "reply" }] });
  });

  it("assistant array with text and tool-call parts forwarded", () => {
    const result = makePrompt([{
      role: "assistant",
      content: [
        { type: "text", text: "Using tool" },
        { type: "tool-call", id: "c1", name: "getCurrentDateTime", params: {} },
      ],
    }]);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual({
      role: "assistant",
      content: [
        { type: "text", text: "Using tool" },
        { type: "tool-call", id: "c1", name: "getCurrentDateTime", params: {} },
      ],
    });
  });

  it("tool message forwarded", () => {
    const result = makePrompt([{
      role: "tool",
      content: [{
        type: "tool-result",
        id: "c1",
        name: "getCurrentDateTime",
        result: "2024-01-01T00:00:00Z",
        isFailure: false,
      }],
    }]);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual({
      role: "tool",
      content: [{
        type: "tool-result",
        id: "c1",
        name: "getCurrentDateTime",
        result: "2024-01-01T00:00:00Z",
        isFailure: false,
      }],
    });
  });
});

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
