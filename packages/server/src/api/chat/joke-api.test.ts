import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import { JokeApi } from "./joke-api.js";

const makeHttpClient = (
  handler: (
    request: HttpClientRequest.HttpClientRequest,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError>,
) =>
  HttpClient.makeWith(
    Effect.fnUntraced(function*(requestEffect) {
      const request = yield* requestEffect;
      return yield* handler(request);
    }),
    Effect.succeed as HttpClient.HttpClient.Preprocess<HttpClientError.HttpClientError, never>,
  );

const makeTestJokeApi = (client: HttpClient.HttpClient) =>
  Layer.effect(JokeApi, JokeApi.make).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
  );

describe("JokeApi", () => {
  it.effect("fetchRandom success returns joke string", () => {
    const client = makeHttpClient((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(
            JSON.stringify({
              joke: "Why did the chicken cross the road? To get to the other side!",
            }),
            { status: 200, headers: { "content-type": "application/json" } },
          ),
        ),
      )
    );

    return Effect.gen(function*() {
      const api = yield* JokeApi;
      const result = yield* api.fetchRandom();
      expect(result).toBe("Why did the chicken cross the road? To get to the other side!");
    }).pipe(Effect.provide(makeTestJokeApi(client)));
  });

  it.effect("fetchRandom HttpClientError fails with Joke API error prefix", () => {
    const client = makeHttpClient((request) => {
      const badResponse = HttpClientResponse.fromWeb(request, new Response(null, { status: 400 }));
      return Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.StatusCodeError({
            request,
            response: badResponse,
            description: "Bad Request",
          }),
        }),
      );
    });

    return Effect.gen(function*() {
      const api = yield* JokeApi;
      const result = yield* api.fetchRandom().pipe(Effect.flip);
      expect(result).toMatch(/^Joke API error:/);
    }).pipe(Effect.provide(makeTestJokeApi(client)));
  });

  it.effect("fetchRandom SchemaError fails with Joke parse error prefix", () => {
    const client = makeHttpClient((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(
            JSON.stringify({ wrong: "shape" }),
            { status: 200, headers: { "content-type": "application/json" } },
          ),
        ),
      )
    );

    return Effect.gen(function*() {
      const api = yield* JokeApi;
      const result = yield* api.fetchRandom().pipe(Effect.flip);
      expect(result).toMatch(/^Joke parse error:/);
    }).pipe(Effect.provide(makeTestJokeApi(client)));
  });
});
