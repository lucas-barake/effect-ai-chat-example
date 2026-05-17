import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

const JokeResponse = Schema.Struct({
  joke: Schema.String,
});

export class JokeApi extends Context.Service<JokeApi>()("@app/chat/JokeApi", {
  make: Effect.gen(function*() {
    const httpClient = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest(HttpClientRequest.setHeader("Accept", "application/json")),
      HttpClient.retryTransient({}),
    );

    return {
      fetchRandom: () =>
        httpClient.get("https://icanhazdadjoke.com/").pipe(
          Effect.flatMap(HttpClientResponse.schemaBodyJson(JokeResponse)),
          Effect.catchTags({
            HttpClientError: (e) => Effect.fail(`Joke API error: ${e.message}`),
            SchemaError: (e) => Effect.fail(`Joke parse error: ${e.message}`),
          }),
          Effect.map((data) => data.joke),
        ),
    };
  }),
}) {
  static layer: Layer.Layer<JokeApi> = Layer.effect(this, this.make).pipe(
    Layer.provide(FetchHttpClient.layer),
    Layer.orDie,
  );
}
