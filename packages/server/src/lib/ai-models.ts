import type { ModelFamily } from "@app/domain/ai-models";
import * as AnthropicClient from "@effect/ai-anthropic/AnthropicClient";
import * as AnthropicLanguageModel from "@effect/ai-anthropic/AnthropicLanguageModel";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServiceMap from "effect/ServiceMap";
import type * as LanguageModel from "effect/unstable/ai/LanguageModel";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";

const AnthropicLive = AnthropicClient.layerConfig({
  apiKey: Config.redacted("ANTHROPIC_API_KEY"),
}).pipe(
  Layer.provide(FetchHttpClient.layer),
);

const SonnetModel = AnthropicLanguageModel.model("claude-sonnet-4-6");
const HaikuModel = AnthropicLanguageModel.model("claude-haiku-4-5-20251001");

export class AiModels extends ServiceMap.Service<AiModels>()("@app/ai/AiModels", {
  make: Effect.gen(function*() {
    const sonnetModel = yield* SonnetModel;
    const haikuModel = yield* HaikuModel;

    const getModelLayer = (model: ModelFamily): Layer.Layer<LanguageModel.LanguageModel> => {
      switch (model) {
        case "sonnet-4.6":
          return sonnetModel;
        case "haiku-4.5":
          return haikuModel;
      }
    };

    return {
      use: (model: ModelFamily) =>
      <A, E, R>(
        self: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E, Exclude<R, LanguageModel.LanguageModel>> =>
        Effect.provide(self, getModelLayer(model)),
    } as const;
  }),
}) {
  static layer: Layer.Layer<AiModels> = Layer.effect(this, this.make).pipe(
    Layer.provide(AnthropicLive),
    Layer.orDie,
  );
}
