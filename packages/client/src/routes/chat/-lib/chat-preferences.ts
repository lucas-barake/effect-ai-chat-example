import { ModelFamily } from "@app/domain/ai-models";
import * as BrowserKeyValueStore from "@effect/platform-browser/BrowserKeyValueStore";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServiceMap from "effect/ServiceMap";
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore";

const selectedModelKey = "@app/chat/selected-model";

export class ChatPreferences extends ServiceMap.Service<ChatPreferences>()(
  "@app/chat/ChatPreferences",
  {
    make: Effect.gen(function*() {
      const store = yield* KeyValueStore.KeyValueStore;
      const schemaStore = KeyValueStore.toSchemaStore(store, ModelFamily);

      return {
        getSelectedModel: () =>
          schemaStore.get(selectedModelKey).pipe(
            Effect.map(Option.getOrElse(() => "sonnet-4.6" as const)),
          ),
        setSelectedModel: (model: typeof ModelFamily.Type) =>
          schemaStore.set(selectedModelKey, model),
      };
    }),
  },
) {
  static layer: Layer.Layer<ChatPreferences> = Layer.effect(this, this.make).pipe(
    Layer.provide(BrowserKeyValueStore.layerLocalStorage),
  );
}
