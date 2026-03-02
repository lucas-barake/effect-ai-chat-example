import * as Schema from "effect/Schema";

export const ModelFamily = Schema.Literals(["sonnet-4.6", "haiku-4.5"]);
export type ModelFamily = typeof ModelFamily.Type;
