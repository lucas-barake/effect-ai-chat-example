import * as Schema from "effect/Schema";
import * as ServiceMap from "effect/ServiceMap";

export const CurrentUserSchema = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  email: Schema.String,
});

export class CurrentUser
  extends ServiceMap.Service<CurrentUser, typeof CurrentUserSchema.Type>()("CurrentUser") {}
