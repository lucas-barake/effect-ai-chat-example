import { AuthMiddleware, CurrentUser } from "@app/domain";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export const AuthMiddlewareLive = Layer.succeed(AuthMiddleware)(
  AuthMiddleware.of((effect) =>
    Effect.provideService(effect, CurrentUser, {
      id: "user_1",
      name: "Mock User",
      email: "mock@example.com",
    })
  ),
);
