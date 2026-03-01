import { useAtomValue } from "@effect/atom-react";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Cause from "effect/Cause";
import { currentUserAtom } from "./atoms/current-user-atom.js";

export function App() {
  const result = useAtomValue(currentUserAtom);

  return (
    <div style={{ padding: 24, fontFamily: "system-ui, sans-serif" }}>
      <h1>Effect v4 Playground</h1>
      {AsyncResult.builder(result)
        .onInitial(() => <p>Loading...</p>)
        .onSuccess((user) => (
          <div>
            <h2>Current User</h2>
            <p><strong>ID:</strong> {user.id}</p>
            <p><strong>Name:</strong> {user.name}</p>
            <p><strong>Email:</strong> {user.email}</p>
          </div>
        ))
        .onFailure((cause) => (
          <div style={{ color: "red" }}>
            <h2>Error</h2>
            <pre>{Cause.pretty(cause)}</pre>
          </div>
        ))
        .render()}
    </div>
  );
}
