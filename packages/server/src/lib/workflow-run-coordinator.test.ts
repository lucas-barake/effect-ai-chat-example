import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { makeWorkflowRunCoordinator } from "./workflow-run-coordinator.js";

class BusyError extends Schema.TaggedErrorClass<BusyError>()("BusyError", {
  ownerId: Schema.String,
}) {}

class MissingRunError extends Schema.TaggedErrorClass<MissingRunError>()("MissingRunError", {
  runId: Schema.String,
}) {}

class RunFailed extends Schema.TaggedErrorClass<RunFailed>()("RunFailed", {
  message: Schema.String,
}) {}

const TestWorkflow = Workflow.make({
  name: "test/WorkflowRunCoordinator",
  payload: {
    ownerId: Schema.String,
    runId: Schema.String,
    mode: Schema.Literals(["success", "wait"]),
  },
  success: Schema.Void,
  error: RunFailed,
  idempotencyKey: ({ runId }) => runId,
});

const makeCoordinator = (gate: Deferred.Deferred<void>) =>
  makeWorkflowRunCoordinator<
    string,
    string,
    { readonly _tag: "Value"; readonly value: string; },
    "test/WorkflowRunCoordinator",
    typeof TestWorkflow.payloadSchema,
    typeof TestWorkflow.successSchema,
    typeof TestWorkflow.errorSchema,
    { readonly ownerId: string; },
    MissingRunError,
    BusyError
  >({
    workflow: TestWorkflow,
    ownerId: (payload) => payload.ownerId,
    runId: (payload) => payload.runId,
    missingRun: (runId) => new MissingRunError({ runId }),
    busy: (ownerId) => new BusyError({ ownerId }),
    prepare: (payload) => Effect.succeed({ ownerId: payload.ownerId }),
    run: Effect.fnUntraced(function*({ payload, mailbox }) {
      yield* PubSub.publish(mailbox, [{ _tag: "Value", value: payload.runId }]);
      if (payload.mode === "wait") {
        yield* Deferred.await(gate);
      }
    }),
    finalize: () => Effect.void,
  });

describe("makeWorkflowRunCoordinator", () => {
  it.live("changes emit start and finish transitions", () =>
    Effect.gen(function*() {
      const gate = yield* Deferred.make<void>();
      const runs = yield* makeCoordinator(gate);

      const changesFiber = yield* runs.changes("owner-1").pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;

      yield* runs.start({ ownerId: "owner-1", runId: "run-1", mode: "wait" });
      yield* Deferred.succeed(gate, undefined);

      const changes = yield* Fiber.join(changesFiber);
      expect(changes).toEqual(["run-1", null]);
    }).pipe(Effect.provide(WorkflowEngine.layerMemory)), { timeout: 5000 });

  it.live(
    "resolve replays events while active and becomes missing after completion",
    () =>
      Effect.gen(function*() {
        const gate = yield* Deferred.make<void>();
        const runs = yield* makeCoordinator(gate);

        const { runId } = yield* runs.start({ ownerId: "owner-1", runId: "run-1", mode: "wait" });
        const run = yield* runs.resolve(runId);
        const events = yield* run.events.pipe(Stream.take(1), Stream.runCollect);

        expect(events).toEqual([{ _tag: "Value", value: "run-1" }]);

        yield* Deferred.succeed(gate, undefined);
        yield* Effect.sleep("200 millis");

        const error = yield* runs.resolve(runId).pipe(Effect.flip);
        expect(error._tag).toBe("MissingRunError");
      }).pipe(Effect.provide(WorkflowEngine.layerMemory)),
    { timeout: 5000 },
  );

  it.live(
    "start fails with BusyError while the owner already has an active run",
    () =>
      Effect.gen(function*() {
        const gate = yield* Deferred.make<void>();
        const runs = yield* makeCoordinator(gate);

        yield* runs.start({ ownerId: "owner-1", runId: "run-1", mode: "wait" });
        const error = yield* runs.start({ ownerId: "owner-1", runId: "run-2", mode: "success" })
          .pipe(
            Effect.flip,
          );

        expect(error._tag).toBe("BusyError");

        yield* Deferred.succeed(gate, undefined);
      }).pipe(Effect.provide(WorkflowEngine.layerMemory)),
    { timeout: 5000 },
  );
});
