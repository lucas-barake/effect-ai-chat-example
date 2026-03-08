import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as RcMap from "effect/RcMap";
import * as Ref from "effect/Ref";
import type * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Take from "effect/Take";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { WorkflowInstance } from "effect/unstable/workflow/WorkflowEngine";

export const makeWorkflowRunCoordinator = <
  OwnerId,
  RunId,
  Event,
  Name extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  Metadata,
  MissingError,
  BusyError,
>(options: {
  readonly workflow: Workflow.Workflow<Name, Payload, Success, Error>;
  readonly ownerId: (payload: Payload["Type"]) => OwnerId;
  readonly runId: (payload: Payload["Type"]) => RunId;
  readonly missingRun: (runId: RunId) => MissingError;
  readonly busy: (ownerId: OwnerId) => BusyError;
  readonly prepare: (payload: Payload["Type"]) => Effect.Effect<Metadata, BusyError>;
  readonly run: (args: {
    readonly payload: Payload["Type"];
    readonly metadata: Metadata;
    readonly mailbox: PubSub.PubSub<Take.Take<Event, Error["Type"]>>;
  }) => Effect.Effect<Success["Type"], Error["Type"]>;
  readonly finalize: (args: {
    readonly payload: Payload["Type"];
    readonly metadata: Metadata;
    readonly exit: Exit.Exit<Success["Type"], Error["Type"]>;
  }) => Effect.Effect<void>;
}) =>
  Effect.gen(function*() {
    const workflowEngine = yield* WorkflowEngine.WorkflowEngine;

    const activeOwners = yield* Ref.make(HashMap.empty<OwnerId, RunId>());
    const runs = yield* Ref.make(
      HashMap.empty<RunId, {
        readonly ownerId: OwnerId;
        readonly metadata: Metadata;
        readonly executionId: string;
        readonly interrupt: Deferred.Deferred<void>;
      }>(),
    );
    const ownerChanges = yield* RcMap.make({
      lookup: () => SubscriptionRef.make<RunId | null>(null),
    });
    const eventChannels = yield* RcMap.make({
      lookup: () => PubSub.unbounded<Take.Take<Event, Error["Type"]>>({ replay: Infinity }),
      idleTimeToLive: Duration.infinity,
    });

    const lookupRun = (runId: RunId) =>
      Ref.get(runs).pipe(
        Effect.map((map) => HashMap.get(map, runId)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(options.missingRun(runId)),
            onSome: Effect.succeed,
          }),
        ),
      );

    yield* workflowEngine.register(
      options.workflow,
      Effect.fnUntraced(function*(payload) {
        const runId = options.runId(payload);
        const state = yield* Ref.get(runs).pipe(
          Effect.map((map) => HashMap.get(map, runId)),
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.die(`missing run state for ${String(runId)}`),
              onSome: Effect.succeed,
            }),
          ),
        );
        const mailbox = yield* RcMap.get(eventChannels, runId);
        const ownerRef = yield* RcMap.get(ownerChanges, state.ownerId);
        const workflow = yield* WorkflowInstance;

        yield* SubscriptionRef.set(ownerRef, runId);

        const exit = yield* Effect.gen(function*() {
          const runFiber = yield* options.run({ payload, metadata: state.metadata, mailbox }).pipe(
            Effect.forkScoped,
          );

          yield* Deferred.await(state.interrupt).pipe(
            Effect.andThen(Effect.sync(() => {
              workflow.interrupted = true;
            })),
            Effect.andThen(Fiber.interrupt(runFiber)),
            Effect.forkScoped,
          );

          return yield* Fiber.await(runFiber);
        });

        yield* Exit.isSuccess(exit)
          ? PubSub.publish(mailbox, Exit.void)
          : PubSub.publish(mailbox, Exit.failCause(exit.cause));

        yield* options.finalize({ payload, metadata: state.metadata, exit }).pipe(
          Effect.ensuring(
            Effect.gen(function*() {
              yield* Ref.update(activeOwners, HashMap.remove(state.ownerId));
              yield* Ref.update(runs, HashMap.remove(runId));
              yield* SubscriptionRef.set(ownerRef, null);
              yield* RcMap.invalidate(eventChannels, runId);
            }),
          ),
        );
      }),
    );

    return {
      changes: (ownerId: OwnerId) =>
        Stream.unwrap(
          RcMap.get(ownerChanges, ownerId).pipe(
            Effect.map((ref) => SubscriptionRef.changes(ref).pipe(Stream.drop(1))),
          ),
        ),

      resolve: Effect.fnUntraced(function*(runId: RunId) {
        const state = yield* lookupRun(runId);
        const events = Stream.unwrap(
          RcMap.get(eventChannels, runId).pipe(
            Effect.map((mailbox) => Stream.fromPubSubTake(mailbox)),
          ),
        );
        return {
          ownerId: state.ownerId,
          metadata: state.metadata,
          events,
        } as const;
      }),

      start: Effect.fnUntraced(function*(payload: Payload["Type"]) {
        const ownerId = options.ownerId(payload);
        const runId = options.runId(payload);

        return yield* Effect.uninterruptible(
          Effect.gen(function*() {
            const reserved = yield* Ref.modify(activeOwners, (map) => {
              if (HashMap.has(map, ownerId)) {
                return [false, map] as const;
              }
              return [true, HashMap.set(map, ownerId, runId)] as const;
            });
            if (!reserved) {
              return yield* Effect.fail(options.busy(ownerId));
            }

            const metadataExit = yield* Effect.exit(options.prepare(payload));
            if (Exit.isFailure(metadataExit)) {
              yield* Ref.update(activeOwners, HashMap.remove(ownerId));
              return yield* Effect.failCause(metadataExit.cause);
            }

            const interrupt = yield* Deferred.make<void>();
            const executionId = yield* options.workflow.executionId(payload);
            yield* Ref.update(
              runs,
              HashMap.set(runId, { ownerId, metadata: metadataExit.value, executionId, interrupt }),
            );
            yield* options.workflow.execute(payload, { discard: true }).pipe(
              Effect.provideService(WorkflowEngine.WorkflowEngine, workflowEngine),
            );
            return { runId } as const;
          }),
        );
      }),

      interrupt: Effect.fnUntraced(function*(ownerId: OwnerId) {
        const activeRun = yield* Ref.get(activeOwners).pipe(
          Effect.map((map) => HashMap.get(map, ownerId)),
        );
        if (Option.isNone(activeRun)) {
          return;
        }

        const state = yield* Ref.get(runs).pipe(
          Effect.map((map) => HashMap.get(map, activeRun.value)),
        );
        if (Option.isSome(state)) {
          yield* workflowEngine.interrupt(options.workflow, state.value.executionId);
          yield* Deferred.succeed(state.value.interrupt, undefined);
        }
      }),
    } as const;
  });
