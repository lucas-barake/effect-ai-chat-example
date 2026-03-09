import * as Deferred from "effect/Deferred";
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

export namespace WorkflowRunCoordinator {
  export const make = <
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

      const state = yield* Ref.make({
        activeOwners: HashMap.empty<OwnerId, RunId>(),
        runs: HashMap.empty<
          RunId,
          | {
            readonly _tag: "Preparing";
            readonly ownerId: OwnerId;
            readonly interrupt: Deferred.Deferred<void>;
          }
          | {
            readonly _tag: "Active";
            readonly ownerId: OwnerId;
            readonly metadata: Metadata;
            readonly executionId: string;
            readonly interrupt: Deferred.Deferred<void>;
          }
        >(),
      });
      const ownerChanges = yield* RcMap.make({
        lookup: () => SubscriptionRef.make<RunId | null>(null),
      });
      const eventChannels = yield* RcMap.make({
        lookup: () => PubSub.unbounded<Take.Take<Event, Error["Type"]>>({ replay: Infinity }),
      });

      const reserveOwner = (ownerId: OwnerId, runId: RunId) =>
        Ref.modify(state, (current) => {
          if (HashMap.has(current.activeOwners, ownerId)) {
            return [false, current] as const;
          }

          return [true, {
            ...current,
            activeOwners: HashMap.set(current.activeOwners, ownerId, runId),
          }] as const;
        });

      const storeRun = (
        runId: RunId,
        run:
          | {
            readonly _tag: "Preparing";
            readonly ownerId: OwnerId;
            readonly interrupt: Deferred.Deferred<void>;
          }
          | {
            readonly _tag: "Active";
            readonly ownerId: OwnerId;
            readonly metadata: Metadata;
            readonly executionId: string;
            readonly interrupt: Deferred.Deferred<void>;
          },
      ) =>
        Ref.update(state, (current) => ({
          ...current,
          runs: HashMap.set(current.runs, runId, run),
        }));

      const removeRun = (ownerId: OwnerId, runId: RunId) =>
        Ref.update(state, (current) => ({
          activeOwners: Option.match(HashMap.get(current.activeOwners, ownerId), {
            onNone: () => current.activeOwners,
            onSome: (activeRunId) =>
              activeRunId === runId
                ? HashMap.remove(current.activeOwners, ownerId)
                : current.activeOwners,
          }),
          runs: HashMap.remove(current.runs, runId),
        }));

      const lookupRun = (runId: RunId) =>
        Ref.get(state).pipe(
          Effect.map((current) => HashMap.get(current.runs, runId)),
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.fail(options.missingRun(runId)),
              onSome: Effect.succeed,
            }),
          ),
        );

      const lookupActiveRun = (runId: RunId) =>
        lookupRun(runId).pipe(
          Effect.flatMap((run) =>
            run._tag === "Active" ? Effect.succeed(run) : Effect.fail(options.missingRun(runId))
          ),
        );

      const cleanupRun = (args: {
        readonly ownerId: OwnerId;
        readonly runId: RunId;
        readonly ownerRef?: SubscriptionRef.SubscriptionRef<RunId | null>;
      }) =>
        Effect.gen(function*() {
          if (args.ownerRef) {
            yield* SubscriptionRef.modify(args.ownerRef, (current) => [
              undefined,
              current === args.runId ? null : current,
            ]);
          }
          yield* removeRun(args.ownerId, args.runId);
          yield* RcMap.invalidate(eventChannels, args.runId);
        });

      yield* workflowEngine.register(
        options.workflow,
        Effect.fnUntraced(function*(payload) {
          const runId = options.runId(payload);
          const run = yield* lookupActiveRun(runId).pipe(
            Effect.catch((error) => Effect.die(error)),
          );
          const mailbox = yield* RcMap.get(eventChannels, runId);
          const ownerRef = yield* RcMap.get(ownerChanges, run.ownerId);
          const workflow = yield* WorkflowInstance;

          yield* SubscriptionRef.set(ownerRef, runId);

          const exit = yield* Effect.gen(function*() {
            const runFiber = yield* options.run({ payload, metadata: run.metadata, mailbox }).pipe(
              Effect.forkScoped,
            );

            yield* Deferred.await(run.interrupt).pipe(
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

          yield* options.finalize({ payload, metadata: run.metadata, exit }).pipe(
            Effect.ensuring(cleanupRun({ ownerId: run.ownerId, runId, ownerRef })),
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
          const run = yield* lookupActiveRun(runId);
          const events = Stream.unwrap(
            RcMap.get(eventChannels, runId).pipe(
              Effect.map((mailbox) => Stream.fromPubSubTake(mailbox)),
            ),
          );
          return {
            ownerId: run.ownerId,
            metadata: run.metadata,
            events,
          } as const;
        }),

        start: Effect.fnUntraced(function*(payload: Payload["Type"]) {
          const ownerId = options.ownerId(payload);
          const runId = options.runId(payload);

          return yield* Effect.uninterruptible(
            Effect.gen(function*() {
              const reserved = yield* reserveOwner(ownerId, runId);
              if (!reserved) {
                return yield* Effect.fail(options.busy(ownerId));
              }

              const interrupt = yield* Deferred.make<void>();
              yield* storeRun(runId, { _tag: "Preparing", ownerId, interrupt });

              const metadataExit = yield* Effect.exit(options.prepare(payload));
              if (Exit.isFailure(metadataExit)) {
                yield* cleanupRun({ ownerId, runId });
                return yield* Effect.failCause(metadataExit.cause);
              }

              const activeRun = {
                _tag: "Active" as const,
                ownerId,
                metadata: metadataExit.value,
                executionId: yield* options.workflow.executionId(payload),
                interrupt,
              };
              yield* storeRun(runId, activeRun);

              const launchExit = yield* Effect.exit(
                options.workflow.execute(payload, { discard: true }).pipe(
                  Effect.provideService(WorkflowEngine.WorkflowEngine, workflowEngine),
                ),
              );
              if (Exit.isFailure(launchExit)) {
                yield* options.finalize({
                  payload,
                  metadata: activeRun.metadata,
                  exit: Exit.failCause(launchExit.cause),
                }).pipe(Effect.ensuring(cleanupRun({ ownerId, runId })));
                return yield* Effect.failCause(launchExit.cause);
              }

              return { runId } as const;
            }),
          );
        }),

        interrupt: Effect.fnUntraced(function*(ownerId: OwnerId) {
          const current = yield* Ref.get(state);
          const activeRun = HashMap.get(current.activeOwners, ownerId);
          if (Option.isNone(activeRun)) {
            return;
          }

          const run = HashMap.get(current.runs, activeRun.value);
          if (Option.isNone(run)) {
            return;
          }

          yield* Deferred.succeed(run.value.interrupt, undefined);
          if (run.value._tag === "Active") {
            yield* workflowEngine.interrupt(options.workflow, run.value.executionId);
          }
        }),
      } as const;
    });
}
