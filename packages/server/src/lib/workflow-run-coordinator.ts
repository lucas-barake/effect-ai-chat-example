import type * as Cause from "effect/Cause";
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
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Take from "effect/Take";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";

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
    readonly prepare: (
      payload: Payload["Type"],
    ) => Effect.Effect<Metadata, BusyError>;
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
      const coordinatorScope = yield* Effect.scope;

      const state = yield* Ref.make({
        activeOwners: HashMap.empty<OwnerId, RunId>(),
        runIds: new Set<RunId>(),
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
        lookup: () =>
          PubSub.unbounded<Take.Take<Event, Error["Type"]>>({
            replay: Infinity,
          }),
      });
      const resources = new Map<
        RunId,
        {
          readonly mailbox: PubSub.PubSub<Take.Take<Event, Error["Type"]>>;
          readonly ownerRef: SubscriptionRef.SubscriptionRef<RunId | null>;
          readonly completion: Ref.Ref<"Open" | "Completing" | "Done">;
        }
      >();

      const reserveRun = (
        ownerId: OwnerId,
        runId: RunId,
        interrupt: Deferred.Deferred<void>,
      ) =>
        Ref.modify(state, (current) => {
          if (HashMap.has(current.activeOwners, ownerId) || current.runIds.has(runId)) {
            return [false, current] as const;
          }

          const runIds = new Set(current.runIds);
          runIds.add(runId);
          return [
            true,
            {
              ...current,
              activeOwners: HashMap.set(current.activeOwners, ownerId, runId),
              runIds,
              runs: HashMap.set(current.runs, runId, { _tag: "Preparing", ownerId, interrupt }),
            },
          ] as const;
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
          ...current,
          activeOwners: Option.match(
            HashMap.get(current.activeOwners, ownerId),
            {
              onNone: () => current.activeOwners,
              onSome: (activeRunId) =>
                activeRunId === runId
                  ? HashMap.remove(current.activeOwners, ownerId)
                  : current.activeOwners,
            },
          ),
          runs: HashMap.remove(current.runs, runId),
        }));

      const lookupRun = (runId: RunId) =>
        Ref.get(state).pipe(
          Effect.map((current) => HashMap.get(current.runs, runId)),
          Effect.flatMap(Effect.fromOption),
          Effect.mapError(() => options.missingRun(runId)),
        );

      const lookupActiveRun = (runId: RunId) =>
        lookupRun(runId).pipe(
          Effect.filterOrFail(
            (run) => run._tag === "Active",
            () => options.missingRun(runId),
          ),
        );

      const cleanupRun = (args: {
        readonly ownerId: OwnerId;
        readonly runId: RunId;
        readonly ownerRef?: SubscriptionRef.SubscriptionRef<RunId | null>;
      }) =>
        Effect.gen(function*() {
          if (args.ownerRef) {
            yield* SubscriptionRef.updateSome(args.ownerRef, (current) =>
              current === args.runId
                ? Option.some<RunId | null>(null)
                : Option.none());
          }
          yield* removeRun(args.ownerId, args.runId);
          resources.delete(args.runId);
          yield* RcMap.invalidate(eventChannels, args.runId);
        });

      const completeRun = (args: {
        readonly payload: Payload["Type"];
        readonly ownerId: OwnerId;
        readonly runId: RunId;
        readonly metadata: Metadata;
        readonly mailbox: PubSub.PubSub<Take.Take<Event, Error["Type"]>>;
        readonly ownerRef: SubscriptionRef.SubscriptionRef<RunId | null>;
        readonly completion: Ref.Ref<"Open" | "Completing" | "Done">;
        readonly exit: Exit.Exit<Success["Type"], Error["Type"]>;
      }) => {
        const finalize = (remaining: number): Effect.Effect<void> =>
          Effect.exit(options.finalize({
            payload: args.payload,
            metadata: args.metadata,
            exit: args.exit,
          })).pipe(
            Effect.flatMap((exit) => {
              if (Exit.isSuccess(exit)) {
                return Effect.void;
              }
              return remaining > 0
                ? Effect.yieldNow.pipe(Effect.andThen(finalize(remaining - 1)))
                : Effect.failCause(exit.cause);
            }),
          );

        return Ref.modify(args.completion, (current) =>
          current === "Open" ? [true, "Completing"] as const : [false, current] as const)
          .pipe(
            Effect.flatMap((shouldComplete) =>
              shouldComplete
                ? finalize(3).pipe(
                  Effect.andThen(PubSub.publish(args.mailbox, Exit.asVoid(args.exit))),
                  Effect.andThen(
                    cleanupRun({
                      ownerId: args.ownerId,
                      runId: args.runId,
                      ownerRef: args.ownerRef,
                    }),
                  ),
                  Effect.onExit((exit) =>
                    Ref.set(args.completion, Exit.isFailure(exit) ? "Open" : "Done")
                  ),
                )
                : Effect.void
            ),
          );
      };

      yield* workflowEngine.register(
        options.workflow,
        Effect.fnUntraced(function*(payload) {
          const runId = options.runId(payload);
          const run = yield* lookupActiveRun(runId).pipe(
            Effect.catch((error) =>
              Effect.die(error)
            ),
          );
          const resource = resources.get(runId);
          if (resource === undefined) {
            return yield* Effect.die(options.missingRun(runId));
          }
          const workflow = yield* WorkflowEngine.WorkflowInstance;

          yield* Effect.addFinalizer((exit) =>
            Exit.isFailure(exit)
              ? completeRun({
                payload,
                ownerId: run.ownerId,
                runId,
                metadata: run.metadata,
                mailbox: resource.mailbox,
                ownerRef: resource.ownerRef,
                completion: resource.completion,
                exit: Exit.failCause(exit.cause as Cause.Cause<Error["Type"]>),
              })
              : Effect.void
          );

          yield* SubscriptionRef.set(resource.ownerRef, runId);

          const runExit = yield* Effect.exit(Effect.gen(function*() {
            if (yield* Deferred.isDone(run.interrupt)) {
              workflow.interrupted = true;
              return yield* Effect.interrupt;
            }

            const runFiber = yield* options
              .run({ payload, metadata: run.metadata, mailbox: resource.mailbox })
              .pipe(Effect.forkScoped);

            yield* Deferred.await(run.interrupt).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  workflow.interrupted = true;
                }),
              ),
              Effect.andThen(Fiber.interrupt(runFiber)),
              Effect.forkScoped,
            );

            return yield* Fiber.await(runFiber);
          }));
          const exit = Exit.match(runExit, {
            onFailure: Exit.failCause,
            onSuccess: (exit) => exit,
          });

          yield* completeRun({
            payload,
            ownerId: run.ownerId,
            runId,
            metadata: run.metadata,
            mailbox: resource.mailbox,
            ownerRef: resource.ownerRef,
            completion: resource.completion,
            exit,
          });
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
          const mailbox = yield* RcMap.get(eventChannels, runId);
          return {
            ownerId: run.ownerId,
            metadata: run.metadata,
            events: Stream.unwrap(
              Effect.succeed(Stream.fromPubSubTake(mailbox)),
            ),
          } as const;
        }),

        start: Effect.fnUntraced(function*(payload: Payload["Type"]) {
          const ownerId = options.ownerId(payload);
          const runId = options.runId(payload);

          return yield* Effect.uninterruptible(
            Effect.gen(function*() {
              const interrupt = yield* Deferred.make<void>();
              const reserved = yield* reserveRun(ownerId, runId, interrupt);
              if (!reserved) {
                return yield* Effect.fail(options.busy(ownerId));
              }

              const metadataExit = yield* Effect.exit(
                Effect.interruptible(options.prepare(payload)),
              );
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
              const resource = {
                mailbox: yield* Scope.provide(RcMap.get(eventChannels, runId), coordinatorScope),
                ownerRef: yield* Scope.provide(RcMap.get(ownerChanges, ownerId), coordinatorScope),
                completion: yield* Ref.make<"Open" | "Completing" | "Done">("Open"),
              };
              resources.set(runId, resource);
              yield* storeRun(runId, activeRun);
              yield* Scope.addFinalizer(
                coordinatorScope,
                Effect.gen(function*() {
                  yield* Deferred.succeed(interrupt, undefined);
                  const interruptExit = yield* Effect.exit(Effect.interrupt);
                  yield* completeRun({
                    payload,
                    ownerId,
                    runId,
                    metadata: activeRun.metadata,
                    mailbox: resource.mailbox,
                    ownerRef: resource.ownerRef,
                    completion: resource.completion,
                    exit: interruptExit as Exit.Exit<Success["Type"], Error["Type"]>,
                  });
                }),
              );

              const launchExit = yield* Effect.exit(
                options.workflow
                  .execute(payload, { discard: true })
                  .pipe(
                    Effect.provideService(
                      WorkflowEngine.WorkflowEngine,
                      workflowEngine,
                    ),
                  ),
              );
              if (Exit.isFailure(launchExit)) {
                yield* options
                  .finalize({
                    payload,
                    metadata: activeRun.metadata,
                    exit: Exit.failCause(launchExit.cause),
                  })
                  .pipe(Effect.ensuring(cleanupRun({ ownerId, runId })));
                return yield* Effect.failCause(launchExit.cause);
              }

              return { runId } as const;
            }),
          );
        }),

        interrupt: Effect.fnUntraced(function*(ownerId: OwnerId) {
          const current = yield* Ref.get(state);
          const run = HashMap.get(current.activeOwners, ownerId).pipe(
            Option.flatMap((runId) => HashMap.get(current.runs, runId)),
          );
          if (Option.isNone(run)) {
            return;
          }

          yield* Deferred.succeed(run.value.interrupt, undefined);
          if (run.value._tag === "Active") {
            yield* workflowEngine.interrupt(
              options.workflow,
              run.value.executionId,
            );
          }
        }),
      } as const;
    });
}
