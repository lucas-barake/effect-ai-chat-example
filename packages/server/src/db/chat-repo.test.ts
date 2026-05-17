import * as Chat from "@app/domain/api/chat-rpc";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PgMigrator from "@effect/sql-pg/PgMigrator";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Migrator from "effect/unstable/sql/Migrator";
import { ChatRepo } from "./chat-repo.js";
import { PgTest, withTransactionRollback } from "./pg-test.js";

const TestMigrationLayer = PgMigrator.layer({
  loader: Migrator.fromGlob(import.meta.glob("./migrations/*.ts")),
}).pipe(
  Layer.provide(NodeServices.layer),
  Layer.orDie,
);

const TestLive = Layer.mergeAll(
  Layer.effect(ChatRepo, ChatRepo.make),
).pipe(
  Layer.provideMerge(TestMigrationLayer),
  Layer.provideMerge(PgTest),
);

describe("ChatRepo", () => {
  it.layer(TestLive, { timeout: "30 seconds" })("integration", (it) => {
    it.effect("create inserts chat and returns it with generated id, createdAt, updatedAt", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const chat = yield* repo.create({
            userId: "user-1",
            title: "My Chat",
            model: "haiku-4.5",
          });
          expect(chat.title).toBe("My Chat");
          expect(chat.model).toBe("haiku-4.5");
          expect(chat.userId).toBe("user-1");
          expect(chat.id).toBeDefined();
          expect(chat.createdAt).toBeDefined();
          expect(chat.updatedAt).toBeDefined();
          expect(chat.messages).toEqual([]);
        }),
      ));

    it.effect("findById returns chat when id and userId match", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const created = yield* repo.create({
            userId: "user-1",
            title: "Find Me",
            model: "haiku-4.5",
          });
          const found = yield* repo.findById(created.id, "user-1");
          expect(found.id).toBe(created.id);
          expect(found.title).toBe("Find Me");
        }),
      ));

    it.effect("findById fails with ChatNotFoundError when userId does not match", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const created = yield* repo.create({
            userId: "user-1",
            title: "Secret",
            model: "haiku-4.5",
          });
          const exit = yield* repo.findById(created.id, "user-2").pipe(Effect.exit);
          expect(exit._tag).toBe("Failure");
        }),
      ));

    it.effect("findById fails with ChatNotFoundError for non-existent id", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const fakeId = Chat.ChatId.make("00000000-0000-4000-8000-000000000099");
          const exit = yield* repo.findById(fakeId, "user-1").pipe(Effect.exit);
          expect(exit._tag).toBe("Failure");
        }),
      ));

    it.effect("listByUser returns chats ordered by updatedAt desc", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          yield* repo.create({ userId: "user-1", title: "Chat A", model: "haiku-4.5" });
          yield* repo.create({ userId: "user-1", title: "Chat B", model: "haiku-4.5" });
          yield* repo.create({ userId: "user-2", title: "Other User", model: "haiku-4.5" });

          const result = yield* repo.listByUser("user-1", Option.none());
          expect(result.items).toHaveLength(2);
          expect(result.hasMore).toBe(false);
          const titles = result.items.map((c) => c.title);
          expect(titles).toContain("Chat A");
          expect(titles).toContain("Chat B");
        }),
      ));

    it.effect("listByUser pagination: cursor excludes items at or after it", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          yield* repo.create({ userId: "user-1", title: "Old", model: "haiku-4.5" });
          const newer = yield* repo.create({ userId: "user-1", title: "New", model: "haiku-4.5" });

          const result = yield* repo.listByUser("user-1", Option.some(newer.updatedAt));
          expect(result.items.every((c) => c.title !== "New")).toBe(true);
        }),
      ));

    it.effect("delete removes chat", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const chat = yield* repo.create({
            userId: "user-1",
            title: "Delete Me",
            model: "haiku-4.5",
          });
          yield* repo.delete(chat.id, "user-1");
          const exit = yield* repo.findById(chat.id, "user-1").pipe(Effect.exit);
          expect(exit._tag).toBe("Failure");
        }),
      ));

    it.effect("delete fails with ChatNotFoundError for wrong userId", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const chat = yield* repo.create({ userId: "user-1", title: "Mine", model: "haiku-4.5" });
          const exit = yield* repo.delete(chat.id, "user-2").pipe(Effect.exit);
          expect(exit._tag).toBe("Failure");
        }),
      ));

    it.effect("updateMessages persists new messages array", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const chat = yield* repo.create({
            userId: "user-1",
            title: "Msg Test",
            model: "haiku-4.5",
          });

          yield* repo.updateMessages({
            chatId: chat.id,
            userId: "user-1",
            messages: [
              { role: "user", content: "Hello" },
              { role: "assistant", content: "Hi there" },
            ],
          });

          const updated = yield* repo.findById(chat.id, "user-1");
          expect(updated.messages).toHaveLength(2);
          expect(updated.messages[0]).toEqual({ role: "user", content: "Hello" });
          expect(updated.messages[1]).toEqual({ role: "assistant", content: "Hi there" });
        }),
      ));

    it.effect("updateMessages round-trips tool-call and tool-result entries", () =>
      withTransactionRollback(
        Effect.gen(function*() {
          const repo = yield* ChatRepo;
          const chat = yield* repo.create({ userId: "user-1", title: "Tools", model: "haiku-4.5" });

          const messages: ReadonlyArray<typeof Chat.Message.Type> = [
            { role: "user", content: "What time is it?" },
            {
              role: "assistant",
              content: [
                { type: "text", text: "Let me check" },
                { type: "tool-call", id: "c1", name: "getCurrentDateTime", params: {} },
              ],
            },
            {
              role: "tool",
              content: [{
                type: "tool-result",
                id: "c1",
                name: "getCurrentDateTime",
                result: "2024-01-01T00:00:00Z",
                isFailure: false,
              }],
            },
            { role: "assistant", content: "It is midnight on January 1st, 2024." },
          ];

          yield* repo.updateMessages({ chatId: chat.id, userId: "user-1", messages });

          const updated = yield* repo.findById(chat.id, "user-1");
          expect(updated.messages).toHaveLength(4);
          expect(updated.messages[1]!.role).toBe("assistant");
          expect(Array.isArray(updated.messages[1]!.content)).toBe(true);
          const parts = updated.messages[1]!.content as ReadonlyArray<Chat.MessagePart>;
          expect(parts[1]!.type).toBe("tool-call");
          expect(updated.messages[2]!.role).toBe("tool");
        }),
      ));
  });
});
