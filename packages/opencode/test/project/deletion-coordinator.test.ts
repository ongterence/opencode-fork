import { afterEach, describe, expect } from "bun:test"
import { ProjectDeletionJobTable, ProjectDeletionShareTable } from "@opencode-ai/core/project/deletion.sql"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { ProjectV2 } from "@opencode-ai/core/project"
import { EventV2 } from "@opencode-ai/core/event"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { eq } from "drizzle-orm"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { resetDatabase } from "../fixture/db"
import { testEffect } from "../lib/effect"
import { make, ProjectDeletingError } from "../../src/project/deletion-coordinator"

afterEach(resetDatabase)

const it = testEffect(Layer.mergeAll(AppNodeBuilder.build(Database.node)))

const projectID = ProjectV2.ID.make("proj_deletion_coordinator")

function seedProject() {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({
        id: projectID,
        worktree: AbsolutePath.make("C:\\repo\\coordinator"),
        vcs: "git",
        sandboxes: [],
        time_created: 1,
        time_updated: 1,
      })
      .run()
      .pipe(Effect.orDie)
  })
}

describe("project deletion coordinator", () => {
  it.live("installs one owner, fences mutations, and executes one purge", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const calls = { revoke: 0, cleanup: 0, publish: 0 }
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () =>
          Effect.gen(function* () {
            calls.revoke += 1
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
          }),
        cleanup: () => Effect.sync(() => (calls.cleanup += 1)),
        publish: () => Effect.sync(() => (calls.publish += 1)),
      })

      expect(yield* coordinator.begin(projectID)).toBe("owner")
      const owner = yield* coordinator.execute(projectID).pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(entered)

      expect(yield* coordinator.ownerCount()).toBe(1)
      expect(yield* coordinator.begin(projectID)).toBe("in_progress")
      expect(yield* coordinator.runOwned(projectID)).toEqual({ status: "in_progress", phase: "revoking_shares" })
      expect(yield* coordinator.ownerCount()).toBe(1)
      const fenced = yield* coordinator.assertWritable(projectID).pipe(Effect.flip)
      expect(fenced).toBeInstanceOf(ProjectDeletingError)
      const lateID = ProjectV2.ID.make("proj_late_mutation")
      yield* coordinator.assertWritable(projectID).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            yield* db
              .insert(ProjectTable)
              .values({
                id: lateID,
                worktree: AbsolutePath.make("C:\\repo\\late"),
                vcs: "git",
                sandboxes: [],
                time_created: 1,
                time_updated: 1,
              })
              .run()
              .pipe(Effect.orDie)
          }),
        ),
        Effect.ignore,
      )
      const { db } = yield* Database.Service
      expect(
        yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, lateID)).get().pipe(Effect.orDie),
      ).toBeUndefined()

      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(owner)).toEqual({ status: "completed" })
      expect(calls).toEqual({ revoke: 1, cleanup: 1, publish: 1 })
      expect(yield* coordinator.ownerCount()).toBe(0)

      expect(yield* db.select().from(ProjectDeletionJobTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.live("waits for admitted leases to drain before cleanup", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const entered = [yield* Deferred.make<void>(), yield* Deferred.make<void>()]
      const cancelled = [yield* Deferred.make<void>(), yield* Deferred.make<void>()]
      const release = [yield* Deferred.make<void>(), yield* Deferred.make<void>()]
      const cleaned = yield* Deferred.make<void>()
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () => Effect.void,
        cleanup: () => Deferred.succeed(cleaned, undefined),
        publish: () => Effect.void,
      })
      const scope = yield* Effect.scope
      const leases = yield* Effect.forEach([0, 1], (index) =>
        coordinator
          .withLease(
            projectID,
            () => Deferred.succeed(cancelled[index]!, undefined),
            Effect.gen(function* () {
              yield* Deferred.succeed(entered[index]!, undefined)
              yield* Deferred.await(release[index]!)
            }),
          )
          .pipe(Effect.forkIn(scope)),
      )
      yield* Effect.forEach(entered, Deferred.await, { discard: true })

      expect(yield* coordinator.begin(projectID)).toBe("owner")
      const owner = yield* coordinator.execute(projectID).pipe(Effect.forkIn(scope))
      yield* Effect.forEach(cancelled, Deferred.await, { discard: true })
      expect(yield* Deferred.isDone(cleaned)).toBe(false)

      yield* Effect.forEach(release, (item) => Deferred.succeed(item, undefined), { discard: true })
      yield* Effect.forEach(leases, Fiber.join, { discard: true })
      expect(yield* Fiber.join(owner)).toEqual({ status: "completed" })
      expect(yield* Deferred.isDone(cleaned)).toBe(true)
    }),
  )

  it.live("serializes a mutation commit with deletion ownership and rejects a late commit", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const coordinator = yield* make()
      const { db } = yield* Database.Service
      const scope = yield* Effect.scope
      const mutation = yield* coordinator
        .withMutation(
          projectID,
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
            yield* db
              .update(ProjectTable)
              .set({ name: "committed-before-snapshot" })
              .where(eq(ProjectTable.id, projectID))
              .run()
              .pipe(Effect.orDie)
          }),
        )
        .pipe(Effect.forkIn(scope))
      yield* Deferred.await(entered)

      const begin = coordinator.begin(projectID)
      const owner = yield* begin.pipe(Effect.forkIn(scope))
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(mutation)
      expect(yield* Fiber.join(owner)).toBe("owner")

      const late = yield* coordinator
        .withMutation(
          projectID,
          db.update(ProjectTable).set({ name: "late" }).where(eq(ProjectTable.id, projectID)).run().pipe(Effect.orDie),
        )
        .pipe(Effect.flip)
      expect(late).toBeInstanceOf(ProjectDeletingError)
      expect(
        (yield* db
          .select({ name: ProjectTable.name })
          .from(ProjectTable)
          .where(eq(ProjectTable.id, projectID))
          .get()
          .pipe(Effect.orDie))?.name,
      ).toBe("committed-before-snapshot")
    }),
  )

  it.live("releases a unique lease when its owner is interrupted", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const entered = yield* Deferred.make<void>()
      const coordinator = yield* make()
      coordinator.install({ revokeShares: () => Effect.void, cleanup: () => Effect.void, publish: () => Effect.void })
      const fiber = yield* coordinator
        .withLease(
          projectID,
          () => Effect.void,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        )
        .pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(fiber)

      expect(yield* coordinator.begin(projectID)).toBe("owner")
      expect(yield* coordinator.execute(projectID)).toEqual({ status: "completed" })
    }),
  )

  it.live("installs deletion owner finalization atomically with begin", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const entered = yield* Deferred.make<void>()
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        cleanup: () => Effect.void,
        publish: () => Effect.void,
      })

      const owner = yield* coordinator.runOwned(projectID).pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(entered)
      expect(yield* coordinator.ownerCount()).toBe(1)
      yield* Fiber.interrupt(owner)
      expect(yield* coordinator.ownerCount()).toBe(0)
    }),
  )

  it.live("recovers each durable phase with the correct next action", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const phases = ["revoking_shares", "quiescing", "cleanup_complete", "published"] as const

      for (const [index, phase] of phases.entries()) {
        const id = ProjectV2.ID.make(`proj_recover_${index}`)
        yield* db
          .insert(ProjectDeletionJobTable)
          .values({ project_id: id, phase, attempt: 0, last_error: null, created_at: 1, updated_at: 1 })
          .run()
          .pipe(Effect.orDie)
      }

      const calls: string[] = []
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: (id) => Effect.sync(() => calls.push(`revoke:${id}`)),
        cleanup: (id) => Effect.sync(() => calls.push(`cleanup:${id}`)),
        publish: (id) => Effect.sync(() => calls.push(`publish:${id}`)),
      })
      yield* coordinator.recover()

      expect(calls).toEqual([
        "revoke:proj_recover_0",
        "cleanup:proj_recover_0",
        "publish:proj_recover_0",
        "cleanup:proj_recover_1",
        "publish:proj_recover_1",
        "publish:proj_recover_2",
      ])
      expect(yield* db.select().from(ProjectDeletionJobTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.live("retries local cleanup after the project row was already deleted", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectDeletionJobTable)
        .values({
          project_id: projectID,
          phase: "cleaning",
          attempt: 1,
          last_error: null,
          created_at: 1,
          updated_at: 1,
        })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(ProjectDeletionShareTable)
        .values({
          project_id: projectID,
          session_id: "ses_snapshot",
          share_id: "share_snapshot",
          secret: "secret",
          base_url: "https://example.test",
          status: "revoked",
          attempt: 1,
          last_error: null,
          created_at: 1,
          updated_at: 1,
        })
        .run()
        .pipe(Effect.orDie)

      const calls: string[] = []
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () => Effect.die("revocation must not repeat"),
        cleanup: () => Effect.sync(() => calls.push("snapshot removed")),
        publish: () => Effect.sync(() => calls.push("published")),
      })
      yield* coordinator.recover()

      expect(calls).toEqual(["snapshot removed", "published"])
      expect(yield* db.select().from(ProjectDeletionShareTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.live("does not publish or finish when mandatory cleanup fails", () =>
    Effect.gen(function* () {
      yield* seedProject()
      let published = 0
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () => Effect.void,
        cleanup: () => Effect.fail(new Error("snapshot removal failed")),
        publish: () => Effect.sync(() => (published += 1)),
      })
      expect(yield* coordinator.begin(projectID)).toBe("owner")
      expect(yield* coordinator.execute(projectID)).toEqual({ status: "in_progress", phase: "cleaning" })

      const { db } = yield* Database.Service
      const job = yield* db
        .select()
        .from(ProjectDeletionJobTable)
        .where(eq(ProjectDeletionJobTable.project_id, projectID))
        .get()
        .pipe(Effect.orDie)
      expect(job?.phase).toBe("cleaning")
      expect(job?.last_error).toContain("snapshot removal failed")
      expect(published).toBe(0)
    }),
  )

  it.live("retries a failed local recovery phase with bounded backoff", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectDeletionJobTable)
        .values({
          project_id: projectID,
          phase: "cleaning",
          attempt: 0,
          last_error: null,
          created_at: 1,
          updated_at: 1,
        })
        .run()
        .pipe(Effect.orDie)
      let cleanup = 0
      let publish = 0
      const coordinator = yield* make({ recoveryAttempts: 3, recoveryDelay: "1 millis" })
      coordinator.install({
        revokeShares: () => Effect.die("must not revoke"),
        cleanup: () =>
          Effect.suspend(() => (++cleanup < 3 ? Effect.fail(new Error("transient cleanup")) : Effect.void)),
        publish: () => Effect.sync(() => (publish += 1)),
      })
      yield* coordinator.recover()

      expect(cleanup).toBe(3)
      expect(publish).toBe(1)
      expect(yield* db.select().from(ProjectDeletionJobTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.live("recovers an idempotent durable publish crash before the journal marker without emitting twice", () =>
    Effect.gen(function* () {
      yield* seedProject()
      let emitted = 0
      let fail = true
      const durable = new Set<string>()
      const first = yield* make({
        afterPublish: () =>
          Effect.suspend(() => (fail ? ((fail = false), Effect.fail(new Error("crash after publish"))) : Effect.void)),
      })
      const actions = {
        revokeShares: () => Effect.void,
        cleanup: () => Effect.void,
        publish: (_projectID: ProjectV2.ID, eventID: EventV2.ID) =>
          Effect.sync(() => {
            if (durable.has(eventID)) return
            durable.add(eventID)
            emitted += 1
          }),
      }
      first.install(actions)
      expect(yield* first.begin(projectID)).toBe("owner")
      expect(yield* first.execute(projectID)).toEqual({ status: "in_progress", phase: "cleanup_complete" })
      expect(emitted).toBe(1)

      const recovered = yield* make()
      recovered.install(actions)
      yield* recovered.recover()
      expect(emitted).toBe(1)
      expect(durable.size).toBe(1)
    }),
  )

  it.live("cleans interrupted ownership and bounds shutdown drain", () =>
    Effect.gen(function* () {
      yield* seedProject()
      const { db } = yield* Database.Service
      const entered = yield* Deferred.make<void>()
      const coordinator = yield* make({ shutdownTimeout: "20 millis" })
      coordinator.install({
        revokeShares: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        cleanup: () => Effect.void,
        publish: () => Effect.void,
      })
      expect(yield* coordinator.begin(projectID)).toBe("owner")
      const owner = yield* coordinator.execute(projectID).pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(owner)
      expect(yield* coordinator.ownerCount()).toBe(0)
      yield* coordinator.prepareShutdown()

      const afterClose = ProjectV2.ID.make("proj_after_shutdown")
      yield* db
        .insert(ProjectTable)
        .values({
          id: afterClose,
          worktree: AbsolutePath.make("C:\\repo\\after-shutdown"),
          vcs: "git",
          sandboxes: [],
          time_created: 1,
          time_updated: 1,
        })
        .run()
        .pipe(Effect.orDie)
      expect(yield* coordinator.begin(afterClose)).toBe("in_progress")
      expect(yield* coordinator.ownerCount()).toBe(0)
      yield* db
        .insert(ProjectDeletionJobTable)
        .values({
          project_id: afterClose,
          phase: "cleaning",
          attempt: 0,
          last_error: null,
          created_at: 1,
          updated_at: 1,
        })
        .run()
        .pipe(Effect.orDie)
      yield* coordinator.recover()
      expect(yield* coordinator.ownerCount()).toBe(0)
      expect(
        yield* db
          .select()
          .from(ProjectDeletionJobTable)
          .where(eq(ProjectDeletionJobTable.project_id, afterClose))
          .get()
          .pipe(Effect.orDie),
      ).toBeDefined()

      const stuck = yield* make({ shutdownTimeout: "20 millis" })
      expect(yield* stuck.begin(projectID)).toBe("in_progress")
      // Use a second project to create an owner that never reaches execute.
      const orphan = ProjectV2.ID.make("proj_shutdown_orphan")
      yield* db
        .insert(ProjectTable)
        .values({
          id: orphan,
          worktree: AbsolutePath.make("C:\\repo\\orphan"),
          vcs: "git",
          sandboxes: [],
          time_created: 1,
          time_updated: 1,
        })
        .run()
        .pipe(Effect.orDie)
      expect(yield* stuck.begin(orphan)).toBe("owner")
      expect((yield* stuck.prepareShutdown().pipe(Effect.flip))._tag).toBe("DeletionBusyError")
    }),
  )
})
