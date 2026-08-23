import { afterEach, describe, expect } from "bun:test"
import { ProjectDeletionJobTable, ProjectDeletionShareTable } from "@opencode-ai/core/project/deletion.sql"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { ProjectV2 } from "@opencode-ai/core/project"
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
        cleanup: () => Effect.sync(() => calls.cleanup += 1),
        publish: () => Effect.sync(() => calls.publish += 1),
      })

      expect(yield* coordinator.begin(projectID)).toBe("owner")
      const owner = yield* coordinator.execute(projectID).pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(entered)

      expect(yield* coordinator.ownerCount()).toBe(1)
      expect(yield* coordinator.begin(projectID)).toBe("in_progress")
      const fenced = yield* coordinator.assertWritable(projectID).pipe(Effect.flip)
      expect(fenced).toBeInstanceOf(ProjectDeletingError)
      const lateID = ProjectV2.ID.make("proj_late_mutation")
      yield* coordinator
        .assertWritable(projectID)
        .pipe(
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
      expect(yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, lateID)).get().pipe(Effect.orDie)).toBeUndefined()

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
      const cancelled = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const cleaned = yield* Deferred.make<void>()
      const coordinator = yield* make()
      coordinator.install({
        revokeShares: () => Effect.void,
        cleanup: () => Deferred.succeed(cleaned, undefined),
        publish: () => Effect.void,
      })
      const lease = yield* coordinator.lease(projectID, "session:ses_active", () =>
        Deferred.succeed(cancelled, undefined),
      )

      expect(yield* coordinator.begin(projectID)).toBe("owner")
      const owner = yield* coordinator.execute(projectID).pipe(Effect.forkIn(yield* Effect.scope))
      yield* Deferred.await(cancelled)
      expect(yield* Deferred.isDone(cleaned)).toBe(false)

      yield* Deferred.succeed(release, undefined)
      yield* lease
      expect(yield* Fiber.join(owner)).toEqual({ status: "completed" })
      expect(yield* Deferred.isDone(cleaned)).toBe(true)
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
})
