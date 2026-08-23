import { describe, expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import projectDeletionJobMigration from "@opencode-ai/core/database/migration/20260823_project_deletion_job"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()

describe("project deletion job migration", () => {
  test("creates the durable journal exactly once with recovery indexes", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb

        yield* DatabaseMigration.applyOnly(db, [projectDeletionJobMigration])
        yield* DatabaseMigration.applyOnly(db, [projectDeletionJobMigration])

        expect(
          yield* db.all(
            sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('project_deletion_job', 'project_deletion_share', 'project_deletion_worktree') ORDER BY name`,
          ),
        ).toEqual([
          { name: "project_deletion_job" },
          { name: "project_deletion_share" },
          { name: "project_deletion_worktree" },
        ])
        expect(
          yield* db.all(
            sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name IN ('project_deletion_job_phase_idx', 'project_deletion_share_status_idx') ORDER BY name`,
          ),
        ).toEqual([
          { name: "project_deletion_job_phase_idx" },
          { name: "project_deletion_share_status_idx" },
        ])
        expect(yield* db.all(sql`SELECT name, pk FROM pragma_table_info('project_deletion_job') WHERE pk > 0 ORDER BY pk`)).toEqual([
          { name: "project_id", pk: 1 },
        ])
        expect(yield* db.get(sql`SELECT name, "notnull" AS "notnull", pk FROM pragma_table_info('project_deletion_job') WHERE name = 'project_id'`)).toEqual({
          name: "project_id",
          notnull: 1,
          pk: 1,
        })
        expect(yield* db.all(sql`SELECT name, pk FROM pragma_table_info('project_deletion_share') WHERE pk > 0 ORDER BY pk`)).toEqual([
          { name: "project_id", pk: 1 },
          { name: "session_id", pk: 2 },
        ])
        expect(
          yield* db.all(sql`SELECT name, pk FROM pragma_table_info('project_deletion_worktree') WHERE pk > 0 ORDER BY pk`),
        ).toEqual([
          { name: "project_id", pk: 1 },
          { name: "canonical_path", pk: 2 },
        ])
      }),
    )
  })

  test("includes the journal in a fresh database schema", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        expect(
          yield* db.all(
            sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('project_deletion_job', 'project_deletion_share', 'project_deletion_worktree') ORDER BY name`,
          ),
        ).toEqual([
          { name: "project_deletion_job" },
          { name: "project_deletion_share" },
          { name: "project_deletion_worktree" },
        ])
        expect(yield* db.get(sql`SELECT name, "notnull" AS "notnull", pk FROM pragma_table_info('project_deletion_job') WHERE name = 'project_id'`)).toEqual({
          name: "project_id",
          notnull: 1,
          pk: 1,
        })
      }),
    )
  })
})
