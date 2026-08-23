import { afterEach, describe, expect } from "bun:test"
import { $ } from "bun"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectDeletionJobTable, ProjectDeletionWorktreeTable } from "@opencode-ai/core/project/deletion.sql"
import { EventTable } from "@opencode-ai/core/event/sql"
import { Effect, Layer } from "effect"
import { eq } from "drizzle-orm"
import path from "path"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { Snapshot } from "../../src/snapshot"
import { ProjectDeletionCoordinator } from "../../src/project/deletion-coordinator"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, request, requestInDirectory } from "./httpapi-layer"

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const noopBootstrap = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const testInstanceStore = AppNodeBuilder.build(InstanceStore.node, [[InstanceStore.bootstrapNode, noopBootstrap]])

const it = testEffect(
  Layer.mergeAll(
    AppNodeBuilder.build(LayerNode.group([FSUtil.node, Snapshot.node, Database.node])),
    AppNodeBuilder.build(ProjectDeletionCoordinator.node),
    testInstanceStore,
    httpApiLayer,
  ),
)

function collectGlobalEvents() {
  return Effect.acquireRelease(
    Effect.sync(() => {
      const seen: GlobalEvent[] = []
      const on = (event: GlobalEvent) => {
        seen.push(event)
      }
      GlobalBus.on("event", on)
      return { seen, on }
    }),
    ({ on }) => Effect.sync(() => GlobalBus.off("event", on)),
  )
}

describe("global project delete endpoint", () => {
  it.instance(
    "purges opencode data and keeps user files",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const fs = yield* FSUtil.Service
        yield* Effect.promise(() => Bun.write(path.join(tmp.directory, "keep.txt"), "data"))

        // Register the project by hitting a directory-scoped route.
        yield* requestInDirectory("/project/current", tmp.directory)
        const listed = yield* requestInDirectory("/project", tmp.directory)
        expect(listed.status).toBe(200)
        const projects = (yield* listed.json) as Array<{ id: string; worktree: string }>
        const target = projects.find((entry) => entry.worktree === tmp.directory)
        if (!target) throw new Error(`project for ${tmp.directory} was not registered`)

        // Seed an opencode-owned artifact dir under the data root.
        const snapshotDir = path.join(Global.Path.data, "snapshot", target.id)
        yield* fs.ensureDir(snapshotDir)
        expect(yield* fs.exists(snapshotDir)).toBe(true)

        const events = yield* collectGlobalEvents()
        const del = yield* request(`/global/project/${target.id}`, { method: "DELETE" })
        expect(del.status).toBe(204)

        const repeat = yield* request(`/global/project/${target!.id}`, { method: "DELETE" })
        expect(repeat.status).toBe(404)

        // Assert on the database directly: a follow-up directory-scoped request would
        // boot a fresh instance and legitimately re-register the project.
        const { db } = yield* Database.Service
        const rows = yield* db.select({ id: ProjectTable.id }).from(ProjectTable).all().pipe(Effect.orDie)
        expect(rows.find((row) => row.id === target.id)).toBeUndefined()
        expect(yield* fs.exists(snapshotDir)).toBe(false)
        expect(yield* fs.exists(path.join(tmp.directory, "keep.txt"))).toBe(true)
        // Instance disposal runs as part of the purge.
        expect(events.seen.some((event) => event.payload.type === "server.instance.disposed")).toBe(true)
        const deleted = events.seen.find(
          (event) =>
            event.directory === "global" &&
            event.payload.type === "project.deleted" &&
            (event.payload.properties as { id?: string }).id === target.id,
        )
        expect(deleted).toBeDefined()
        const eventID = deleted?.payload.id
        if (!eventID) throw new Error("durable project deletion event did not include an id")
        expect(
          yield* db.select().from(EventTable).where(eq(EventTable.id, eventID)).get().pipe(Effect.orDie),
        ).toBeDefined()

        // Simulate a crash after EventV2 committed and bridged the event but
        // before the deletion journal recorded delivery. Recovery must use the
        // stable event id and must not bridge a second local notification.
        yield* db
          .insert(ProjectDeletionJobTable)
          .values({
            project_id: target.id,
            phase: "cleanup_complete",
            attempt: 0,
            last_error: null,
            event_id: eventID,
            event_delivered_at: null,
            created_at: 1,
            updated_at: 1,
          })
          .run()
          .pipe(Effect.orDie)
        const coordinator = yield* ProjectDeletionCoordinator.Service
        yield* coordinator.recover()
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(1)
      }),
    { git: true },
  )

  it.instance("rejects unknown projects and the global pseudo-project", () =>
    Effect.gen(function* () {
      const missing = yield* request("/global/project/proj_does_not_exist", { method: "DELETE" })
      expect(missing.status).toBe(404)

      const guarded = yield* request("/global/project/global", { method: "DELETE" })
      expect(guarded.status).toBe(400)
    }),
  )

  it.instance(
    "returns conflict while a durable deletion is already in progress",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: string }
        const { db } = yield* Database.Service
        yield* db
          .insert(ProjectDeletionJobTable)
          .values({
            project_id: project.id,
            phase: "revoking_shares",
            attempt: 0,
            last_error: null,
            created_at: 1,
            updated_at: 1,
          })
          .run()
          .pipe(Effect.orDie)

        const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })
        expect(response.status).toBe(409)
        expect(yield* response.json).toEqual({
          _tag: "ProjectDeletionInProgressError",
          projectID: project.id,
          phase: "revoking_shares",
          code: "project_deletion_in_progress",
          message: "Project deletion is in progress",
        })
      }),
    { git: true },
  )

  it.instance(
    "maps fenced project, session, prompt, workspace, and share mutations to 409",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: string }
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        expect(created.status).toBe(200)
        const session = (yield* created.json) as { id: string }

        const { db } = yield* Database.Service
        yield* db
          .insert(ProjectDeletionJobTable)
          .values({
            project_id: project.id,
            phase: "quiescing",
            attempt: 0,
            last_error: null,
            event_id: "evt_fence_routes",
            event_delivered_at: null,
            created_at: 1,
            updated_at: 1,
          })
          .run()
          .pipe(Effect.orDie)

        const requests = [
          requestInDirectory(`/project/${project.id}`, tmp.directory, {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: "late" }),
          }),
          requestInDirectory("/session", tmp.directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
          }),
          requestInDirectory(`/session/${session.id}`, tmp.directory, {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ title: "late" }),
          }),
          requestInDirectory(`/session/${session.id}/message`, tmp.directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              agent: "build",
              model: { providerID: "test", modelID: "test" },
              parts: [{ type: "text", text: "late" }],
            }),
          }),
          requestInDirectory("/experimental/workspace", tmp.directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ type: "missing", branch: null }),
          }),
          requestInDirectory(`/session/${session.id}/share`, tmp.directory, { method: "POST" }),
          requestInDirectory(`/session/${session.id}/command`, tmp.directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ command: "missing", arguments: "" }),
          }),
          requestInDirectory(`/session/${session.id}/shell`, tmp.directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ agent: "build", command: "echo late" }),
          }),
        ]
        const responses = yield* Effect.all(requests, { concurrency: "unbounded" })
        expect(responses.map((response) => response.status)).toEqual([409, 409, 409, 409, 409, 409, 409, 409])
        for (const response of responses) {
          const body = (yield* response.json) as { code?: string }
          expect(body.code).toBe("project_deletion_in_progress")
        }

        const row = (yield* db
          .select({ id: ProjectTable.id, name: ProjectTable.name })
          .from(ProjectTable)
          .all()
          .pipe(Effect.orDie)).find((entry) => entry.id === project.id)
        expect(row?.name).not.toBe("late")
      }),
    { git: true },
  )

  it.instance(
    "retries mandatory worktree cleanup from durable project metadata before terminal success",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const fs = yield* FSUtil.Service
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: string }
        const root = path.join(Global.Path.data, "worktree", project.id)
        const worktree = path.join(root, "retry-locked")
        const artifact = path.join(Global.Path.data, "storage", "project", `${project.id}.json`)
        yield* fs.ensureDir(root)
        yield* Effect.promise(() => $`git worktree add -b deletion-retry ${worktree}`.cwd(tmp.directory).quiet())
        yield* Effect.promise(() => $`git worktree lock ${worktree}`.cwd(tmp.directory).quiet())
        yield* Effect.promise(() => Bun.write(artifact, "must be removed"))

        const events = yield* collectGlobalEvents()
        const first = yield* request(`/global/project/${project.id}`, { method: "DELETE" })
        expect(first.status).toBe(409)
        const { db } = yield* Database.Service
        expect(
          (yield* db.select().from(ProjectTable).all().pipe(Effect.orDie)).some((row) => row.id === project.id),
        ).toBe(true)
        expect(yield* fs.exists(worktree)).toBe(true)
        expect(yield* fs.exists(artifact)).toBe(true)

        yield* Effect.promise(() => $`git worktree unlock ${worktree}`.cwd(tmp.directory).quiet())
        const coordinator = yield* ProjectDeletionCoordinator.Service
        yield* coordinator.recover()

        expect(
          (yield* db.select().from(ProjectTable).all().pipe(Effect.orDie)).some((row) => row.id === project.id),
        ).toBe(false)
        expect(yield* fs.exists(root)).toBe(false)
        expect(yield* fs.exists(artifact)).toBe(false)
        expect(yield* db.select().from(ProjectDeletionJobTable).all().pipe(Effect.orDie)).toEqual([])
        expect(yield* db.select().from(ProjectDeletionWorktreeTable).all().pipe(Effect.orDie)).toEqual([])
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(1)
      }),
    { git: true },
  )
})
