import { afterEach, describe, expect } from "bun:test"
import { $ } from "bun"
import { NodeHttpServer } from "@effect/platform-node"
import Http from "node:http"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import {
  ProjectDeletionArtifactTable,
  ProjectDeletionJobTable,
  ProjectDeletionShareTable,
  ProjectDeletionWorktreeTable,
} from "@opencode-ai/core/project/deletion.sql"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Context, Effect, Layer } from "effect"
import { eq, sql } from "drizzle-orm"
import { HttpServer, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import path from "path"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { Snapshot } from "../../src/snapshot"
import { ProjectDeletionCoordinator } from "../../src/project/deletion-coordinator"
import { deletionTarget, opaqueStorageKey } from "../../src/project/removal-paths"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionShareTable } from "@opencode-ai/core/share/sql"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance, tmpdirScoped } from "../fixture/fixture"
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

function listenShareServer(
  status: number | number[] | ((request: HttpServerRequest.HttpServerRequest) => number),
  requests: Array<{ method: string; url: string; body: string }> = [],
) {
  return Effect.gen(function* () {
    const responses = typeof status === "number" ? [status] : Array.isArray(status) ? [...status] : []
    const context = yield* Layer.build(NodeHttpServer.layer(Http.createServer, { host: "127.0.0.1", port: 0 }))
    const server = Context.get(context, HttpServer.HttpServer)
    yield* server.serve(
      HttpServerRequest.HttpServerRequest.use((request) =>
        Effect.gen(function* () {
          requests.push({ method: request.method, url: request.url, body: yield* request.text })
          return HttpServerResponse.empty({
            status: typeof status === "function" ? status(request) : (responses.shift() ?? (typeof status === "number" ? status : 500)),
          })
        }),
      ),
    )
    return HttpServer.formatAddress(server.address)
  })
}

function unavailableShareUrl() {
  return Effect.promise(async () => {
    const server = Http.createServer()
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject)
      server.listen(0, "127.0.0.1", resolve)
    })
    const address = server.address()
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
    if (!address || typeof address === "string") throw new Error("could not reserve a loopback port")
    return `http://127.0.0.1:${address.port}`
  })
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
        const project = (yield* current.json) as { id: ProjectV2.ID }
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
    "retains share credentials and local rows when historical remote revocation fails",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        const session = (yield* created.json) as { id: SessionID }
        const { db } = yield* Database.Service
        const remoteRequests: Array<{ method: string; url: string; body: string }> = []
        const shareServer = yield* listenShareServer(500, remoteRequests)
        yield* db
          .insert(SessionShareTable)
          .values({
            session_id: session.id,
            id: "shr_retained",
            secret: "sec_retained",
            url: `${shareServer}/share/retained`,
          })
          .run()
          .pipe(Effect.orDie)
        const events = yield* collectGlobalEvents()
        const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })

        expect(response.status).toBe(409)
        expect(yield* response.json).toMatchObject({
          code: "project_deletion_retryable",
          phase: "share_failed",
          retry: true,
        })
        expect(remoteRequests).toHaveLength(3)
        expect(remoteRequests[0]).toEqual({
          method: "DELETE",
          url: "/api/share/shr_retained",
          body: JSON.stringify({ secret: "sec_retained" }),
        })
        expect(
          yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, project.id)).get().pipe(Effect.orDie),
        ).toBeDefined()
        expect(
          yield* db
            .select()
            .from(SessionShareTable)
            .where(eq(SessionShareTable.session_id, session.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ secret: "sec_retained" })
        const journal = yield* db
          .select()
          .from(ProjectDeletionShareTable)
          .where(eq(ProjectDeletionShareTable.project_id, project.id))
          .get()
          .pipe(Effect.orDie)
        expect(journal).toMatchObject({ status: "failed", secret: "sec_retained", base_url: shareServer })
        expect(journal?.last_error).toContain("HTTP 500")
        expect(journal?.last_error).not.toContain("sec_retained")
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(0)
      }),
    { git: true },
  )

  it.instance(
    "retains local credentials and reports retryable 401 and 403 share revocation failures",
    () =>
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const events = yield* collectGlobalEvents()
        for (const status of [401, 403]) {
          const directory = yield* tmpdirScoped({ git: true })
          const current = yield* requestInDirectory("/project/current", directory)
          const project = (yield* current.json) as { id: ProjectV2.ID }
          const created = yield* requestInDirectory("/session", directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
          })
          const session = (yield* created.json) as { id: SessionID }
          const shareServer = yield* listenShareServer(status)
          const secret = `sec_auth_${status}`
          yield* db
            .insert(SessionShareTable)
            .values({
              session_id: session.id,
              id: `shr_auth_${status}`,
              secret,
              url: `${shareServer}/share/auth-${status}`,
            })
            .run()
            .pipe(Effect.orDie)
          const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })

          expect(response.status).toBe(409)
          expect(yield* response.json).toMatchObject({
            code: "project_deletion_retryable",
            phase: "share_failed",
            retry: true,
          })
          expect(
            yield* db
              .select()
              .from(ProjectTable)
              .where(eq(ProjectTable.id, project.id))
              .get()
              .pipe(Effect.orDie),
          ).toBeDefined()
          expect(
            yield* db
              .select()
              .from(SessionShareTable)
              .where(eq(SessionShareTable.session_id, session.id))
              .get()
              .pipe(Effect.orDie),
          ).toMatchObject({ secret })
          const journal = yield* db
            .select()
            .from(ProjectDeletionShareTable)
            .where(eq(ProjectDeletionShareTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie)
          expect(journal).toMatchObject({ status: "failed", secret })
          expect(journal?.last_error).toContain(`HTTP ${status}`)
          expect(journal?.last_error).not.toContain(secret)
        }
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(0)
      }),
    { git: true },
    { timeout: 15_000 },
  )

  it.instance(
    "retains local credentials after a historical share network failure",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        const session = (yield* created.json) as { id: SessionID }
        const { db } = yield* Database.Service
        const secret = "sec_network_retained"
        const shareUrl = yield* unavailableShareUrl()
        yield* db
          .insert(SessionShareTable)
          .values({ session_id: session.id, id: "shr_network", secret, url: `${shareUrl}/share/network` })
          .run()
          .pipe(Effect.orDie)
        const events = yield* collectGlobalEvents()
        const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })

        expect(response.status).toBe(409)
        expect(yield* response.json).toMatchObject({ code: "project_deletion_retryable", retry: true })
        expect(
          yield* db
            .select()
            .from(SessionShareTable)
            .where(eq(SessionShareTable.session_id, session.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ secret })
        const journal = yield* db
          .select()
          .from(ProjectDeletionShareTable)
          .where(eq(ProjectDeletionShareTable.project_id, project.id))
          .get()
          .pipe(Effect.orDie)
        expect(journal).toMatchObject({ status: "failed", secret })
        expect(journal?.last_error).not.toContain(secret)
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(0)
      }),
    { git: true },
  )

  it.instance(
    "reports a retryable share failure and completes an explicit retry after a 404",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        const session = (yield* created.json) as { id: SessionID }
        const { db } = yield* Database.Service
        const shareServer = yield* listenShareServer([500, 500, 500, 404])
        yield* db
          .insert(SessionShareTable)
          .values({
            session_id: session.id,
            id: "shr_retryable",
            secret: "sec_retryable",
            url: `${shareServer}/share/retryable`,
          })
          .run()
          .pipe(Effect.orDie)
        const events = yield* collectGlobalEvents()
        const failed = yield* request(`/global/project/${project.id}`, { method: "DELETE" })

        expect(failed.status).toBe(409)
        expect(yield* failed.json).toMatchObject({
          code: "project_deletion_retryable",
          phase: "share_failed",
          retry: true,
        })
        expect(
          yield* db
            .select()
            .from(SessionShareTable)
            .where(eq(SessionShareTable.session_id, session.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ secret: "sec_retryable" })
        const retried = yield* request(`/global/project/${project.id}/delete/retry`, { method: "POST" })

        expect(retried.status).toBe(204)
        expect(
          yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, project.id)).get().pipe(Effect.orDie),
        ).toBeUndefined()
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(1)
      }),
    { git: true },
  )

  it.instance(
    "scrubs only a share acknowledged absent before a later revocation failure",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const created = yield* Effect.all(
          [
            requestInDirectory("/session", tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: "{}",
            }),
            requestInDirectory("/session", tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: "{}",
            }),
          ],
          { concurrency: 1 },
        )
        const first = (yield* created[0].json) as { id: SessionID }
        const second = (yield* created[1].json) as { id: SessionID }
        const absent = first.id < second.id ? first : second
        const failed = first.id < second.id ? second : first
        const { db } = yield* Database.Service
        const shareServer = yield* listenShareServer((request) => (request.url.endsWith("shr_absent") ? 404 : 500))
        yield* db
          .insert(SessionShareTable)
          .values([
            {
              session_id: absent.id,
              id: "shr_absent",
              secret: "sec_absent",
              url: `${shareServer}/share/absent`,
            },
            {
              session_id: failed.id,
              id: "shr_failed",
              secret: "sec_failed",
              url: `${shareServer}/share/failed`,
            },
          ])
          .run()
          .pipe(Effect.orDie)
        const events = yield* collectGlobalEvents()
        const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })

        expect(response.status).toBe(409)
        expect(
          yield* db
            .select()
            .from(SessionShareTable)
            .where(eq(SessionShareTable.session_id, absent.id))
            .get()
            .pipe(Effect.orDie),
        ).toBeUndefined()
        expect(
          yield* db
            .select()
            .from(SessionShareTable)
            .where(eq(SessionShareTable.session_id, failed.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ secret: "sec_failed" })
        expect(
          yield* db
            .select()
            .from(ProjectDeletionShareTable)
            .where(eq(ProjectDeletionShareTable.session_id, absent.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ status: "revoked", secret: "" })
        expect(
          yield* db
            .select()
            .from(ProjectDeletionShareTable)
            .where(eq(ProjectDeletionShareTable.session_id, failed.id))
            .get()
            .pipe(Effect.orDie),
        ).toMatchObject({ status: "failed", secret: "sec_failed" })
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(0)
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
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        const session = (yield* created.json) as { id: SessionID }
        const messageID = MessageID.make("msg_cleanup_snapshot")
        const partID = PartID.make("prt_cleanup_snapshot")
        const root = deletionTarget({
          pathApi: path,
          dataRoot: Global.Path.data,
          category: "worktree",
          projectID: project.id,
        })
        const worktree = path.join(root, "retry-locked")
        const legacyWorktree = path.join(Global.Path.data, "worktree", project.id, "legacy-owned")
        const sibling = path.join(
          Global.Path.data,
          "project-artifacts",
          "v1",
          `${opaqueStorageKey(project.id)}-personal`,
          "sibling",
        )
        const artifact = path.join(Global.Path.data, "storage", "project", `${project.id}.json`)
        const sessionDiffArtifact = path.join(Global.Path.data, "storage", "session_diff", `${session.id}.json`)
        const messageArtifact = path.join(Global.Path.data, "storage", "message", messageID)
        const partArtifact = path.join(Global.Path.data, "storage", "part", partID)
        yield* fs.ensureDir(root)
        const { db } = yield* Database.Service
        yield* db
          .insert(MessageTable)
          .values({ id: messageID, session_id: session.id, time_created: 1, time_updated: 1, data: {} as never })
          .run()
          .pipe(Effect.orDie)
        yield* db
          .insert(PartTable)
          .values({
            id: partID,
            message_id: messageID,
            session_id: session.id,
            time_created: 1,
            time_updated: 1,
            data: {} as never,
          })
          .run()
          .pipe(Effect.orDie)
        yield* Effect.promise(() => $`git worktree add -b deletion-retry ${worktree}`.cwd(tmp.directory).quiet())
        yield* Effect.promise(() => $`git worktree add -b deletion-legacy ${legacyWorktree}`.cwd(tmp.directory).quiet())
        yield* Effect.promise(() => $`git worktree add -b deletion-sibling ${sibling}`.cwd(tmp.directory).quiet())
        yield* Effect.promise(() => $`git worktree lock ${worktree}`.cwd(tmp.directory).quiet())
        yield* db.run(sql`UPDATE project SET sandboxes = ${JSON.stringify([worktree, legacyWorktree])} WHERE id = ${project.id}`).pipe(Effect.orDie)
        yield* Effect.forEach(
          [artifact, sessionDiffArtifact, path.join(messageArtifact, "data"), path.join(partArtifact, "data")],
          (target) => Effect.promise(() => Bun.write(target, "must be removed")),
          { discard: true },
        )

        const events = yield* collectGlobalEvents()
        const first = yield* request(`/global/project/${project.id}`, { method: "DELETE" })
        expect(first.status).toBe(409)
        expect(
          (yield* db.select().from(ProjectTable).all().pipe(Effect.orDie)).some((row) => row.id === project.id),
        ).toBe(true)
        expect(yield* fs.exists(worktree)).toBe(true)
        expect(yield* fs.exists(artifact)).toBe(true)
        // The local cleanup has already projected the session removal when the
        // locked worktree makes a later required operation fail. Keep an
        // aggregate behind to prove recovery uses the immutable journal rather
        // than rediscovering sessions from the now-empty live table.
        expect(
          yield* db.select().from(SessionTable).where(eq(SessionTable.id, session.id)).get().pipe(Effect.orDie),
        ).toBeUndefined()
        yield* db.insert(EventSequenceTable).values({ aggregate_id: session.id, seq: 1 }).run().pipe(Effect.orDie)
        expect(
          (yield* db
            .select({ path: ProjectDeletionWorktreeTable.canonical_path, branch: ProjectDeletionWorktreeTable.branch })
            .from(ProjectDeletionWorktreeTable)
            .where(eq(ProjectDeletionWorktreeTable.project_id, project.id))
            .all()
            .pipe(Effect.orDie))
            .map((entry) => `${entry.path}:${entry.branch}`)
            .sort(),
        ).toEqual([`${legacyWorktree}:deletion-legacy`, `${worktree}:deletion-retry`].sort())
        expect(
          (yield* db
            .select({ kind: ProjectDeletionArtifactTable.kind, id: ProjectDeletionArtifactTable.artifact_id })
            .from(ProjectDeletionArtifactTable)
            .where(eq(ProjectDeletionArtifactTable.project_id, project.id))
            .all()
            .pipe(Effect.orDie))
            .map((entry) => `${entry.kind}:${entry.id}`)
            .sort(),
        ).toEqual([`message:${messageID}`, `part:${partID}`, `session_diff:${session.id}`])

        yield* Effect.promise(() => $`git worktree unlock ${worktree}`.cwd(tmp.directory).quiet())
        const coordinator = yield* ProjectDeletionCoordinator.Service
        yield* coordinator.recover()

        expect(
          (yield* db.select().from(ProjectTable).all().pipe(Effect.orDie)).some((row) => row.id === project.id),
        ).toBe(false)
        expect(yield* fs.exists(worktree)).toBe(false)
        expect(yield* fs.exists(legacyWorktree)).toBe(false)
        expect(yield* fs.exists(artifact)).toBe(false)
        expect(yield* fs.exists(sessionDiffArtifact)).toBe(false)
        expect(yield* fs.exists(messageArtifact)).toBe(false)
        expect(yield* fs.exists(partArtifact)).toBe(false)
        expect(yield* fs.exists(sibling)).toBe(true)
        expect(
          yield* db
            .select()
            .from(EventSequenceTable)
            .where(eq(EventSequenceTable.aggregate_id, session.id))
            .get()
            .pipe(Effect.orDie),
        ).toBeUndefined()
        expect((yield* Effect.promise(() => $`git show-ref --verify --quiet refs/heads/deletion-retry`.cwd(tmp.directory).quiet().nothrow())).exitCode).not.toBe(0)
        expect((yield* Effect.promise(() => $`git show-ref --verify --quiet refs/heads/deletion-legacy`.cwd(tmp.directory).quiet().nothrow())).exitCode).not.toBe(0)
        expect((yield* Effect.promise(() => $`git show-ref --verify --quiet refs/heads/deletion-sibling`.cwd(tmp.directory).quiet().nothrow())).exitCode).toBe(0)
        expect(yield* db.select().from(ProjectDeletionJobTable).all().pipe(Effect.orDie)).toEqual([])
        expect(yield* db.select().from(ProjectDeletionWorktreeTable).all().pipe(Effect.orDie)).toEqual([])
        expect(yield* db.select().from(ProjectDeletionArtifactTable).all().pipe(Effect.orDie)).toEqual([])
        expect(events.seen.filter((event) => event.payload.type === "project.deleted")).toHaveLength(1)
      }),
    { git: true },
  )

  it.instance(
    "keeps a branch changed after deletion begin out of the cleanup journal",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const worktree = path.join(
          deletionTarget({
            pathApi: path,
            dataRoot: Global.Path.data,
            category: "worktree",
            projectID: project.id,
          }),
          "branch-race",
        )
        const { db } = yield* Database.Service
        yield* Effect.promise(() => $`git worktree add -b deletion-owner ${worktree}`.cwd(tmp.directory).quiet())
        yield* db.run(sql`UPDATE project SET sandboxes = ${JSON.stringify([worktree])} WHERE id = ${project.id}`).pipe(Effect.orDie)

        const coordinator = yield* ProjectDeletionCoordinator.Service
        expect(yield* coordinator.begin(project.id)).toBe("owner")
        expect(
          (yield* db
            .select({ branch: ProjectDeletionWorktreeTable.branch })
            .from(ProjectDeletionWorktreeTable)
            .where(eq(ProjectDeletionWorktreeTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie))?.branch,
        ).toBe("deletion-owner")
        yield* Effect.promise(() => $`git checkout -b deletion-personal`.cwd(worktree).quiet())

        expect(yield* coordinator.execute(project.id)).toEqual({ status: "in_progress", phase: "cleaning" })
        expect(
          (yield* db
            .select({ error: ProjectDeletionJobTable.last_error })
            .from(ProjectDeletionJobTable)
            .where(eq(ProjectDeletionJobTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie))?.error,
        ).toContain("worktree branch changed")
        expect((yield* Effect.promise(() => $`git show-ref --verify --quiet refs/heads/deletion-owner`.cwd(tmp.directory).quiet().nothrow())).exitCode).toBe(0)
        expect((yield* Effect.promise(() => $`git show-ref --verify --quiet refs/heads/deletion-personal`.cwd(tmp.directory).quiet().nothrow())).exitCode).toBe(0)
      }),
    { git: true },
  )

  it.instance(
    "retains the journal when git cannot spawn the show-ref confirmation after branch deletion fails",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const fs = yield* FSUtil.Service
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const worktree = path.join(
          deletionTarget({
            pathApi: path,
            dataRoot: Global.Path.data,
            category: "worktree",
            projectID: project.id,
          }),
          "spawn-sentinel",
        )
        const { db } = yield* Database.Service
        yield* Effect.promise(() => $`git worktree add -b deletion-spawn-sentinel ${worktree}`.cwd(tmp.directory).quiet())
        yield* db.run(sql`UPDATE project SET sandboxes = ${JSON.stringify([worktree])} WHERE id = ${project.id}`).pipe(Effect.orDie)
        const real = path.join(process.env.ProgramFiles ?? "C:\\Program Files", "Git", "cmd", "git.exe")
        const bin = path.join(tmp.directory, "git-spawn-sentinel")
        yield* fs.ensureDir(bin)
        yield* Effect.promise(() =>
          Bun.write(
            path.join(bin, "git.cmd"),
            [
              "@echo off",
              'if "%~1"=="branch" (',
              '  ren "%~f0" "git-disabled.cmd"',
              "  echo fatal: simulated branch delete failure 1>&2",
              "  exit /b 128",
              ")",
              `"${real}" %*`,
            ].join("\r\n"),
          ),
        )
        const coordinator = yield* ProjectDeletionCoordinator.Service
        expect(yield* coordinator.begin(project.id)).toBe("owner")
        const outcome = yield* Effect.acquireUseRelease(
          Effect.sync(() => {
            const previous = { PATH: process.env.PATH, Path: process.env.Path }
            process.env.PATH = bin
            process.env.Path = bin
            return previous
          }),
          () => coordinator.execute(project.id),
          (previous) =>
            Effect.sync(() => {
              if (previous.PATH === undefined) delete process.env.PATH
              else process.env.PATH = previous.PATH
              if (previous.Path === undefined) delete process.env.Path
              else process.env.Path = previous.Path
            }),
        )

        expect(outcome).toEqual({ status: "in_progress", phase: "cleaning" })
        expect(
          yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, project.id)).get().pipe(Effect.orDie),
        ).toBeDefined()
        expect(
          yield* db
            .select()
            .from(ProjectDeletionWorktreeTable)
            .where(eq(ProjectDeletionWorktreeTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie),
        ).toBeDefined()
        expect(
          (yield* db
            .select({ error: ProjectDeletionJobTable.last_error })
            .from(ProjectDeletionJobTable)
            .where(eq(ProjectDeletionJobTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie))?.error,
        ).toContain("spawn")
        expect(
          (yield* db
            .select({ error: ProjectDeletionJobTable.last_error })
            .from(ProjectDeletionJobTable)
            .where(eq(ProjectDeletionJobTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie))?.error,
        ).toContain("worktree branch delete failed")
      }),
    { git: true },
  )

  it.instance(
    "replays a committed deletion event that crashed before the global bridge",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const eventID = EventV2.ID.make("evt_project_deleted_unbridged")
        const { db } = yield* Database.Service
        yield* db.delete(ProjectTable).where(eq(ProjectTable.id, project.id)).run().pipe(Effect.orDie)
        yield* db
          .insert(EventSequenceTable)
          .values({ aggregate_id: project.id, seq: 0, owner_id: null })
          .run()
          .pipe(Effect.orDie)
        yield* db
          .insert(EventTable)
          .values({
            id: eventID,
            aggregate_id: project.id,
            seq: 0,
            type: "project.deleted.1",
            data: { id: project.id },
          })
          .run()
          .pipe(Effect.orDie)
        yield* db
          .insert(ProjectDeletionJobTable)
          .values({
            project_id: project.id,
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

        const events = yield* collectGlobalEvents()
        const coordinator = yield* ProjectDeletionCoordinator.Service
        yield* coordinator.recover()

        expect(
          events.seen.filter((event) => event.payload.id === eventID && event.payload.type === "project.deleted"),
        ).toHaveLength(1)
        expect(
          events.seen.filter((event) => event.payload.type === "sync" && event.payload.syncEvent?.id === eventID),
        ).toHaveLength(1)
      }),
    { git: true },
  )

  it.instance(
    "records manual reconciliation before touching rows or files for malformed legacy metadata",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const fs = yield* FSUtil.Service
        const current = yield* requestInDirectory("/project/current", tmp.directory)
        const project = (yield* current.json) as { id: ProjectV2.ID }
        const created = yield* requestInDirectory("/session", tmp.directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        })
        const session = (yield* created.json) as { id: SessionID }
        const malformed = "../outside"
        const sentinel = path.join(tmp.directory, "manual-reconciliation-sentinel")
        const { db } = yield* Database.Service
        yield* db
          .insert(ProjectDeletionArtifactTable)
          .values({ project_id: project.id, kind: "message", artifact_id: malformed })
          .run()
          .pipe(Effect.orDie)
        yield* Effect.promise(() => Bun.write(sentinel, "do not touch"))

        const response = yield* request(`/global/project/${project.id}`, { method: "DELETE" })
        expect(response.status).toBe(409)
        expect(yield* fs.exists(sentinel)).toBe(true)
        expect(
          yield* db.select().from(SessionTable).where(eq(SessionTable.id, session.id)).get().pipe(Effect.orDie),
        ).toBeDefined()
        expect(
          yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, project.id)).get().pipe(Effect.orDie),
        ).toBeDefined()
        expect(
          (yield* db
            .select({ error: ProjectDeletionJobTable.last_error })
            .from(ProjectDeletionJobTable)
            .where(eq(ProjectDeletionJobTable.project_id, project.id))
            .get()
            .pipe(Effect.orDie))?.error,
        ).toContain("legacy_artifact_manual_reconciliation")
      }),
    { git: true },
  )
})
