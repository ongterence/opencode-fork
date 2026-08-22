import { afterEach, describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Global } from "@opencode-ai/core/global"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { Effect, Layer } from "effect"
import path from "path"
import { GlobalBus, type GlobalEvent } from "../../src/bus/global"
import { Snapshot } from "../../src/snapshot"
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
        expect(
          events.seen.some(
            (event) =>
              event.directory === "global" &&
              event.payload.type === "project.deleted" &&
              (event.payload.properties as { id?: string }).id === target.id,
          ),
        ).toBe(true)
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
})
