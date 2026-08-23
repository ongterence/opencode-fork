import { and, eq } from "drizzle-orm"
import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import {
  ProjectDeletionArtifactTable,
  ProjectDeletionShareTable,
  ProjectDeletionWorktreeTable,
} from "@opencode-ai/core/project/deletion.sql"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { serviceUse } from "@opencode-ai/core/effect/service-use"
import { Global } from "@opencode-ai/core/global"
import { AppProcess } from "@opencode-ai/core/process"
import { ProjectV2 } from "@opencode-ai/core/project"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ProjectDirectoryTable, ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionShareTable } from "@opencode-ai/core/share/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { EventTable } from "@opencode-ai/core/event/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { Cause, Context, Effect, Layer, Schedule, Scope } from "effect"
import { existsSync } from "fs"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import * as Stream from "effect/Stream"
import path from "path"
import { Workspace } from "@/control-plane/workspace"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { ShareNext } from "@/share/share-next"
import { InstanceStore } from "./instance-store"
import * as Project from "./project"
import { ProjectDeletingError, ProjectDeletionCoordinator } from "./deletion-coordinator"
import { NotRemovableError } from "./project-errors"
import { legacyDeletionTarget } from "./removal-paths"

export { NotRemovableError } from "./project-errors"

export interface Interface {
  readonly remove: (
    projectID: ProjectV2.ID,
  ) => Effect.Effect<void, Project.NotFoundError | NotRemovableError | ProjectDeletingError>
  readonly retry: (
    projectID: ProjectV2.ID,
  ) => Effect.Effect<void, Project.NotFoundError | NotRemovableError | ProjectDeletingError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ProjectRemoval") {}

type GitResult = { code: number; text: string; stderr: string }

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const instanceStore = yield* InstanceStore.Service
    const shareNext = yield* ShareNext.Service
    const events = yield* EventV2Bridge.Service
    const session = yield* Session.Service
    const workspace = yield* Workspace.Service
    const coordinator = yield* ProjectDeletionCoordinator.Service
    const scope = yield* Scope.Scope

    const git = Effect.fnUntraced(
      function* (args: string[], cwd: string) {
        const handle = yield* spawner.spawn(ChildProcess.make("git", args, { cwd, extendEnv: true, stdin: "ignore" }))
        const [text, stderr] = yield* Effect.all(
          [Stream.mkString(Stream.decodeText(handle.stdout)), Stream.mkString(Stream.decodeText(handle.stderr))],
          { concurrency: 2 },
        )
        return { code: yield* handle.exitCode, text, stderr } satisfies GitResult
      },
      Effect.scoped,
      Effect.catch(() => Effect.succeed({ code: 1, text: "", stderr: "" } satisfies GitResult)),
    )

    // Mirrors Worktree.cleanDirectory: Windows file locks require many retries.
    const rmPath = (target: string) =>
      Effect.tryPromise({
        try: async () => {
          const fsp = await import("fs/promises")
          const attempts = process.platform === "win32" ? 50 : 5
          for (const attempt of Array.from({ length: attempts }, (_, i) => i)) {
            try {
              await fsp.rm(target, { recursive: true, force: true })
              return
            } catch {
              if (attempt === attempts - 1) throw new Error(`failed to remove ${target}`)
              await new Promise((resolve) => setTimeout(resolve, 100))
            }
          }
        },
        catch: (cause) => cause,
      })

    function parseWorktreeList(text: string) {
      const entries: { path?: string; branch?: string }[] = []
      for (const line of text.split("\n")) {
        if (line.startsWith("worktree ")) entries.push({ path: line.slice("worktree ".length) })
        else if (line.startsWith("branch ") && entries.length > 0)
          entries[entries.length - 1].branch = line.slice("branch ".length)
      }
      return entries
    }

    // Mirrors Worktree.remove: fsmonitor daemons hold directory handles on Windows
    // and must be stopped before the worktree directory can be removed.
    const stopFsmonitor = Effect.fnUntraced(function* (target: string) {
      if (!existsSync(target)) return
      yield* git(["fsmonitor--daemon", "stop"], target)
    })

    // Opencode-created sandbox worktrees register admin entries inside the user's
    // repo (.git/worktrees). Plain fs deletion would leave dangling entries, so each
    // entry goes through git first.
    const removeGitWorktrees = Effect.fn("ProjectRemoval.removeGitWorktrees")(function* (info: Project.Info) {
      // `git worktree list` emits forward-slash paths even on Windows, where
      // path.join produces backslash separators.
      const normalize = (value: string) => value.toLowerCase().replaceAll("\\", "/")
      const root = normalize(path.join(Global.Path.data, "worktree", info.id))
      const recorded = yield* db
        .select({ path: ProjectDeletionWorktreeTable.canonical_path, branch: ProjectDeletionWorktreeTable.branch })
        .from(ProjectDeletionWorktreeTable)
        .where(eq(ProjectDeletionWorktreeTable.project_id, info.id))
        .all()
        .pipe(Effect.orDie)
      const recordedBranches = new Map(recorded.map((entry) => [normalize(entry.path), entry.branch]))
      const listed = yield* git(["worktree", "list", "--porcelain"], info.worktree)
      const owned =
        listed.code !== 0
          ? []
          : parseWorktreeList(listed.text).flatMap((entry) =>
              entry.path && normalize(entry.path).startsWith(root)
                ? [
                    {
                      path: entry.path,
                      branch:
                        entry.branch?.replace(/^refs\/heads\//, "") ?? recordedBranches.get(normalize(entry.path)),
                    },
                  ]
                : [],
            )
      if (owned.length === 0) {
        yield* rmPath(root)
        return
      }
      for (const entry of owned) {
        yield* instanceStore.disposeDirectory(entry.path).pipe(Effect.ignore)
        yield* stopFsmonitor(entry.path)
        const removed = yield* git(["worktree", "remove", "--force", entry.path], info.worktree)
        if (removed.code !== 0)
          return yield* Effect.fail(new Error(`git worktree remove failed for ${entry.path}: ${removed.stderr}`))
        yield* rmPath(entry.path)
      }
      // Prune stale admin entries for dirs already gone from disk.
      yield* git(["worktree", "prune"], info.worktree).pipe(Effect.ignore)

      // Branch refs are only safe to destroy once git confirms their worktree is
      // gone; failed removals keep both the admin entry and the branch.
      const after = yield* git(["worktree", "list", "--porcelain"], info.worktree)
      const remaining = new Set(
        after.code !== 0
          ? []
          : parseWorktreeList(after.text).flatMap((entry) => (entry.path ? [normalize(entry.path)] : [])),
      )
      for (const entry of owned) {
        if (!entry.branch || remaining.has(normalize(entry.path))) continue
        const deleted = yield* git(["branch", "-D", entry.branch], info.worktree)
        if (deleted.code !== 0)
          return yield* Effect.fail(new Error(`worktree branch delete failed for ${entry.branch}: ${deleted.stderr}`))
      }
      yield* rmPath(root)
    })

    const emitDeleted = Effect.fn("ProjectRemoval.emitDeleted")(function* (id: ProjectV2.ID, eventID: EventV2.ID) {
      const existing = yield* db
        .select({ id: EventTable.id })
        .from(EventTable)
        .where(eq(EventTable.id, eventID))
        .get()
        .pipe(Effect.orDie)
      if (existing) return yield* events.deliverProjectDeleted(id, eventID)
      yield* events.publish(
        Project.Event.Deleted,
        { id },
        {
          id: eventID,
          metadata: { global: true, project: id },
        },
      )
    })

    const purgeScoped = Effect.fn("ProjectRemoval.purgeScoped")(function* (
      projectID: ProjectV2.ID,
      info: Project.Info,
    ) {
      // Collect everything owned by the project before any deletion.
      const sessions = yield* db
        .select({ id: SessionTable.id, parentID: SessionTable.parent_id })
        .from(SessionTable)
        .where(eq(SessionTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)
      const sessionIDs = new Set(sessions.map((entry) => entry.id))
      const ids = [...sessionIDs]

      const artifacts = yield* db
        .select({ kind: ProjectDeletionArtifactTable.kind, id: ProjectDeletionArtifactTable.artifact_id })
        .from(ProjectDeletionArtifactTable)
        .where(eq(ProjectDeletionArtifactTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)

      const directories = [
        ...new Set([
          info.worktree,
          ...info.sandboxes,
          ...(yield* db
            .select({ directory: ProjectDirectoryTable.directory })
            .from(ProjectDirectoryTable)
            .where(eq(ProjectDirectoryTable.project_id, projectID))
            .all()
            .pipe(Effect.orDie)).map((entry) => entry.directory),
        ]),
      ]

      const workspaceIDs = (yield* db
        .select({ id: WorkspaceTable.id })
        .from(WorkspaceTable)
        .where(eq(WorkspaceTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)).map((entry) => entry.id)

      // Workspaces stop sync fibers, dispose adapters, remove their sessions.
      yield* Effect.forEach(workspaceIDs, (id) => workspace.remove(id), { discard: true })

      // 3. Sessions not covered by a workspace: top-most within the project set;
      // Session.remove recurses children and cleans event rows.
      const remaining = yield* db
        .select({ id: SessionTable.id, parentID: SessionTable.parent_id })
        .from(SessionTable)
        .where(eq(SessionTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)
      const remainingIDs = new Set(remaining.map((entry) => entry.id))
      yield* Effect.forEach(
        remaining.filter((entry) => !entry.parentID || !remainingIDs.has(entry.parentID)),
        (entry) => session.remove(entry.id),
        { discard: true },
      )

      // 4. Event aggregates have no FK; no-op where Session.remove already cleaned.
      yield* Effect.forEach(ids, (id) => events.remove(id).pipe(Effect.ignore), { discard: true })

      // 5. Cached instances for every directory of the project.
      yield* Effect.forEach(directories, (directory) => instanceStore.disposeDirectory(directory).pipe(Effect.ignore), {
        discard: true,
      })

      // 6. Artifacts under Global.Path.data only; never inside info.worktree.
      // Keep ProjectTable as immutable retry metadata until every mandatory
      // external cleanup has completed. A failed cleanup can then reconstruct
      // the same target set instead of mistaking a missing row for success.
      yield* removeGitWorktrees(info)
      yield* rmPath(path.join(Global.Path.data, "snapshot", info.id))
      yield* rmPath(path.join(Global.Path.data, "storage", "project", `${info.id}.json`))
      yield* rmPath(path.join(Global.Path.data, "storage", "session", info.id))
      yield* Effect.forEach(
        artifacts,
        (artifact) =>
          Effect.try({
            try: () =>
              legacyDeletionTarget({
                pathApi: path,
                dataRoot: Global.Path.data,
                category:
                  artifact.kind === "session_diff"
                    ? "storage-session-diff"
                    : artifact.kind === "message"
                      ? "storage-message"
                      : "storage-part",
                relatedID: artifact.id,
              }),
            catch: (cause) => cause,
          }).pipe(Effect.flatMap(rmPath)),
        { discard: true },
      )

      // 7. FK cascades sweep project_directory, permission, workspace, stragglers.
      yield* db.delete(ProjectTable).where(eq(ProjectTable.id, projectID)).run().pipe(Effect.orDie)
    })

    const revokeShares = Effect.fn("ProjectRemoval.revokeShares")(function* (projectID: ProjectV2.ID) {
      const shares = yield* db
        .select()
        .from(ProjectDeletionShareTable)
        .where(eq(ProjectDeletionShareTable.project_id, projectID))
        .orderBy(ProjectDeletionShareTable.session_id)
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(
        shares.filter((share) => share.status !== "revoked"),
        (share) =>
          Effect.gen(function* () {
            const baseUrl = new URL(share.base_url).origin
            yield* shareNext
              .revokeHistorical({
                sessionID: SessionID.make(share.session_id),
                shareID: share.share_id,
                secret: share.secret,
                baseUrl,
              })
              .pipe(
                Effect.flatMap(() =>
                  db
                    .transaction(
                      (tx) =>
                        Effect.gen(function* () {
                          yield* tx
                            .update(ProjectDeletionShareTable)
                            .set({ status: "revoked", secret: "", last_error: null, updated_at: Date.now() })
                            .where(
                              and(
                                eq(ProjectDeletionShareTable.project_id, projectID),
                                eq(ProjectDeletionShareTable.session_id, share.session_id),
                              ),
                            )
                            .run()
                          yield* tx
                            .delete(SessionShareTable)
                            .where(eq(SessionShareTable.session_id, share.session_id))
                            .run()
                        }),
                      { behavior: "immediate" },
                    )
                    .pipe(Effect.orDie),
                ),
                Effect.catchCause((cause) =>
                  db
                    .transaction(
                      (tx) =>
                        tx
                          .update(ProjectDeletionShareTable)
                          .set({
                            status: "failed",
                            attempt: share.attempt + 1,
                            last_error: Cause.pretty(cause),
                            updated_at: Date.now(),
                          })
                          .where(
                            and(
                              eq(ProjectDeletionShareTable.project_id, projectID),
                              eq(ProjectDeletionShareTable.session_id, share.session_id),
                            ),
                          )
                          .run(),
                      { behavior: "immediate" },
                    )
                    .pipe(Effect.orDie, Effect.andThen(Effect.failCause(cause))),
                ),
                Effect.retry({ times: 2, schedule: Schedule.spaced("200 millis") }),
              )
          }),
        { discard: true },
      )
    })

    const cleanup = Effect.fn("ProjectRemoval.cleanup")(function* (projectID: ProjectV2.ID) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, projectID)).get().pipe(Effect.orDie)
      if (!row) {
        yield* rmPath(path.join(Global.Path.data, "snapshot", projectID))
        return
      }
      const info = Project.fromRow(row)
      yield* purgeScoped(projectID, info)
    })

    coordinator.install({ revokeShares, cleanup, publish: emitDeleted })
    yield* coordinator.recover().pipe(Effect.forkIn(scope))

    const remove = Effect.fn("ProjectRemoval.remove")(function* (projectID: ProjectV2.ID) {
      const outcome = yield* coordinator.runOwned(projectID)
      if (outcome.status === "completed") return
      return yield* new ProjectDeletingError({ projectID, phase: outcome.phase })
    })

    const retry = Effect.fn("ProjectRemoval.retry")(function* (projectID: ProjectV2.ID) {
      const outcome = yield* coordinator.retry(projectID)
      if (outcome.status === "completed") return
      return yield* new ProjectDeletingError({ projectID, phase: outcome.phase })
    })

    return Service.of({ remove, retry })
  }),
)

export const use = serviceUse(Service)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    Database.node,
    AppProcess.node,
    CrossSpawnSpawner.node,
    InstanceStore.node,
    ShareNext.node,
    EventV2Bridge.node,
    Session.node,
    Workspace.node,
    ProjectDeletionCoordinator.node,
  ],
})

export * as ProjectRemoval from "./removal"
