import { eq } from "drizzle-orm"
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
import {
  UnsafeLegacyMetadataError,
  deletionTarget,
  legacyDeletionTarget,
  ownedProjectWorktreeTarget,
} from "./removal-paths"

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

type GitResult = { started: boolean; code: number; text: string; stderr: string }

export class RequiredCleanupError extends Error {
  readonly code: "legacy_artifact_manual_reconciliation" | "required_cleanup_failed"

  constructor(
    readonly operation: string,
    readonly identity: string,
    cause?: unknown,
  ) {
    super(
      cause instanceof UnsafeLegacyMetadataError
        ? `legacy_artifact_manual_reconciliation: ${operation}:${identity}`
        : `required_cleanup_failed: ${operation}:${identity}`,
      { cause },
    )
    this.code = cause instanceof UnsafeLegacyMetadataError ? "legacy_artifact_manual_reconciliation" : "required_cleanup_failed"
  }
}

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
        return { started: true, code: yield* handle.exitCode, text, stderr } satisfies GitResult
      },
      Effect.scoped,
      Effect.catch((cause) =>
        Effect.succeed({
          started: false,
          code: -1,
          text: "",
          stderr: cause instanceof Error ? cause.message : String(cause),
        } satisfies GitResult),
      ),
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

    const required = <A>(operation: string, identity: string, effect: Effect.Effect<A, unknown>) =>
      effect.pipe(Effect.mapError((cause) => new RequiredCleanupError(operation, identity, cause)))

    const legacyTarget = (
      category: Parameters<typeof legacyDeletionTarget>[0]["category"],
      projectID: string | undefined,
      relatedID: string | undefined,
    ) =>
      required(
        "legacy_artifact",
        `${category}:${relatedID ?? projectID ?? ""}`,
        Effect.try({
          try: () => legacyDeletionTarget({ pathApi: path, dataRoot: Global.Path.data, category, projectID, relatedID }),
          catch: (cause) => cause,
        }),
      )

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
      const stopped = yield* git(["fsmonitor--daemon", "stop"], target)
      if (stopped.code === 0) return
      if ((stopped.stderr || stopped.text).includes("fsmonitor--daemon is not running")) return
      return yield* Effect.fail(new Error(`git fsmonitor stop failed for ${target}: ${stopped.stderr || stopped.text}`))
    })

    // Opencode-created sandbox worktrees register admin entries inside the user's
    // repo (.git/worktrees). Plain fs deletion would leave dangling entries, so each
    // entry goes through git first.
    const removeGitWorktrees = Effect.fn("ProjectRemoval.removeGitWorktrees")(function* (projectID: ProjectV2.ID, info: Project.Info) {
      const identity = (value: string) => (process.platform === "win32" ? value.toLowerCase() : value)
      const recorded = yield* db
        .select({ path: ProjectDeletionWorktreeTable.canonical_path, branch: ProjectDeletionWorktreeTable.branch })
        .from(ProjectDeletionWorktreeTable)
        .where(eq(ProjectDeletionWorktreeTable.project_id, info.id))
        .all()
        .pipe(Effect.orDie)
      const owned = yield* Effect.forEach(
        recorded,
        (entry) =>
          required(
            "worktree_snapshot",
            entry.path,
            Effect.try({
              try: () => ({
                canonical_path: ownedProjectWorktreeTarget({
                  pathApi: path,
                  dataRoot: Global.Path.data,
                  projectID,
                  candidate: entry.path,
                }),
                branch: entry.branch,
              }),
              catch: (cause) => cause,
            }),
          ),
        { concurrency: 1 },
      )
      const listed = yield* git(["worktree", "list", "--porcelain"], info.worktree)
      if (listed.code !== 0)
        return yield* Effect.fail(new Error(`git worktree list failed: ${listed.stderr || listed.text}`))
      const listedByPath = new Map(
        parseWorktreeList(listed.text).flatMap((entry) => {
          if (!entry.path) return []
          try {
            return [
              [
                identity(
                  ownedProjectWorktreeTarget({
                    pathApi: path,
                    dataRoot: Global.Path.data,
                    projectID,
                    candidate: entry.path,
                  }),
                ),
                entry,
              ] as const,
            ]
          } catch {
            return []
          }
        }),
      )
      const prepared = yield* Effect.forEach(owned, (entry) => {
        const listedEntry = listedByPath.get(identity(entry.canonical_path))
        const discoveredBranch = listedEntry?.branch?.replace(/^refs\/heads\//, "")
        if (listedEntry && entry.branch !== discoveredBranch)
          return Effect.fail(new Error(`worktree branch changed for ${entry.canonical_path}`))
        return Effect.succeed({ ...entry, listed: Boolean(listedEntry) })
      })
      for (const entry of prepared) {
        if (!entry.listed) {
          yield* required("worktree_rm", entry.canonical_path, rmPath(entry.canonical_path))
          continue
        }
        yield* required("worktree_dispose", entry.canonical_path, instanceStore.disposeDirectory(entry.canonical_path))
        yield* required("worktree_fsmonitor", entry.canonical_path, stopFsmonitor(entry.canonical_path))
        const removed = yield* git(["worktree", "remove", "--force", entry.canonical_path], info.worktree)
        if (removed.code !== 0) {
          const afterFailure = yield* git(["worktree", "list", "--porcelain"], info.worktree)
          if (afterFailure.code !== 0)
            return yield* Effect.fail(
              new Error(`git worktree remove failed for ${entry.canonical_path}: ${removed.stderr || removed.text}`),
            )
          const stillListed = parseWorktreeList(afterFailure.text).some((candidate) => {
            if (!candidate.path) return false
            try {
              return identity(
                ownedProjectWorktreeTarget({
                  pathApi: path,
                  dataRoot: Global.Path.data,
                  projectID,
                  candidate: candidate.path,
                }),
              ) === identity(entry.canonical_path)
            } catch {
              return false
            }
          })
          if (stillListed)
            return yield* Effect.fail(
              new Error(`git worktree remove failed for ${entry.canonical_path}: ${removed.stderr || removed.text}`),
            )
        }
        yield* required("worktree_rm", entry.canonical_path, rmPath(entry.canonical_path))
      }
      const after = yield* git(["worktree", "list", "--porcelain"], info.worktree)
      if (after.code !== 0) return yield* Effect.fail(new Error(`git worktree list failed: ${after.stderr || after.text}`))
      const remaining = new Set(
        parseWorktreeList(after.text).flatMap((entry) => {
          if (!entry.path) return []
          try {
            return [
              identity(
                ownedProjectWorktreeTarget({ pathApi: path, dataRoot: Global.Path.data, projectID, candidate: entry.path }),
              ),
            ]
          } catch {
            return []
          }
        }),
      )
      for (const entry of prepared) {
        if (!entry.branch || remaining.has(identity(entry.canonical_path))) continue
        const deleted = yield* git(["branch", "-D", "--", entry.branch], info.worktree)
        if (deleted.code === 0) continue
        const present = yield* git(["show-ref", "--verify", "--quiet", `refs/heads/${entry.branch}`], info.worktree)
        if (present.started && present.code === 1 && !present.text.trim() && !present.stderr.trim()) continue
        return yield* Effect.fail(new Error(`worktree branch delete failed for ${entry.branch}: ${deleted.stderr || deleted.text}`))
      }
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

    const preflightLocalCleanup = Effect.fn("ProjectRemoval.preflightLocalCleanup")(function* (
      projectID: ProjectV2.ID,
      info: Project.Info,
    ) {
      const artifacts = yield* db
        .select({ kind: ProjectDeletionArtifactTable.kind, id: ProjectDeletionArtifactTable.artifact_id })
        .from(ProjectDeletionArtifactTable)
        .where(eq(ProjectDeletionArtifactTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)
      const worktrees = yield* db
        .select({ path: ProjectDeletionWorktreeTable.canonical_path })
        .from(ProjectDeletionWorktreeTable)
        .where(eq(ProjectDeletionWorktreeTable.project_id, projectID))
        .all()
        .pipe(Effect.orDie)

      yield* Effect.all(
        [
          legacyTarget("worktree", info.id, undefined),
          legacyTarget("snapshot", info.id, undefined),
          legacyTarget("storage-project", info.id, undefined),
          legacyTarget("storage-session", info.id, undefined),
          required(
            "snapshot",
            projectID,
            Effect.try({
              try: () => deletionTarget({ pathApi: path, dataRoot: Global.Path.data, category: "snapshot", projectID }),
              catch: (cause) => cause,
            }),
          ),
          Effect.forEach(
            artifacts,
            (artifact) =>
              legacyTarget(
                artifact.kind === "session_diff"
                  ? "storage-session-diff"
                  : artifact.kind === "message"
                    ? "storage-message"
                    : "storage-part",
                undefined,
                artifact.id,
              ),
            { discard: true },
          ),
          Effect.forEach(
            worktrees,
            (worktree) =>
              required(
                "worktree_snapshot",
                worktree.path,
                Effect.try({
                  try: () =>
                    ownedProjectWorktreeTarget({
                      pathApi: path,
                      dataRoot: Global.Path.data,
                      projectID,
                      candidate: worktree.path,
                    }),
                  catch: (cause) => cause,
                }),
              ),
            { discard: true },
          ),
        ],
        { concurrency: 1, discard: true },
      )
      return artifacts
    })

    const purgeScoped = Effect.fn("ProjectRemoval.purgeScoped")(function* (
      projectID: ProjectV2.ID,
      info: Project.Info,
      artifacts: ReadonlyArray<{ kind: "session_diff" | "message" | "part"; id: string }>,
    ) {
      // Use the immutable session artifact snapshot: Session.remove may have
      // already deleted live rows before a later required cleanup retry.
      const ids = artifacts.filter((artifact) => artifact.kind === "session_diff").map((artifact) => artifact.id)
      const sessions = yield* db
        .select({ id: SessionTable.id, parentID: SessionTable.parent_id })
        .from(SessionTable)
        .where(eq(SessionTable.project_id, projectID))
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
      yield* Effect.forEach(ids, (id) => events.remove(id), { discard: true })

      // 5. Cached instances for every directory of the project.
      yield* Effect.forEach(directories, (directory) => instanceStore.disposeDirectory(directory), { discard: true })

      // 6. Artifacts under Global.Path.data only; never inside info.worktree.
      // Keep ProjectTable as immutable retry metadata until every mandatory
      // external cleanup has completed. A failed cleanup can then reconstruct
      // the same target set instead of mistaking a missing row for success.
      yield* removeGitWorktrees(projectID, info)
      yield* legacyTarget("snapshot", info.id, undefined).pipe(Effect.flatMap((target) => required("legacy_snapshot", info.id, rmPath(target))))
      yield* legacyTarget("storage-project", info.id, undefined).pipe(
        Effect.flatMap((target) => required("legacy_project_storage", info.id, rmPath(target))),
      )
      yield* legacyTarget("storage-session", info.id, undefined).pipe(
        Effect.flatMap((target) => required("legacy_session_storage", info.id, rmPath(target))),
      )
      yield* required(
        "snapshot",
        projectID,
        rmPath(deletionTarget({ pathApi: path, dataRoot: Global.Path.data, category: "snapshot", projectID })),
      )
      yield* Effect.forEach(
        artifacts,
        (artifact) =>
          legacyTarget(
            artifact.kind === "session_diff"
              ? "storage-session-diff"
              : artifact.kind === "message"
                ? "storage-message"
                : "storage-part",
            undefined,
            artifact.id,
          ).pipe(Effect.flatMap((target) => required("legacy_artifact_rm", artifact.id, rmPath(target)))),
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

    const completeLocalCleanup = Effect.fn("ProjectRemoval.completeLocalCleanup")(function* (projectID: ProjectV2.ID) {
      const row = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, projectID)).get().pipe(Effect.orDie)
      if (!row) return
      const info = Project.fromRow(row)
      yield* purgeScoped(projectID, info, yield* preflightLocalCleanup(projectID, info))
    })

    const cleanup = (projectID: ProjectV2.ID) =>
      completeLocalCleanup(projectID).pipe(
        Effect.mapError((cause) =>
          cause instanceof RequiredCleanupError ? cause : new RequiredCleanupError("local_cleanup", "project", cause),
        ),
      )

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
