import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { Database } from "@opencode-ai/core/database/database"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { KeyedMutex } from "@opencode-ai/core/effect/keyed-mutex"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Global } from "@opencode-ai/core/global"
import {
  type DeletionPhase,
  ProjectDeletionArtifactTable,
  ProjectDeletionJobTable,
  ProjectDeletionShareTable,
  ProjectDeletionWorktreeTable,
} from "@opencode-ai/core/project/deletion.sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionShareTable } from "@opencode-ai/core/share/sql"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { and, eq, inArray, sql } from "drizzle-orm"
import { Cause, Context, Deferred, Duration, Effect, Exit, Layer, Schema } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import * as Stream from "effect/Stream"
import path from "path"
import { NotFoundError, NotRemovableError } from "./project-errors"
import { ownedProjectWorktreeTarget } from "./removal-paths"

export type DeleteOutcome =
  | { status: "completed" }
  | { status: "in_progress"; phase: DeletionPhase }
  | { status: "retryable_failure"; phase: "share_failed"; message: string }

export class ProjectDeletingError extends Schema.TaggedErrorClass<ProjectDeletingError>()(
  "ProjectDeletingError",
  { projectID: ProjectV2.ID, phase: Schema.String },
  { httpApiStatus: 409 },
) {}

export class DeletionBusyError extends Schema.TaggedErrorClass<DeletionBusyError>()("DeletionBusyError", {
  message: Schema.String,
}) {}

export interface DeletionActions {
  readonly revokeShares: (projectID: ProjectV2.ID) => Effect.Effect<void, unknown>
  readonly cleanup: (projectID: ProjectV2.ID) => Effect.Effect<void, unknown>
  readonly publish: (projectID: ProjectV2.ID, eventID: EventV2.ID) => Effect.Effect<void, unknown>
}

export interface Interface {
  readonly begin: (projectID: ProjectV2.ID) => Effect.Effect<"owner" | "in_progress", NotFoundError | NotRemovableError>
  readonly retry: (projectID: ProjectV2.ID) => Effect.Effect<DeleteOutcome>
  readonly execute: (projectID: ProjectV2.ID) => Effect.Effect<DeleteOutcome>
  readonly runOwned: (projectID: ProjectV2.ID) => Effect.Effect<DeleteOutcome, NotFoundError | NotRemovableError>
  readonly assertWritable: (projectID: ProjectV2.ID) => Effect.Effect<void, ProjectDeletingError>
  readonly withMutation: <A, E, R>(
    projectID: ProjectV2.ID,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectDeletingError, R>
  readonly withLease: <A, E, R>(
    projectID: ProjectV2.ID,
    cancel: () => Effect.Effect<unknown, unknown>,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ProjectDeletingError, R>
  readonly awaitQuiescence: (projectID: ProjectV2.ID) => Effect.Effect<void>
  readonly recover: () => Effect.Effect<void>
  readonly prepareShutdown: () => Effect.Effect<void, DeletionBusyError>
  readonly install: (actions: DeletionActions) => void
  readonly ownerCount: () => Effect.Effect<number>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ProjectDeletionCoordinator") {}

type Lease = { cancel: () => Effect.Effect<unknown, unknown> }

export type MakeOptions = {
  readonly shutdownTimeout?: Duration.Input
  readonly recoveryAttempts?: number
  readonly recoveryDelay?: Duration.Input
  readonly afterPublish?: () => Effect.Effect<void, unknown>
  readonly worktreeBranch?: (directory: string) => Effect.Effect<string | null>
}

export function make(options: MakeOptions = {}) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const gates = KeyedMutex.makeUnsafe<string>()
    const owners = new Map<string, Deferred.Deferred<void>>()
    const closing = new Set<string>()
    const leases = new Map<string, Map<symbol, Lease>>()
    const waiters = new Map<string, Deferred.Deferred<void>>()
    let actions: DeletionActions | undefined
    let admissionClosed = false
    const worktreeBranch = options.worktreeBranch ?? (() => Effect.succeed(null))

    const phase = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      return yield* db
        .select({
          phase: ProjectDeletionJobTable.phase,
          lastError: ProjectDeletionJobTable.last_error,
          eventID: ProjectDeletionJobTable.event_id,
          eventDeliveredAt: ProjectDeletionJobTable.event_delivered_at,
        })
        .from(ProjectDeletionJobTable)
        .where(eq(ProjectDeletionJobTable.project_id, projectID))
        .get()
        .pipe(Effect.orDie)
    })

    const transition = Effect.fnUntraced(function* (
      projectID: ProjectV2.ID,
      next: DeletionPhase,
      error: string | null = null,
    ) {
      yield* db
        .transaction(
          (tx) =>
            tx
              .update(ProjectDeletionJobTable)
              .set({
                phase: next,
                last_error: error,
                updated_at: Date.now(),
                ...(error ? { attempt: sql`${ProjectDeletionJobTable.attempt} + 1` } : {}),
              })
              .where(eq(ProjectDeletionJobTable.project_id, projectID))
              .run(),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const finish = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      yield* db
        .transaction(
          (tx) =>
            Effect.gen(function* () {
              yield* tx
                .delete(ProjectDeletionShareTable)
                .where(eq(ProjectDeletionShareTable.project_id, projectID))
                .run()
              yield* tx
                .delete(ProjectDeletionWorktreeTable)
                .where(eq(ProjectDeletionWorktreeTable.project_id, projectID))
                .run()
              yield* tx
                .delete(ProjectDeletionArtifactTable)
                .where(eq(ProjectDeletionArtifactTable.project_id, projectID))
                .run()
              yield* tx.delete(ProjectDeletionJobTable).where(eq(ProjectDeletionJobTable.project_id, projectID)).run()
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const assertWritableUnlocked = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      const current = yield* phase(projectID)
      if (!admissionClosed && !closing.has(projectID) && !owners.has(projectID) && !current) return
      return yield* new ProjectDeletingError({
        projectID,
        phase: current?.phase ?? (admissionClosed ? "shutdown" : "requested"),
      })
    })

    const assertWritable = Effect.fn("ProjectDeletionCoordinator.assertWritable")(function* (projectID: ProjectV2.ID) {
      yield* gates.withLock(projectID)(assertWritableUnlocked(projectID))
    })

    const withMutation: Interface["withMutation"] = (projectID, effect) =>
      gates.withLock(projectID)(assertWritableUnlocked(projectID).pipe(Effect.andThen(effect)))

    const releaseLease = Effect.fnUntraced(function* (projectID: ProjectV2.ID, token: symbol) {
      const active = leases.get(projectID)
      if (!active) return
      active.delete(token)
      if (active.size > 0) return
      leases.delete(projectID)
      const waiter = waiters.get(projectID)
      if (!waiter) return
      waiters.delete(projectID)
      yield* Deferred.succeed(waiter, undefined)
    })

    const withLease: Interface["withLease"] = (projectID, cancel, effect) =>
      Effect.acquireUseRelease(
        gates.withLock(projectID)(
          Effect.gen(function* () {
            yield* assertWritableUnlocked(projectID)
            const token = Symbol(projectID)
            const active = leases.get(projectID) ?? new Map<symbol, Lease>()
            active.set(token, { cancel })
            leases.set(projectID, active)
            return token
          }),
        ),
        () => effect,
        (token) => releaseLease(projectID, token),
      )

    const awaitQuiescence = Effect.fn("ProjectDeletionCoordinator.awaitQuiescence")(function* (
      projectID: ProjectV2.ID,
    ) {
      closing.add(projectID)
      const active = leases.get(projectID)
      if (!active || active.size === 0) return
      const waiter = yield* Deferred.make<void>()
      waiters.set(projectID, waiter)
      yield* Effect.forEach(active.values(), (item) => item.cancel().pipe(Effect.ignore), { discard: true })
      if ((leases.get(projectID)?.size ?? 0) === 0) yield* Deferred.succeed(waiter, undefined)
      yield* Deferred.await(waiter)
    })

    const actionFailure = Effect.fnUntraced(function* (
      projectID: ProjectV2.ID,
      current: DeletionPhase,
      cause: Cause.Cause<unknown>,
    ) {
      if (Cause.hasInterruptsOnly(cause)) return yield* Effect.interrupt
      const message = Cause.pretty(cause)
      const next = current === "revoking_shares" ? "share_failed" : current
      yield* transition(projectID, next, message)
      return next === "share_failed"
        ? ({ status: "retryable_failure", phase: "share_failed", message } as const)
        : ({ status: "in_progress", phase: next } as const)
    })

    const deliver = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      const currentActions = actions
      if (!currentActions) return
      const job = yield* db
        .transaction(
          (tx) =>
            Effect.gen(function* () {
              const job = yield* tx
                .select({
                  eventID: ProjectDeletionJobTable.event_id,
                  deliveredAt: ProjectDeletionJobTable.event_delivered_at,
                })
                .from(ProjectDeletionJobTable)
                .where(eq(ProjectDeletionJobTable.project_id, projectID))
                .get()
              if (!job || job.deliveredAt !== null) return job
              const eventID = EventV2.ID.make(job.eventID ?? EventV2.ID.create())
              if (!job.eventID)
                yield* tx
                  .update(ProjectDeletionJobTable)
                  .set({ event_id: eventID })
                  .where(eq(ProjectDeletionJobTable.project_id, projectID))
                  .run()
              return { ...job, eventID }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      if (!job || job.deliveredAt !== null || !job.eventID) return
      yield* currentActions.publish(projectID, EventV2.ID.make(job.eventID))
      if (options.afterPublish) yield* options.afterPublish()
      yield* db
        .transaction(
          (tx) =>
            tx
              .update(ProjectDeletionJobTable)
              .set({ event_delivered_at: Date.now(), updated_at: Date.now() })
              .where(eq(ProjectDeletionJobTable.project_id, projectID))
              .run(),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const run = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      if (!actions) return { status: "in_progress", phase: (yield* phase(projectID))?.phase ?? "requested" } as const
      const initial = yield* phase(projectID)
      if (!initial) return { status: "completed" } as const
      if (initial.phase === "share_failed")
        return {
          status: "retryable_failure",
          phase: "share_failed",
          message: initial.lastError ?? "Share revocation failed",
        } as const

      if (initial.phase === "requested" || initial.phase === "revoking_shares") {
        yield* transition(projectID, "revoking_shares")
        const outcome = yield* actions.revokeShares(projectID).pipe(
          Effect.as<DeleteOutcome | undefined>(undefined),
          Effect.catchCause((cause) => actionFailure(projectID, "revoking_shares", cause)),
        )
        if (outcome) return outcome
        yield* transition(projectID, "quiescing")
      }

      if ((yield* phase(projectID))?.phase === "quiescing") {
        yield* awaitQuiescence(projectID)
        yield* transition(projectID, "cleaning")
      }

      if ((yield* phase(projectID))?.phase === "cleaning") {
        const outcome = yield* actions.cleanup(projectID).pipe(
          Effect.as<DeleteOutcome | undefined>(undefined),
          Effect.catchCause((cause) => actionFailure(projectID, "cleaning", cause)),
        )
        if (outcome) return outcome
        yield* transition(projectID, "cleanup_complete")
      }

      if ((yield* phase(projectID))?.phase === "cleanup_complete") {
        const outcome = yield* deliver(projectID).pipe(
          Effect.as<DeleteOutcome | undefined>(undefined),
          Effect.catchCause((cause) => actionFailure(projectID, "cleanup_complete", cause)),
        )
        if (outcome) return outcome
        yield* transition(projectID, "published")
      }
      yield* finish(projectID)
      return { status: "completed" } as const
    })

    const releaseOwner = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
      const done = owners.get(projectID)
      owners.delete(projectID)
      closing.delete(projectID)
      if (done) yield* Deferred.succeed(done, undefined)
    })

    const execute: Interface["execute"] = (projectID) => run(projectID).pipe(Effect.ensuring(releaseOwner(projectID)))

    const runOwned: Interface["runOwned"] = (projectID) =>
      Effect.acquireUseRelease(
        begin(projectID),
        (owner) =>
          owner === "owner"
            ? execute(projectID)
            : phase(projectID).pipe(
                Effect.map((current) => ({ status: "in_progress", phase: current?.phase ?? "requested" }) as const),
              ),
        (owner) => (owner === "owner" ? releaseOwner(projectID) : Effect.void),
      )

    const retry: Interface["retry"] = (projectID) =>
      Effect.acquireUseRelease(
        gates.withLock(projectID)(
          Effect.gen(function* () {
            if (admissionClosed || owners.has(projectID) || closing.has(projectID)) return "in_progress" as const
            const claimed = yield* db
              .transaction(
                (tx) =>
                  Effect.gen(function* () {
                    const current = yield* tx
                      .select({ phase: ProjectDeletionJobTable.phase })
                      .from(ProjectDeletionJobTable)
                      .where(eq(ProjectDeletionJobTable.project_id, projectID))
                      .get()
                    if (!current || current.phase !== "share_failed") return false
                    yield* tx
                      .update(ProjectDeletionJobTable)
                      .set({ phase: "revoking_shares", updated_at: Date.now() })
                      .where(
                        and(
                          eq(ProjectDeletionJobTable.project_id, projectID),
                          eq(ProjectDeletionJobTable.phase, "share_failed"),
                        ),
                      )
                      .run()
                    return true
                  }),
                { behavior: "immediate" },
              )
              .pipe(Effect.orDie)
            if (!claimed) return "in_progress" as const
            closing.add(projectID)
            owners.set(projectID, Deferred.makeUnsafe<void>())
            return "owner" as const
          }),
        ),
        (owner) =>
          owner === "owner"
            ? execute(projectID)
            : phase(projectID).pipe(
                Effect.map((current) => ({ status: "in_progress", phase: current?.phase ?? "requested" }) as const),
              ),
        (owner) => (owner === "owner" ? releaseOwner(projectID) : Effect.void),
      )

    const begin: Interface["begin"] = (projectID) =>
      Effect.suspend(() => {
        let installed = false
        return gates
          .withLock(projectID)(
            Effect.gen(function* () {
              if (admissionClosed || owners.has(projectID) || closing.has(projectID)) return "in_progress" as const
              closing.add(projectID)
              owners.set(projectID, Deferred.makeUnsafe<void>())
              installed = true
              return yield* db.transaction(
                (tx) =>
                  Effect.gen(function* () {
                    if (projectID === ProjectV2.ID.global) return yield* new NotRemovableError({ projectID })
                    const existing = yield* tx
                      .select({ phase: ProjectDeletionJobTable.phase })
                      .from(ProjectDeletionJobTable)
                      .where(eq(ProjectDeletionJobTable.project_id, projectID))
                      .get()
                    if (existing) return "in_progress" as const
                    const project = yield* tx.select().from(ProjectTable).where(eq(ProjectTable.id, projectID)).get()
                    if (!project) return yield* new NotFoundError({ projectID })
                    const now = Date.now()
                    yield* tx
                      .insert(ProjectDeletionJobTable)
                      .values({
                        project_id: projectID,
                        phase: "requested",
                        attempt: 0,
                        last_error: null,
                        event_id: EventV2.ID.create(),
                        event_delivered_at: null,
                        created_at: now,
                        updated_at: now,
                      })
                      .run()
                    const shares = yield* tx
                      .select({
                        sessionID: SessionShareTable.session_id,
                        shareID: SessionShareTable.id,
                        secret: SessionShareTable.secret,
                        baseUrl: SessionShareTable.url,
                      })
                      .from(SessionShareTable)
                      .innerJoin(SessionTable, eq(SessionTable.id, SessionShareTable.session_id))
                      .where(eq(SessionTable.project_id, projectID))
                      .all()
                    if (shares.length > 0)
                      yield* tx
                        .insert(ProjectDeletionShareTable)
                        .values(
                          shares.map((share) => ({
                            project_id: projectID,
                            session_id: share.sessionID,
                            share_id: share.shareID,
                            secret: share.secret,
                            base_url: new URL(share.baseUrl).origin,
                            status: "pending" as const,
                            attempt: 0,
                            last_error: null,
                            created_at: now,
                            updated_at: now,
                          })),
                        )
                        .run()
                    const workspaces = yield* tx
                      .select({ path: WorkspaceTable.directory, branch: WorkspaceTable.branch })
                      .from(WorkspaceTable)
                      .where(eq(WorkspaceTable.project_id, projectID))
                      .all()
                    const paths = [...project.sandboxes.map((path) => ({ path, branch: null })), ...workspaces]
                      .filter((item): item is { path: string; branch: string | null } => item.path !== null)
                      .flatMap((item) => {
                        try {
                          return [
                            {
                              ...item,
                              path: ownedProjectWorktreeTarget({
                                pathApi: path,
                                dataRoot: Global.Path.data,
                                projectID,
                                candidate: item.path,
                              }),
                            },
                          ]
                        } catch {
                          return []
                        }
                      })
                      .filter((item, index, all) => all.findIndex((other) => other.path === item.path) === index)
                    const ownedWorktrees = yield* Effect.forEach(paths, (item) =>
                      worktreeBranch(item.path).pipe(Effect.map((branch) => ({ ...item, branch }))),
                    )
                    if (ownedWorktrees.length > 0)
                      yield* tx
                        .insert(ProjectDeletionWorktreeTable)
                        .values(
                          ownedWorktrees.map((item) => ({
                            project_id: projectID,
                            canonical_path: item.path,
                            branch: item.branch,
                            attempt: 0,
                            last_error: null,
                            created_at: now,
                            updated_at: now,
                          })),
                        )
                        .run()
                    const sessions = yield* tx
                      .select({ id: SessionTable.id })
                      .from(SessionTable)
                      .where(eq(SessionTable.project_id, projectID))
                      .all()
                    const sessionIDs = sessions.map((session) => session.id)
                    const messages =
                      sessionIDs.length === 0
                        ? []
                        : yield* tx
                            .select({ id: MessageTable.id })
                            .from(MessageTable)
                            .where(inArray(MessageTable.session_id, sessionIDs))
                            .all()
                    const parts =
                      sessionIDs.length === 0
                        ? []
                        : yield* tx
                            .select({ id: PartTable.id })
                            .from(PartTable)
                            .where(inArray(PartTable.session_id, sessionIDs))
                            .all()
                    const artifacts = [
                      ...sessionIDs.map((artifactID) => ({ kind: "session_diff" as const, artifactID })),
                      ...messages.map(({ id: artifactID }) => ({ kind: "message" as const, artifactID })),
                      ...parts.map(({ id: artifactID }) => ({ kind: "part" as const, artifactID })),
                    ]
                    if (artifacts.length > 0)
                      yield* tx
                        .insert(ProjectDeletionArtifactTable)
                        .values(
                          artifacts.map((artifact) => ({
                            project_id: projectID,
                            kind: artifact.kind,
                            artifact_id: artifact.artifactID,
                          })),
                        )
                        .run()
                    return "owner" as const
                  }),
                { behavior: "immediate" },
              )
            }),
          )
          .pipe(
            Effect.onExit((exit) =>
              !installed || (Exit.isSuccess(exit) && exit.value === "owner") ? Effect.void : releaseOwner(projectID),
            ),
            Effect.catchTag("SqlError", Effect.die),
          ) as Effect.Effect<"owner" | "in_progress", NotFoundError | NotRemovableError>
      })

    const recover: Interface["recover"] = Effect.fn("ProjectDeletionCoordinator.recover")(function* () {
      if (!actions || admissionClosed) return
      const jobs = yield* db
        .select({ projectID: ProjectDeletionJobTable.project_id, phase: ProjectDeletionJobTable.phase })
        .from(ProjectDeletionJobTable)
        .all()
        .pipe(Effect.orDie)
      for (const job of jobs) {
        if (admissionClosed) return
        if (job.phase === "share_failed") continue
        const projectID = ProjectV2.ID.make(job.projectID)
        for (let attempt = 0; attempt < (options.recoveryAttempts ?? 3); attempt++) {
          if (owners.has(projectID)) break
          if (admissionClosed) return
          closing.add(projectID)
          owners.set(projectID, Deferred.makeUnsafe<void>())
          const outcome = yield* execute(projectID)
          if (outcome.status !== "in_progress") break
          if (attempt + 1 >= (options.recoveryAttempts ?? 3)) break
          yield* Effect.sleep(options.recoveryDelay ?? `${2 ** attempt * 25} millis`)
        }
      }
    })

    const prepareShutdown: Interface["prepareShutdown"] = Effect.fn("ProjectDeletionCoordinator.prepareShutdown")(
      function* () {
        admissionClosed = true
        const pending = [...owners.values()]
        if (pending.length === 0) return
        yield* Effect.forEach(pending, Deferred.await, { discard: true }).pipe(
          Effect.timeoutOrElse({
            duration: options.shutdownTimeout ?? "30 seconds",
            orElse: () =>
              new DeletionBusyError({ message: "Project deletion could not reach a durable shutdown boundary" }),
          }),
        )
      },
    )

    return Service.of({
      begin,
      retry,
      execute,
      runOwned,
      assertWritable,
      withMutation,
      withLease,
      awaitQuiescence,
      recover,
      prepareShutdown,
      install(next) {
        actions = next
      },
      ownerCount: () => Effect.sync(() => owners.size),
    })
  })
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    return yield* make({
      worktreeBranch: (directory) =>
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            ChildProcess.make("git", ["symbolic-ref", "--quiet", "--short", "HEAD"], {
              cwd: directory,
              extendEnv: true,
              stdin: "ignore",
            }),
          )
          const [text, stderr] = yield* Effect.all(
            [Stream.mkString(Stream.decodeText(handle.stdout)), Stream.mkString(Stream.decodeText(handle.stderr))],
            { concurrency: 2 },
          )
          const code = yield* handle.exitCode
          if (code === 0 && text.trim()) return text.trim()
          if (code === 1 && !text.trim() && !stderr.trim()) return null
          return yield* Effect.fail(new Error(`git branch snapshot failed for ${directory}: ${stderr || text}`))
        }).pipe(Effect.scoped),
    })
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node, CrossSpawnSpawner.node] })

export * as ProjectDeletionCoordinator from "./deletion-coordinator"
