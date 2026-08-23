import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import {
  type DeletionPhase,
  ProjectDeletionJobTable,
  ProjectDeletionShareTable,
  ProjectDeletionWorktreeTable,
} from "@opencode-ai/core/project/deletion.sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionShareTable } from "@opencode-ai/core/share/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { eq, sql } from "drizzle-orm"
import { Cause, Context, Deferred, Effect, Layer, Schema } from "effect"
import { NotFoundError, NotRemovableError } from "./project-errors"

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
  readonly publish: (projectID: ProjectV2.ID) => Effect.Effect<void, unknown>
}

export interface Interface {
  readonly begin: (
    projectID: ProjectV2.ID,
  ) => Effect.Effect<"owner" | "in_progress", NotFoundError | NotRemovableError>
  readonly execute: (projectID: ProjectV2.ID) => Effect.Effect<DeleteOutcome>
  readonly assertWritable: (projectID: ProjectV2.ID) => Effect.Effect<void, ProjectDeletingError>
  readonly awaitQuiescence: (projectID: ProjectV2.ID) => Effect.Effect<void>
  readonly recover: () => Effect.Effect<void>
  readonly prepareShutdown: () => Effect.Effect<void, DeletionBusyError>
  readonly install: (actions: DeletionActions) => void
  readonly lease: (
    projectID: ProjectV2.ID,
    key: string,
    cancel: () => Effect.Effect<unknown, unknown>,
  ) => Effect.Effect<Effect.Effect<void>, ProjectDeletingError>
  readonly ownerCount: () => Effect.Effect<number>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ProjectDeletionCoordinator") {}

type Lease = { cancel: () => Effect.Effect<unknown, unknown> }

export function make() {
  return Effect.gen(function* () {
  const { db } = yield* Database.Service
  const owners = new Set<string>()
  const closing = new Set<string>()
  const leases = new Map<string, Map<string, Lease>>()
  const waiters = new Map<string, Deferred.Deferred<void>>()
  let actions: DeletionActions | undefined
  let admissionClosed = false

  const phase = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
    return yield* db
      .select({ phase: ProjectDeletionJobTable.phase, lastError: ProjectDeletionJobTable.last_error })
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
            yield* tx.delete(ProjectDeletionShareTable).where(eq(ProjectDeletionShareTable.project_id, projectID)).run()
            yield* tx
              .delete(ProjectDeletionWorktreeTable)
              .where(eq(ProjectDeletionWorktreeTable.project_id, projectID))
              .run()
            yield* tx.delete(ProjectDeletionJobTable).where(eq(ProjectDeletionJobTable.project_id, projectID)).run()
          }),
        { behavior: "immediate" },
      )
      .pipe(Effect.orDie)
  })

  const assertWritable = Effect.fn("ProjectDeletionCoordinator.assertWritable")(function* (
    projectID: ProjectV2.ID,
  ) {
    const current = yield* phase(projectID)
    if (!admissionClosed && !closing.has(projectID) && !owners.has(projectID) && !current) return
    return yield* new ProjectDeletingError({
      projectID,
      phase: current?.phase ?? (admissionClosed ? "shutdown" : "requested"),
    })
  })

  const lease = Effect.fn("ProjectDeletionCoordinator.lease")(function* (
    projectID: ProjectV2.ID,
    key: string,
    cancel: () => Effect.Effect<unknown, unknown>,
  ) {
    yield* assertWritable(projectID)
    if (closing.has(projectID) || owners.has(projectID))
      return yield* new ProjectDeletingError({ projectID, phase: "requested" })
    const active = leases.get(projectID) ?? new Map<string, Lease>()
    active.set(key, { cancel })
    leases.set(projectID, active)
    let released = false
    return Effect.gen(function* () {
      if (released) return
      released = true
      active.delete(key)
      if (active.size > 0) return
      leases.delete(projectID)
      const waiter = waiters.get(projectID)
      if (!waiter) return
      waiters.delete(projectID)
      yield* Deferred.succeed(waiter, undefined)
    })
  })

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

  const run = Effect.fnUntraced(function* (projectID: ProjectV2.ID) {
    if (!actions) return { status: "in_progress", phase: (yield* phase(projectID))?.phase ?? "requested" } as const
    const initial = yield* phase(projectID)
    if (!initial) return { status: "completed" } as const
    if (initial.phase === "share_failed")
      return { status: "retryable_failure", phase: "share_failed", message: initial.lastError ?? "Share revocation failed" } as const

    if (initial.phase === "requested" || initial.phase === "revoking_shares") {
      yield* transition(projectID, "revoking_shares")
      const revoked = yield* actions.revokeShares(projectID).pipe(Effect.exit)
      if (revoked._tag === "Failure") {
        const message = Cause.pretty(revoked.cause)
        yield* transition(projectID, "share_failed", message)
        return { status: "retryable_failure", phase: "share_failed", message } as const
      }
      yield* transition(projectID, "quiescing")
    }

    const beforeCleanup = yield* phase(projectID)
    if (beforeCleanup?.phase === "quiescing") {
      yield* awaitQuiescence(projectID)
      yield* transition(projectID, "cleaning")
    }

    const cleaning = yield* phase(projectID)
    if (cleaning?.phase === "cleaning") {
      yield* actions.cleanup(projectID)
      yield* transition(projectID, "cleanup_complete")
    }

    const complete = yield* phase(projectID)
    if (complete?.phase === "cleanup_complete") {
      yield* actions.publish(projectID)
      yield* transition(projectID, "published")
    }
    yield* finish(projectID)
    return { status: "completed" } as const
  })

  const execute: Interface["execute"] = (projectID) =>
    run(projectID).pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const current = yield* phase(projectID)
          if (!current) return { status: "completed" } as const
          yield* transition(projectID, current.phase, Cause.pretty(cause))
          return { status: "in_progress", phase: current.phase } as const
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          owners.delete(projectID)
          closing.delete(projectID)
        }),
      ),
    )

  const begin: Interface["begin"] = (projectID) => {
    if (owners.has(projectID) || closing.has(projectID)) return Effect.succeed("in_progress" as const)
    closing.add(projectID)
    owners.add(projectID)
    return db
      .transaction(
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
              .values({ project_id: projectID, phase: "requested", attempt: 0, last_error: null, created_at: now, updated_at: now })
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
                    base_url: share.baseUrl,
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
              .filter((item, index, all) => all.findIndex((other) => other.path === item.path) === index)
            if (paths.length > 0)
              yield* tx
                .insert(ProjectDeletionWorktreeTable)
                .values(
                  paths.map((item) => ({
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
            return "owner" as const
          }),
        { behavior: "immediate" },
      )
      .pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            if (result === "owner") return
            owners.delete(projectID)
            closing.delete(projectID)
          }),
        ),
        Effect.tapError(() =>
          Effect.sync(() => {
            owners.delete(projectID)
            closing.delete(projectID)
          }),
        ),
        Effect.catchTag("SqlError", Effect.die),
      ) as Effect.Effect<"owner" | "in_progress", NotFoundError | NotRemovableError>
  }

  const recover: Interface["recover"] = Effect.fn("ProjectDeletionCoordinator.recover")(function* () {
    if (!actions) return
    const jobs = yield* db
      .select({ projectID: ProjectDeletionJobTable.project_id, phase: ProjectDeletionJobTable.phase })
      .from(ProjectDeletionJobTable)
      .all()
      .pipe(Effect.orDie)
    for (const job of jobs) {
      if (job.phase === "share_failed") continue
      const projectID = ProjectV2.ID.make(job.projectID)
      if (owners.has(projectID)) continue
      owners.add(projectID)
      closing.add(projectID)
      yield* execute(projectID).pipe(
        Effect.catchCause((cause) => Effect.logError("project deletion recovery failed", { projectID, cause })),
      )
    }
  })

  const prepareShutdown: Interface["prepareShutdown"] = Effect.fn("ProjectDeletionCoordinator.prepareShutdown")(function* () {
    admissionClosed = true
    while (owners.size > 0) yield* Effect.sleep("10 millis")
  })

  return Service.of({
    begin,
    execute,
    assertWritable,
    awaitQuiescence,
    recover,
    prepareShutdown,
    install(next) {
      actions = next
    },
    lease,
    ownerCount: () => Effect.sync(() => owners.size),
  })
  })
}

const layer = Layer.effect(Service, make())

export const node = LayerNode.make({ service: Service, layer, deps: [Database.node] })

export * as ProjectDeletionCoordinator from "./deletion-coordinator"
