// Opencode publish boundary for core events. Attach routed instance location
// so direct EventV2 consumers can isolate directory/workspace streams.
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceRef, WorkspaceRef } from "@/effect/instance-ref"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventTable } from "@opencode-ai/core/event/sql"
import { eq } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"

export interface Interface extends EventV2.Interface {
  readonly deliverProjectDeleted: (projectID: string, eventID: EventV2.ID) => Effect.Effect<void, Error>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/EventV2Bridge") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const { db } = yield* Database.Service
    const delivered = new Set<EventV2.ID>()

    const publish: EventV2.Interface["publish"] = (definition, data, options) =>
      Effect.gen(function* () {
        if (options?.location) return yield* events.publish(definition, data, options)
        const ctx = yield* InstanceRef
        if (!ctx) return yield* events.publish(definition, data, options)
        const workspaceID = yield* WorkspaceRef
        return yield* events.publish(definition, data, {
          ...options,
          location: new Location.Info({
            directory: AbsolutePath.make(ctx.directory),
            ...(workspaceID ? { workspaceID } : {}),
            project: { id: Project.ID.make(ctx.project.id), directory: AbsolutePath.make(ctx.worktree) },
          }),
        })
      })

    const forward = Effect.fn("EventV2Bridge.forward")(function* (event: EventV2.Payload) {
      const idempotent = event.type === "project.deleted" && event.durable !== undefined
      if (idempotent && delivered.has(event.id)) return
      if (idempotent) delivered.add(event.id)
      const ctx = yield* InstanceRef
      const workspaceID = (yield* WorkspaceRef) ?? event.location?.workspaceID
      const global = event.metadata?.global === true
      const project = typeof event.metadata?.project === "string" ? event.metadata.project : ctx?.project.id
      GlobalBus.emit("event", {
        directory: global ? "global" : (event.location?.directory ?? ctx?.directory),
        project,
        workspace: workspaceID,
        payload: { id: event.id, type: event.type, properties: event.data },
      })
      if (event.durable === undefined) return
      GlobalBus.emit("event", {
        directory: global ? "global" : (event.location?.directory ?? ctx?.directory),
        project,
        workspace: workspaceID,
        payload: {
          type: "sync",
          syncEvent: {
            id: event.id,
            type: EventV2.versionedType(event.type, event.durable.version),
            seq: event.durable.seq,
            aggregateID: event.durable.aggregateID,
            data: event.data,
          },
        },
      })
    })

    const unsubscribe = yield* events.listen(forward)
    yield* Effect.addFinalizer(() => unsubscribe)

    const deliverProjectDeleted = Effect.fn("EventV2Bridge.deliverProjectDeleted")(function* (
      projectID: string,
      eventID: EventV2.ID,
    ) {
      const stored = yield* db
        .select({
          id: EventTable.id,
          aggregateID: EventTable.aggregate_id,
          seq: EventTable.seq,
          type: EventTable.type,
          data: EventTable.data,
        })
        .from(EventTable)
        .where(eq(EventTable.id, eventID))
        .get()
        .pipe(Effect.orDie)
      if (!stored)
        return yield* Effect.fail(new Error(`Durable project deletion event is missing: ${eventID}`))
      if (stored.type !== "project.deleted.1" || stored.data.id !== projectID)
        return yield* Effect.fail(new Error(`Durable project deletion event does not match project: ${eventID}`))
      yield* forward({
        id: stored.id,
        type: "project.deleted",
        data: stored.data,
        metadata: { global: true, project: projectID },
        durable: { aggregateID: stored.aggregateID, seq: stored.seq, version: 1 },
      })
    })

    return Service.of({ ...events, publish, deliverProjectDeleted })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2.node, Database.node] })

export * as EventV2Bridge from "./event-v2-bridge"
