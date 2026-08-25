import { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { EventV2 } from "@opencode-ai/core/event"
import { ProjectV2 } from "@opencode-ai/core/project"
import { EventManifest } from "@/event-manifest"
import { InstanceDisposed } from "@/server/event"
import "@opencode-ai/core/account"
import "@/server/event"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/unstable/httpapi"
import semver from "semver"
import {
  ProjectDeletionInProgressError,
  ProjectDeletionRetryNotAllowedError,
  ProjectDeletionRetryableError,
  ProjectDeletionShutdownBusyError,
  ProjectNotFoundError,
  ProjectNotRemovableError,
} from "../errors"
import { described } from "./metadata"

const GlobalHealth = Schema.Struct({
  healthy: Schema.Literal(true),
  version: Schema.String,
})

const SyncEventSchemas = EventManifest.Latest.values()
  .flatMap((definition) => {
    if (!definition.durable) return []
    return [
      Schema.Struct({
        type: Schema.Literal("sync"),
        id: EventV2.ID,
        syncEvent: Schema.Struct({
          type: Schema.Literal(EventV2.versionedType(definition.type, definition.durable.version)),
          id: EventV2.ID,
          seq: Schema.Finite,
          aggregateID: Schema.String,
          data: definition.data,
        }),
      }).annotate({ identifier: `SyncEvent.${definition.type}` }),
    ]
  })
  .toArray()

const GlobalEventSchema = Schema.Struct({
  directory: Schema.String,
  project: Schema.optional(Schema.String),
  workspace: Schema.optional(Schema.String),
  payload: Schema.Union([
    ...EventManifest.Latest.values()
      .map((definition) =>
        Schema.Struct({ id: EventV2.ID, type: Schema.Literal(definition.type), properties: definition.data }),
      )
      .toArray(),
    InstanceDisposed,
    ...SyncEventSchemas,
  ]),
}).annotate({ identifier: "GlobalEvent" })

export const GlobalUpgradeInput = Schema.Struct({
  target: Schema.String.check(
    Schema.makeFilter((value) => (semver.valid(value) === null ? "Expected a semantic version" : undefined)),
  ),
})

const GlobalUpgradeResult = Schema.Union([
  Schema.Struct({
    success: Schema.Literal(true),
    version: Schema.String,
  }),
  Schema.Struct({
    success: Schema.Literal(false),
    error: Schema.String,
  }),
])

export const GlobalPaths = {
  health: "/global/health",
  event: "/global/event",
  config: "/global/config",
  dispose: "/global/dispose",
  upgrade: "/global/upgrade",
  projectDelete: "/global/project/:projectID",
  projectDeleteRetry: "/global/project/:projectID/delete/retry",
  projectDeletePrepareShutdown: "/global/project/delete/prepare-shutdown",
} as const

export const GlobalApi = HttpApi.make("global").add(
  HttpApiGroup.make("global")
    .add(
      HttpApiEndpoint.get("health", GlobalPaths.health, {
        success: described(GlobalHealth, "Health information"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.health",
          summary: "Get health",
          description: "Get health information about the OpenCode server.",
        }),
      ),
      HttpApiEndpoint.get("event", GlobalPaths.event, {
        success: GlobalEventSchema,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.event",
          summary: "Get global events",
          description: "Subscribe to global events from the OpenCode system using server-sent events.",
        }),
      ),
      HttpApiEndpoint.get("configGet", GlobalPaths.config, {
        success: described(ConfigV1.Info, "Get global config info"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.config.get",
          summary: "Get global configuration",
          description: "Retrieve the current global OpenCode configuration settings and preferences.",
        }),
      ),
      HttpApiEndpoint.patch("configUpdate", GlobalPaths.config, {
        payload: ConfigV1.Info,
        success: described(ConfigV1.Info, "Successfully updated global config"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.config.update",
          summary: "Update global configuration",
          description: "Update global OpenCode configuration settings and preferences.",
        }),
      ),
      HttpApiEndpoint.post("dispose", GlobalPaths.dispose, {
        success: described(Schema.Boolean, "Global disposed"),
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.dispose",
          summary: "Dispose instance",
          description: "Clean up and dispose all OpenCode instances, releasing all resources.",
        }),
      ),
      HttpApiEndpoint.post("upgrade", GlobalPaths.upgrade, {
        payload: GlobalUpgradeInput,
        success: described(GlobalUpgradeResult, "Upgrade result"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.upgrade",
          summary: "Upgrade opencode",
          description: "Upgrade opencode to the specified version.",
        }),
      ),
      HttpApiEndpoint.delete("projectDelete", GlobalPaths.projectDelete, {
        params: { projectID: ProjectV2.ID },
        success: HttpApiSchema.NoContent,
        error: [ProjectNotFoundError, ProjectNotRemovableError, ProjectDeletionInProgressError, ProjectDeletionRetryableError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.project.delete",
          summary: "Delete a project",
          description:
            "Permanently remove all OpenCode-owned data for a project: sessions, history, workspaces, permissions and caches under the OpenCode data directory. Never deletes files inside the project directory.",
        }),
      ),
      HttpApiEndpoint.post("projectDeleteRetry", GlobalPaths.projectDeleteRetry, {
        params: { projectID: ProjectV2.ID },
        success: HttpApiSchema.NoContent,
        error: [
          ProjectNotFoundError,
          ProjectNotRemovableError,
          ProjectDeletionInProgressError,
          ProjectDeletionRetryableError,
          ProjectDeletionRetryNotAllowedError,
        ],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.project.delete.retry",
          summary: "Retry failed project deletion share revocation",
          description: "Retries a project deletion only after a retained remote share revocation failure.",
        }),
      ),
      HttpApiEndpoint.post("projectDeletePrepareShutdown", GlobalPaths.projectDeletePrepareShutdown, {
        success: HttpApiSchema.NoContent,
        error: ProjectDeletionShutdownBusyError,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "global.project.delete.prepareShutdown",
          summary: "Prepare project deletion shutdown",
          description: "Stop accepting project deletions and wait for active deletion owners to reach a durable boundary.",
        }),
      ),
    )
    .annotateMerge(OpenApi.annotations({ title: "global", description: "Global server routes." })),
)
