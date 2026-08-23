import { ProjectV2 } from "@opencode-ai/core/project"
import { Schema } from "effect"

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("Project.NotFoundError", {
  projectID: ProjectV2.ID,
}) {}

export class NotRemovableError extends Schema.TaggedErrorClass<NotRemovableError>()("Project.NotRemovableError", {
  projectID: ProjectV2.ID,
}) {}
