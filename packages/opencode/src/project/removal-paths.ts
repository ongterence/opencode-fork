import { createHash } from "node:crypto"
import path from "node:path"

type PathApi = typeof import("node:path").posix

export class UnsafeLegacyMetadataError extends Error {
  readonly code = "unsafe_legacy_metadata"

  constructor(label: "project" | "session" | "message" | "part") {
    super(`Unsafe legacy ${label} identifier`)
  }
}

export function opaqueStorageKey(identity: string): string {
  return createHash("sha256").update(identity, "utf8").digest("hex")
}

export function requireLegacyLeaf(value: string, label: "project" | "session" | "message" | "part"): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,191}$/.test(value)) throw new UnsafeLegacyMetadataError(label)
  return value
}

export function requireStrictDescendant(pathApi: PathApi, root: string, candidate: string): string {
  const resolvedRoot = pathApi.resolve(root)
  const resolvedCandidate = pathApi.resolve(candidate)
  const relative = pathApi.relative(resolvedRoot, resolvedCandidate)

  if (
    relative.length === 0 ||
    pathApi.isAbsolute(relative) ||
    relative === ".." ||
    relative.startsWith(`..${pathApi.sep}`)
  )
    throw new Error(`Deletion target must be a strict descendant of ${resolvedRoot}`)

  return resolvedCandidate
}

export function deletionTarget(input: {
  pathApi: PathApi
  dataRoot: string
  category: "snapshot" | "worktree" | "journal-artifact"
  projectID: string
  relatedID?: string
}): string {
  const target = input.pathApi.join(
    input.dataRoot,
    "project-artifacts",
    "v1",
    opaqueStorageKey(input.projectID),
    input.category,
    ...(input.relatedID === undefined ? [] : [opaqueStorageKey(input.relatedID)]),
  )
  return requireStrictDescendant(input.pathApi, input.dataRoot, target)
}

export function ownedWorktreeTarget(input: {
  pathApi: PathApi
  dataRoot: string
  projectID: string
  candidate: string
}): string {
  const root = deletionTarget({
    pathApi: input.pathApi,
    dataRoot: input.dataRoot,
    category: "worktree",
    projectID: input.projectID,
  })
  return requireStrictDescendant(input.pathApi, root, input.candidate)
}

export function legacyDeletionTarget(input: {
  pathApi: PathApi
  dataRoot: string
  category: "storage-project" | "storage-session" | "storage-session-diff" | "storage-message" | "storage-part" | "snapshot" | "worktree"
  projectID?: string
  relatedID?: string
}): string {
  const projectID =
    input.category === "storage-project" ||
    input.category === "storage-session" ||
    input.category === "snapshot" ||
    input.category === "worktree"
      ? requireLegacyLeaf(input.projectID ?? "", "project")
      : undefined
  const related =
    input.category === "storage-session-diff"
      ? requireLegacyLeaf(input.relatedID ?? "", "session")
      : input.category === "storage-message"
        ? requireLegacyLeaf(input.relatedID ?? "", "message")
        : input.category === "storage-part"
          ? requireLegacyLeaf(input.relatedID ?? "", "part")
          : undefined
  const target =
    input.category === "storage-project"
      ? input.pathApi.join(input.dataRoot, "storage", "project", `${projectID}.json`)
      : input.category === "storage-session"
        ? input.pathApi.join(input.dataRoot, "storage", "session", projectID ?? "")
        : input.category === "storage-session-diff"
          ? input.pathApi.join(input.dataRoot, "storage", "session_diff", `${related}.json`)
          : input.category === "storage-message"
            ? input.pathApi.join(input.dataRoot, "storage", "message", related ?? "")
            : input.category === "storage-part"
              ? input.pathApi.join(input.dataRoot, "storage", "part", related ?? "")
              : input.category === "snapshot"
                ? input.pathApi.join(input.dataRoot, "snapshot", projectID ?? "")
                : input.pathApi.join(input.dataRoot, "worktree", projectID ?? "")

  return requireStrictDescendant(input.pathApi, input.dataRoot, target)
}
