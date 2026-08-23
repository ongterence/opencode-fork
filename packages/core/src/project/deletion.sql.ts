import { index, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"

export type DeletionPhase =
  | "requested"
  | "revoking_shares"
  | "share_failed"
  | "quiescing"
  | "cleaning"
  | "cleanup_complete"
  | "published"

export type DeletionJob = {
  project_id: string
  phase: DeletionPhase
  attempt: number
  last_error: string | null
  created_at: number
  updated_at: number
}

export type DeletionShare = {
  project_id: string
  session_id: string
  share_id: string
  secret: string
  base_url: string
  status: "pending" | "revoked" | "failed"
  attempt: number
  last_error: string | null
}

export type DeletionWorktree = {
  project_id: string
  canonical_path: string
  branch: string | null
}

export const ProjectDeletionJobTable = sqliteTable(
  "project_deletion_job",
  {
    project_id: text().notNull().primaryKey(),
    phase: text().$type<DeletionPhase>().notNull(),
    attempt: integer().notNull(),
    last_error: text(),
    created_at: integer().notNull(),
    updated_at: integer().notNull(),
  },
  (table) => [index("project_deletion_job_phase_idx").on(table.phase)],
)

export const ProjectDeletionShareTable = sqliteTable(
  "project_deletion_share",
  {
    project_id: text().notNull(),
    session_id: text().notNull(),
    share_id: text().notNull(),
    secret: text().notNull(),
    base_url: text().notNull(),
    status: text().$type<DeletionShare["status"]>().notNull(),
    attempt: integer().notNull(),
    last_error: text(),
    created_at: integer().notNull(),
    updated_at: integer().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.project_id, table.session_id] }),
    index("project_deletion_share_status_idx").on(table.status),
  ],
)

export const ProjectDeletionWorktreeTable = sqliteTable(
  "project_deletion_worktree",
  {
    project_id: text().notNull(),
    canonical_path: text().notNull(),
    branch: text(),
    attempt: integer().notNull(),
    last_error: text(),
    created_at: integer().notNull(),
    updated_at: integer().notNull(),
  },
  (table) => [primaryKey({ columns: [table.project_id, table.canonical_path] })],
)
