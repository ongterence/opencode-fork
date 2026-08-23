import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260823_project_deletion_job",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`project_deletion_job\` (
          \`project_id\` text PRIMARY KEY NOT NULL,
          \`phase\` text NOT NULL,
          \`attempt\` integer NOT NULL,
          \`last_error\` text,
          \`created_at\` integer NOT NULL,
          \`updated_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS \`project_deletion_job_phase_idx\` ON \`project_deletion_job\` (\`phase\`);`)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`project_deletion_share\` (
          \`project_id\` text NOT NULL,
          \`session_id\` text NOT NULL,
          \`share_id\` text NOT NULL,
          \`secret\` text NOT NULL,
          \`base_url\` text NOT NULL,
          \`status\` text NOT NULL,
          \`attempt\` integer NOT NULL,
          \`last_error\` text,
          \`created_at\` integer NOT NULL,
          \`updated_at\` integer NOT NULL,
          PRIMARY KEY (\`project_id\`, \`session_id\`)
        );
      `)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS \`project_deletion_share_status_idx\` ON \`project_deletion_share\` (\`status\`);`)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`project_deletion_worktree\` (
          \`project_id\` text NOT NULL,
          \`canonical_path\` text NOT NULL,
          \`branch\` text,
          \`attempt\` integer NOT NULL,
          \`last_error\` text,
          \`created_at\` integer NOT NULL,
          \`updated_at\` integer NOT NULL,
          PRIMARY KEY (\`project_id\`, \`canonical_path\`)
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
