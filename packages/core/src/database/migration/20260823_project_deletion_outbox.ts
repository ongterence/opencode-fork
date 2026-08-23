import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260823_project_deletion_outbox",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`project_deletion_job\` ADD \`event_id\` text;`)
      yield* tx.run(`ALTER TABLE \`project_deletion_job\` ADD \`event_delivered_at\` integer;`)
    })
  },
} satisfies DatabaseMigration.Migration
