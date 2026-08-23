import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260823124644_20260823_project_deletion_artifact",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`project_deletion_artifact\` (
          \`project_id\` text NOT NULL,
          \`kind\` text NOT NULL,
          \`artifact_id\` text NOT NULL,
          CONSTRAINT \`project_deletion_artifact_pk\` PRIMARY KEY(\`project_id\`, \`kind\`, \`artifact_id\`)
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
