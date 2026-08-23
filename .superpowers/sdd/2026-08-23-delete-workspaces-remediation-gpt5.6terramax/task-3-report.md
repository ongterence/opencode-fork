# Task 3 report: durable deletion coordinator, fence, and recovery

## Delivered behavior

- Added a process-global `ProjectDeletionCoordinator` Effect service whose SQLite journal is the durable source of truth.
- `begin(projectID)` installs process-local ownership synchronously, then uses an immediate SQLite transaction to reject `global`, reject a competing durable job, load the project, insert the job, and snapshot share credentials plus owned workspace/sandbox paths.
- The coordinator serializes durable phases (`requested` → `revoking_shares` → `quiescing` → `cleaning` → `cleanup_complete` → `published`), retains `share_failed`, increments attempt metadata on failed transitions, and removes all journal rows only after publication.
- Recovery resumes `revoking_shares`, `quiescing`, `cleaning`, `cleanup_complete`, and `published` jobs from a fresh coordinator. A `published` recovery removes the journal without re-emitting; `cleanup_complete` emits once before advancing.
- Added cancellable leases for admitted/running prompt work and remote workspace sync. Quiescence closes admission, calls existing cancellation paths, and waits for leases to drain before cleanup.
- Replaced the transient Project deletion `Set` with coordinator admission checks at project upsert/update, session creation/message/prompt mutation, workspace creation/sync admission, and share creation. Checks are repeated immediately before commits that follow asynchronous work.
- `ProjectRemoval` now registers the concrete share-revocation, local-cleanup, and publication actions, starts supervised recovery in its layer scope, and routes new/repeated deletion requests through the coordinator.
- Added the stable `409` global-delete response `project_deletion_in_progress`, including the durable phase.

The project-wide contract from the ledger is preserved: deleting the desktop project row removes all OpenCode-owned workspaces and history for that Project; no individual-workspace deletion API or semantics were added.

## TDD evidence

### Initial red

From `packages/opencode`:

```text
$ bun test test/project/deletion-coordinator.test.ts
error: Cannot find module '../../src/project/deletion-coordinator'
0 pass
1 fail
1 error
```

This was the expected failure before any coordinator production module existed.

### First green and barrier feedback

After the first minimal implementation, three behaviors passed and the lease-drain barrier caught an Effect-version API error:

```text
$ bun test test/project/deletion-coordinator.test.ts
TypeError: Deferred.succeedUnsafe is not a function
3 pass
1 fail
```

The release operation was changed to execute `Deferred.succeed` inside the returned release Effect. The scoped suite then passed:

```text
$ bun test test/project/deletion-coordinator.test.ts
4 pass
0 fail
16 expect() calls
```

### Recovery mutation red/green

The final recovery audit added a `published` journal seed. Before the production correction:

```text
$ bun test test/project/deletion-coordinator.test.ts
Expected: []
Received: [{ phase: "published", project_id: "proj_recover_3", ... }]
3 pass
1 fail
```

Recovery was corrected to finalize `published` rows without invoking publish again. This protects the crash boundary after the published transition but before journal deletion.

## Final verification

From `packages/opencode`:

```text
$ bun test test/project/deletion-coordinator.test.ts test/server/project-global-delete.test.ts && bun typecheck
7 pass
0 fail
30 expect() calls
$ tsgo --noEmit
```

Targeted touched-service regressions:

```text
$ bun test test/session/session.test.ts test/control-plane/workspace.test.ts test/share/share-next.test.ts
Workspace: 34 pass, 1 skip
Session: 8 pass

$ bun test test/share/share-next.test.ts
7 pass
0 fail
34 expect() calls
```

The combined targeted command reached its execution timeout only after every Workspace and Session case printed as passing and while starting ShareNext, so ShareNext was rerun separately to a clean exit.

## Changed files

- Created `packages/opencode/src/project/deletion-coordinator.ts`.
- Created `packages/opencode/src/project/project-errors.ts`.
- Created `packages/opencode/test/project/deletion-coordinator.test.ts`.
- Modified `packages/opencode/src/project/project.ts`.
- Modified `packages/opencode/src/project/removal.ts`.
- Modified `packages/opencode/src/session/session.ts`.
- Modified `packages/opencode/src/session/prompt.ts`.
- Modified `packages/opencode/src/control-plane/workspace.ts`.
- Modified `packages/opencode/src/share/share-next.ts`.
- Modified `packages/opencode/src/server/routes/instance/httpapi/errors.ts`.
- Modified `packages/opencode/src/server/routes/instance/httpapi/groups/global.ts`.
- Modified `packages/opencode/src/server/routes/instance/httpapi/handlers/global.ts`.
- Modified `packages/opencode/test/server/project-global-delete.test.ts`.

The pre-existing user modifications in `packages/app/src/custom-elements.d.ts` and `packages/enterprise/src/custom-elements.d.ts` were not edited or staged.

## Necessary deviations from the brief's narrow file list

- `project-errors.ts` is a dependency-neutral home for the existing `Project.NotFoundError` and `Project.NotRemovableError` tags. Keeping either error in `project.ts`/`removal.ts` created a real ESM initialization cycle once those layers depended on the coordinator. Public tags and re-exports remain unchanged.
- `project/removal.ts` had to register the production phase actions and start recovery; otherwise the coordinator would be an in-memory/test-only shell rather than the durable production authority required by the brief.
- `session/prompt.ts` and `share/share-next.ts` were changed because the acceptance criteria explicitly fence prompt admission and share creation, while those commit boundaries do not live in `session.ts`.
- The three global HttpApi files were changed narrowly to make a competing durable deletion observable as the specified `409`, rather than leaking a typed service error as a server failure. The broader retry/status SDK work remains Task 6.

## Concerns and follow-on boundaries

- Task 4 still owns credential-based historical share revocation. Task 3 snapshots every credential durably and orders revocation before local cleanup, but the registered production action intentionally continues through the existing `ShareNext.remove` path until Task 4 separates disabled creation/sync from historical revocation and records per-share results.
- Task 5 still owns the complete contained mandatory cleanup rewrite. The coordinator preserves non-share cleanup failures at the current durable phase and will retry them, but the existing cleanup action remains the landed best-effort implementation until Task 5 replaces every raw legacy target and swallowed mandatory error.
- `prepareShutdown` closes mutation admission and waits for all keyed owners. The coordinator contains no unjournaled destructive critical section; Task 6 will expose this boundary to the sidecar/updater lifecycle and add bounded user-facing failure behavior.
- The coordinator fence is process-local before the journal transaction and durable after it. This application currently has one writer process; a future multi-process writer would need the mutation itself and the journal check to share a database transaction or an equivalent cross-process admission lock.
