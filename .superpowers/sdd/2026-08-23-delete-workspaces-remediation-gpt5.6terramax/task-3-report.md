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

---

## Fix round 1 — commit-boundary fencing and recovery hardening

### Review findings reproduced

The coordinator tests were expanded before the implementation changed. From `packages/opencode`:

```text
$ bun test test/project/deletion-coordinator.test.ts
4 pass
6 fail
```

The failures were the intended red evidence:

- `coordinator.withLease is not a function` for concurrent and interrupted lease cases.
- `coordinator.withMutation is not a function` for the commit-boundary race.
- bounded recovery expected three cleanup attempts but observed one.
- the publish-before-transition injection observed terminal completion instead of retaining `cleanup_complete`.
- the shutdown-boundary test timed out because owner drain had no bound.

The existing code inspection also confirmed every reported cause: `assertWritable` was a separate read before the write, caller-provided lease keys collided, publication had no durable delivery marker, local recovery ran once, shutdown polled indefinitely, cleanup swallowed required failures, and mutation fences were converted to defects with `Effect.orDie`.

### Implemented corrections

- Replaced the check-then-write path with `withMutation`, backed by the existing keyed mutex. The durable journal check and the mutation effect now execute under the same per-project gate. Deletion installs `closing` synchronously, then takes that same gate for the immediate snapshot transaction.
- Added `withLease` using `Effect.acquireUseRelease` and a unique `Symbol` token per admitted operation. Acquisition installs the lease before releasing the gate and the finalizer is guaranteed on success, failure, or interruption.
- Added owner-completion `Deferred`s. `execute` releases ownership on every exit, and `prepareShutdown` waits on those completions with a bounded timeout that fails with `DeletionBusyError`.
- Added bounded, delayed recovery retries for non-share local phases. `share_failed` remains intentionally user-retryable and is not auto-purged.
- Added durable `event_id` and `event_delivered_at` columns through the narrowly required `20260823_project_deletion_outbox` migration. Publication receives a stable event id and records synchronous delivery before the later phase transition; restart skips an already delivered event.
- Removed best-effort swallowing at the current mandatory cleanup boundary. Filesystem removal, git worktree/branch removal, workspace adapter removal, and session/workspace cleanup failures now stop the coordinator in its durable local phase. No terminal event or journal deletion occurs.
- Routed project upsert/update, session create/update, prompt admission, workspace create/discovery commits, and share creation through the coordinator gates. Prompt and workspace sync use the scoped unique-lease API and existing cancellation callbacks.
- Preserved `ProjectDeletingError` in public service channels and mapped it to `ProjectDeletionInProgressError` (`409`, code `project_deletion_in_progress`) in project, session, prompt, workspace, and share HTTP mutation routes. Internal already-admitted streaming writers convert a later fence to interruption so cancellation remains the control path; no deletion fence is converted to a defect.

### Green verification

Coordinator and real HTTP mutation mapping, from `packages/opencode`:

```text
$ bun test test/project/deletion-coordinator.test.ts test/server/project-global-delete.test.ts
14 pass
0 fail
61 expect() calls
Ran 14 tests across 2 files. [8.49s]
```

The HTTP barrier test creates a session, seeds a durable `quiescing` job, concurrently attempts project update, session create, session update, prompt admission, workspace create, and share create, and observes six typed 409 responses. It also verifies the project update did not commit.

Touched-service regression suite, from `packages/opencode`:

```text
$ bun test --timeout 15000 test/project/deletion-coordinator.test.ts test/server/project-global-delete.test.ts test/session/session.test.ts test/control-plane/workspace.test.ts test/share/share-next.test.ts
63 pass
1 skip
0 fail
238 expect() calls
Ran 64 tests across 5 files. [46.86s]
```

The pre-existing 5-second timeout was insufficient for two live HTTP tests on this Windows checkout. Both were rerun with the same 15-second test timeout used above:

```text
$ bun test --timeout 15000 test/server/httpapi-workspace.test.ts -t "creates a real git worktree"
1 pass
0 fail

$ bun test --timeout 15000 test/server/httpapi-session.test.ts -t "persisted session directory"
1 pass
0 fail
```

Core migration verification, from `packages/core`:

```text
$ bun test test/database/project-deletion-job.test.ts; bun run typecheck
2 pass
0 fail
9 expect() calls
$ tsgo --noEmit
```

Project-wide verification, from the repository root:

```text
$ bun run typecheck
Tasks:    30 successful, 30 total
Cached:   28 cached, 30 total
Time:     32.362s
```

Targeted lint reported zero errors (`44 warnings`, all in existing broad files and predominantly pre-existing unsafe-assertion/unused-import findings).

### Fix-round changed files

- Core durable outbox: `packages/core/src/project/deletion.sql.ts`, `packages/core/src/database/migration/20260823_project_deletion_outbox.ts`, `packages/core/src/database/migration.gen.ts`, `packages/core/src/database/schema.gen.ts`, `packages/core/schema.json`, and `packages/core/test/database/project-deletion-job.test.ts`.
- Coordinator and cleanup: `packages/opencode/src/project/deletion-coordinator.ts`, `packages/opencode/src/project/removal.ts`.
- Commit gates/admission: `packages/opencode/src/project/project.ts`, `packages/opencode/src/project/instance-store.ts`, `packages/opencode/src/session/session.ts`, `packages/opencode/src/session/prompt.ts`, `packages/opencode/src/control-plane/workspace.ts`, `packages/opencode/src/share/share-next.ts`, `packages/opencode/src/share/session.ts`, and the CLI/test adapters that explicitly handle the new typed session-create channel.
- Typed HTTP contract: project/session/workspace HttpApi group and handler files.
- Tests: `packages/opencode/test/project/deletion-coordinator.test.ts`, `packages/opencode/test/server/project-global-delete.test.ts`, and `packages/opencode/test/control-plane/workspace.test.ts`.

The two pre-existing custom-elements declaration files remain user-owned, unmodified by this task, and are excluded from the commit.

### Necessary deviations and remaining boundaries

- The outbox columns are a necessary schema addition beyond the original Task 3 table shape. Without a stable event id and durable delivered marker, recovery could not distinguish publication completed before a phase-transition crash.
- Publication is currently a synchronous in-process `GlobalBus` delivery. The stable id plus delivered marker guarantees the tested publish-before-phase-transition recovery boundary. If publication later becomes an asynchronous external broker operation, the broker/consumer must use the stable id for idempotency or the outbox must gain an acknowledged relay; an external side effect cannot be made exactly-once by SQLite alone.
- Workspace adapter creation runs after the workspace row's gated commit because the builtin adapter re-enters project mutation (`addSandbox`). Holding the non-reentrant project gate across that adapter call deadlocks. The row is therefore always visible to a deletion snapshot before the external adapter side effect starts, and adapter failure retains the row as the existing contract requires.
- Task 4 still owns historical share-credential revocation semantics. Task 5 still owns the complete path inventory and final cleanup implementation. This fix only strengthens the current action boundary so a required cleanup failure cannot produce terminal success.
