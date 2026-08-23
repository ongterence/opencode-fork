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

---

## Fix round 2 — retry metadata, durable EventV2 publication, and lifecycle admission

### TDD red evidence

The production cleanup regression was added first and exercised the real git failure path. From `packages/opencode`:

```text
$ bun test --timeout 20000 test/server/project-global-delete.test.ts -t "retries mandatory worktree cleanup"
Expected: true
Received: false
0 pass
1 fail
2 expect() calls
Ran 1 test across 1 file. [5.37s]
```

The test creates and locks a real git worktree, invokes the production global DELETE route, and expects the project row to remain as retry metadata after mandatory cleanup fails. The red result proved the row had already been removed. During the final lifecycle verification, a new competing-owner assertion also failed (`Expected ownerCount 1, Received 0`), exposing that a non-owning `begin()` exit could release another fiber's owner; the final implementation now releases only ownership installed by that invocation.

### Implemented corrections

- Moved `ProjectTable` deletion after every mandatory git worktree and OpenCode artifact removal. A failed external cleanup therefore retains immutable project metadata, retries the same production target set, and cannot turn a missing project row into false terminal success. Journal worktree rows are retained until `finish` and removed only after cleanup and publication complete.
- Made `project.deleted` a durable EventV2 definition and registered it in the durable manifest. Production publication uses the journal's stable event id, checks the durable `EventTable` idempotency record, and routes the committed event through `EventV2Bridge` with explicit global/project metadata. The coordinator's failure hook now sits at the actual boundary after durable publication and before `event_delivered_at` is written.
- Added a production crash-boundary test: after a real durable deletion event is committed and bridged, it recreates the `cleanup_complete` journal state with the same event id. Recovery observes the durable event and emits no second `project.deleted` notification.
- Added `runOwned` with `Effect.acquireUseRelease`, so the release finalizer is installed atomically with deletion admission. A competing `begin`/`runOwned` no longer releases the active owner. `prepareShutdown` closes owner admission before snapshotting; `begin` rechecks closure under the keyed gate, `recover` rechecks it before every owner installation, and bounded drain still returns `DeletionBusyError` for an owner that cannot reach a durable boundary.
- Held a unique deletion lease across workspace row insertion and the external adapter `create` promise. Adapter creation is uninterruptible inside the lease so deletion quiescence cannot proceed while the underlying JavaScript promise is still mutating external state.
- Added session deletion admission to commands before command lookup and wrapped shell execution in the existing session/project lease with the existing session cancellation path. Command and shell service errors preserve `ProjectDeletingError`; their HttpApi declarations and handlers map it to the stable typed `409` response.

### Final green verification

Coordinator, production global deletion, and the complete workspace suite, from `packages/opencode`:

```text
$ bun test --timeout 30000 test/project/deletion-coordinator.test.ts test/server/project-global-delete.test.ts test/control-plane/workspace.test.ts
51 pass
1 skip
0 fail
204 expect() calls
Ran 52 tests across 3 files. [35.90s]
```

The global mutation barrier now checks project, session-create, session-update, prompt, workspace, share, command, and shell routes and observes eight typed `409` responses. The locked-worktree recovery test proves the project row survives the first failure and that recovery removes the worktree root, project storage artifact, project row, deletion job, and deletion-worktree journal rows before one terminal event.

Package-local typechecks:

```text
# packages/opencode
$ bun run typecheck
$ tsgo --noEmit

# packages/schema
$ bun run typecheck
$ tsgo --noEmit
```

Relevant prompt/session/share regression batch, from `packages/opencode`:

```text
$ bun test --timeout 30000 test/session/prompt.test.ts test/session/session.test.ts test/share/share-next.test.ts test/server/httpapi-session.test.ts
79 pass
14 skip
1 fail
348 expect() calls
Ran 94 tests across 4 files. [77.84s]
```

The one failure is `session HttpApi > validates archived timestamp values`, which expects HTTP 200 for `archived: -1` but receives 500. It is confirmed baseline-existing: the exact focused command at parent commit `ea7809cfe6` in a detached temporary worktree produced the identical `Expected: 200 / Received: 500` failure (`0 pass, 1 fail`, 9.64s). It also reproduced with this round's durable Project event schema change temporarily removed. All other relevant session, prompt, share, deletion, and workspace cases passed.

### Fix-round changed files

- Durable event definition and routing: `packages/schema/src/project.ts`, `packages/schema/src/durable-event-manifest.ts`, `packages/opencode/src/event-v2-bridge.ts`.
- Coordinator and production cleanup: `packages/opencode/src/project/deletion-coordinator.ts`, `packages/opencode/src/project/removal.ts`.
- Adjacent admission paths: `packages/opencode/src/control-plane/workspace.ts`, `packages/opencode/src/session/prompt.ts`.
- Typed HTTP contract: `packages/opencode/src/server/routes/instance/httpapi/groups/session.ts`, `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts`.
- Regression coverage: `packages/opencode/test/project/deletion-coordinator.test.ts`, `packages/opencode/test/server/project-global-delete.test.ts`, `packages/opencode/test/control-plane/workspace.test.ts`.

The two pre-existing custom-elements declaration files remain user-owned, unmodified, and excluded from this commit.

### Necessary deviation and residual boundary

- Making `Project.Event.Deleted` durable and adding it to the schema durable manifest is the only new support change outside the coordinator/admission files. It is necessary to use the repository's existing transactionally committed EventV2 outbox instead of treating volatile `GlobalBus` delivery as the durability boundary; no new database table or migration was added in this round.
- EventV2 commit plus stable event-id suppression guarantees one durable deletion record and prevents recovery from re-bridging an already committed event. Raw in-process `GlobalBus` remains a volatile compatibility projection: a process crash after the SQLite commit but before an in-memory listener runs cannot provide exactly-once delivery to that dead process. Durable EventV2 replay is the authoritative no-loss recovery mechanism.
- Task 4 continues to own historical share credential semantics. Task 5 continues to own the complete cleanup/path inventory rewrite; this round specifically prevents the current production cleanup from returning terminal success after a failed mandatory target.

---

## Fix round 3 — immutable artifact targets and committed-event replay

### TDD red evidence

The production retry test was expanded before the cleanup implementation. It creates a real session plus message/part rows, writes legacy session-diff/message/part artifacts, locks a real git worktree, and deletes through the production endpoint. From `packages/opencode`:

```text
$ bun test --timeout 30000 test/server/project-global-delete.test.ts -t "retries mandatory worktree cleanup"
Expected: false
Received: true
0 pass
1 fail
8 expect() calls
Ran 1 test across 1 file. [6.37s]
```

The failure was specifically the session-diff artifact remaining after unlock/recovery had already removed the project and journal. Message and part targets would have followed the same empty-live-row retry path.

The committed-but-unbridged production recovery test was also red first:

```text
$ bun test --timeout 30000 test/server/project-global-delete.test.ts -t "replays a committed deletion event"
Expected length: 1
Received length: 0
0 pass
1 fail
Ran 1 test across 1 file. [5.04s]
```

The core interlock test installed a crash after the EventTable transaction and before listener notification. Before the hook existed, publication succeeded and the test observed `Expected failure: true / Received: false` (`0 pass, 1 fail`). The migration test likewise failed first because `project_deletion_artifact` was absent from the durable journal schema.

### Implemented corrections

- Added `project_deletion_artifact(project_id, kind, artifact_id)` with a three-column primary key. The generated migration, migration registry, fresh schema, and schema snapshot are included.
- The coordinator's immediate `begin` transaction now snapshots every project session id, legacy message id, and legacy part id before any workspace/session deletion. The rows are tagged `session_diff`, `message`, or `part`; they remain immutable across retries and are deleted only in terminal `finish` with the share/worktree/job journals.
- Production cleanup reads only the durable artifact snapshot for live-derived legacy targets. Each identifier is passed through `legacyDeletionTarget`, so unsafe/corrupt legacy metadata retains the job in `cleaning` for retry/manual repair instead of constructing an unsafe path or reporting terminal success.
- Production git cleanup now reads `ProjectDeletionWorktreeTable` and uses its recorded canonical paths/branches when reconciling the authoritative git worktree list. The journal remains present until terminal `finish`.
- Added an EventV2 `afterDurableCommit` interlock used to prove the real transaction boundary: EventTable contains the event while listeners have received nothing.
- `EventV2Bridge` now has an explicit durable `deliverProjectDeleted` replay path. It reconstructs the deletion and sync projections from EventTable, validates the versioned type/project payload, and deduplicates durable project-deletion IDs within the active process. Recovery therefore emits one terminal project-deleted event and one sync event when the prior process committed but never bridged, while recovery after an already bridged publish emits no duplicate.

### Final green verification

Core EventV2 and durable journal suites, from `packages/core`:

```text
$ bun test test/event.test.ts test/database/project-deletion-job.test.ts
47 pass
0 fail
87 expect() calls
Ran 47 tests across 2 files. [1.70s]
```

Coordinator and production global deletion, from `packages/opencode`:

```text
$ bun test --timeout 30000 test/project/deletion-coordinator.test.ts test/server/project-global-delete.test.ts
17 pass
0 fail
91 expect() calls
Ran 17 tests across 2 files. [12.91s]
```

The expanded production retry asserts the three exact durable artifact keys after the locked-worktree failure, then proves recovery removes the worktree root, project artifact, session-diff artifact, message directory, part directory, project row, and all deletion journals before one terminal notification. The committed-unbridged recovery test observes exactly one `project.deleted` and one `sync` projection with the stable durable event id.

Prior lease/adapter/session/share behavior, from `packages/opencode`:

```text
$ bun test --timeout 30000 test/control-plane/workspace.test.ts test/session/session.test.ts test/share/share-next.test.ts test/event-manifest.test.ts test/project/removal-paths.test.ts
58 pass
1 skip
0 fail
301 expect() calls
Ran 59 tests across 5 files. [33.99s]
```

Fresh package-local verification:

```text
# packages/core
$ bun run typecheck
$ tsgo --noEmit
$ bun run migration --check
No schema changes, nothing to migrate

# packages/opencode
$ bun run typecheck
$ tsgo --noEmit

# packages/schema
$ bun run typecheck
$ tsgo --noEmit
```

### Fix-round changed files and necessary deviations

- Artifact schema/migration: `packages/core/src/project/deletion.sql.ts`, `packages/core/src/database/migration/20260823124644_20260823_project_deletion_artifact.ts`, `packages/core/src/database/migration.gen.ts`, `packages/core/src/database/schema.gen.ts`, `packages/core/schema.json`, and `packages/core/test/database/project-deletion-job.test.ts`.
- Event crash interlock: `packages/core/src/event.ts`, `packages/core/test/event.test.ts`.
- Coordinator/cleanup/replay: `packages/opencode/src/project/deletion-coordinator.ts`, `packages/opencode/src/project/removal.ts`, `packages/opencode/src/event-v2-bridge.ts`.
- Production regressions: `packages/opencode/test/server/project-global-delete.test.ts`.

The artifact table and EventV2 test interlock are narrowly necessary support beyond the original Task 3 file list. No Task 4 share-credential semantics or Task 5 general cleanup-layout rewrite was added. The two pre-existing custom-elements declaration files remain user-owned, unmodified, and excluded from the commit.

### Residual delivery boundary

EventTable is the authoritative no-loss record. After process death, the new process replays a committed-but-unbridged deletion exactly once into its fresh GlobalBus lifetime. Within one process, the stable event-id set suppresses recovery duplicates. As with any synchronous in-process EventEmitter, a machine/process crash after an individual subscriber callback but before a separate durable acknowledgement cannot prove globally exactly-once delivery to that dead process; those consumers no longer exist, and durable replay intentionally informs consumers attached to the restarted process.
