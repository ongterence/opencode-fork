# Task 4 report — authoritative historical share revocation

## Delivered

- Added `ShareNext.revokeHistorical`, which always performs the legacy remote DELETE from the persisted credentials, independently of `OPENCODE_DISABLE_SHARE`.
- Classified 2xx as `revoked`, 404 as `already_absent`, and exposed transport/non-2xx failures as `RemoteShareRevocationError` with HTTP status where available.
- Snapshotted the API origin into the deletion journal; the removal path also normalizes older public-share journal URLs to their origin for backwards-compatible retries.
- Made the deletion coordinator consume the durable share journal rather than live session state. Acknowledged shares atomically become `revoked`, have their retained journal secret scrubbed, and have their live share row deleted. Failures retain all credentials/local rows, record `failed` plus the error, and block cleanup/event publication.

## Tests

TDD red states observed:

- The missing `revokeHistorical` interface failed with `TypeError: svc.revokeHistorical is not a function`.
- The absent-share test failed before the 404 mapping with `RemoteShareRevocationError ... HTTP 404`.
- The global deletion integration test returned 204 before journal-based revocation was introduced.

Fresh package-local verification:

```text
bun test test/share/share-next.test.ts test/server/project-global-delete.test.ts && bun typecheck
19 pass, 0 fail
tsgo --noEmit exit 0
```

## Fix round 1 — explicit retry contract

- Normal `DELETE /global/project/:projectID` now returns `409` with
  `{ code: "project_deletion_retryable", phase: "share_failed", retry: true }`
  when retained historical-share credentials need an explicit retry.
- `POST /global/project/:projectID/delete/retry` is the only operation that
  reopens a `share_failed` journal. It claims the failed phase in an immediate
  transaction, transitions it to `revoking_shares`, and then runs the existing
  durable coordinator. Startup recovery still skips `share_failed`; it never
  silently loops remote revocation.
- The retry endpoint preserves the normal deletion endpoint's in-progress
  semantics. A 2xx or 404 remote DELETE is authoritative success; only then can
  local cleanup and `project.deleted` occur.
- `OPENCODE_DISABLE_SHARE` is evaluated at runtime for ordinary sharing paths.
  `revokeHistorical` remains deliberately independent of it, and its test sets
  the flag before service construction.
- Production global tests cover 500, connection failure, 401, and 403. Each
  proves retained project/session/share/journal credentials, a retryable typed
  response, no deletion event, and a persisted error that does not contain the
  secret. A focused retry test proves a later 404 completes deletion.

Focused evidence from this round:

```text
bun test test/share/share-next.test.ts --test-name-pattern "while new sharing is disabled"
1 pass, 0 fail

bun test test/server/project-global-delete.test.ts --test-name-pattern "reports a retryable"
1 pass, 0 fail

bun test test/server/project-global-delete.test.ts --test-name-pattern "401 and 403"
1 pass, 0 fail
```

`bun run generate` from `packages/client` completed successfully and produced
no generated-file changes. The aggregate focused test command was retried with
an isolated `OPENCODE_DB`; it remained active for over two minutes without
printing a summary amid the shared runner's active lint/typecheck load, so that
specific aggregate process was stopped. No unrelated process was terminated.

## Fix round 2 — handler typing, SDK, and route coverage

- Replaced the sequential `catchTag` chain in the global delete handlers with
  one explicitly typed `catchTags` map. This keeps the `share_failed` branch
  as the distinct `ProjectDeletionRetryableError` union member instead of
  narrowing it to the in-progress error while preserving exact Effect errors.
- Added the mandatory coverage fixture for
  `POST /global/project/:projectID/delete/retry`. A seeded project without a
  `share_failed` journal is a valid request and deterministically receives its
  typed 409 in-progress response.
- Regenerated the legacy JS SDK with `bun run build` in `packages/sdk/js`.
  The generated public operation is
  `Global.project.delete2.retry({ projectID })`, an explicit retry operation;
  its 409 error type is the union of `ProjectDeletionInProgressError` and
  `ProjectDeletionRetryableError`.

Fresh package-local verification:

```text
packages/opencode: bun typecheck
$ tsgo --noEmit

packages/sdk/js: bun typecheck
$ tsgo --noEmit

packages/opencode: bun run script/httpapi-exercise.ts --mode coverage --fail-on-missing --fail-on-skip
summary pass=210 fail=0 skip=0 missing=0 extra=0

packages/opencode: bun test test/share/share-next.test.ts test/server/project-global-delete.test.ts --test-name-pattern "while new sharing is disabled|401 and 403|reports a retryable"
3 pass, 0 fail, 19 filtered out
```

The unfiltered aggregate test command was also rerun. Two existing long
integration cases exceeded Bun's per-test five-second timeout while the shared
runner was active; the three Task 4 production cases above completed and
passed. No runner process was terminated.
