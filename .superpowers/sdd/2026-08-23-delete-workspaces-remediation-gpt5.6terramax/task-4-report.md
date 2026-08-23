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
