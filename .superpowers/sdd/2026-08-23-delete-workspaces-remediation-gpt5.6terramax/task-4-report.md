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
