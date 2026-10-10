# Immutable object generations: crash-consistency work inventory

Status: **release blocker**. The original crash diagnostics in
`object_replica_integration_pg_test.go` and
`object_generation_integration_pg_test.go` now pass for the production S3
PUT/copy/delete/restore and local multipart paths they exercise. This is a
scoped checkpoint, not a provider parity result: the remaining console,
lifecycle, replication, reconciliation, legacy migration, and reader-lifetime
paths below still need implementation and crash tests. The cross-replica
advisory locks prevent concurrent mutations but cannot make PostgreSQL and
storage one atomic system.

## Current commit boundary

Legacy `objects` rows name a mutable public-key blob. New S3 API PUT, copy,
multipart completion, and restore paths stage create-only bytes under a
`generation_id`, then commit the current pointer and any archived version row
in one PostgreSQL transaction. Current and historical S3 reads resolve that
pointer. The original gap remains wherever a writer still replaces, moves,
or deletes public-key bytes before its metadata commit. A crash there can
serve new bytes under an old ETag or leave a live row without bytes.

## Complete path inventory

| Operation | Current code | Required generation behavior |
| --- | --- | --- |
| S3 PUT | `s3_api_handler.go` `PutObject` | Write a new immutable generation; commit its pointer and any archived-version row in one DB transaction. |
| S3 CopyObject | `s3_api_handler.go` `CopyObject` | Read the source generation named by its row; stage a distinct destination generation, including self-copy and cross-bucket copy. |
| S3 multipart complete | `s3_multipart_handler.go` `CompleteMultipartUpload` | Assemble into a staging generation; publishing the current pointer is the commit. Aborts must not remove committed bytes. |
| S3 UploadPartCopy | `s3_multipart_handler.go` `UploadPartCopy` | Read the source generation for the requested range, not a mutable public key. |
| S3 delete and batch delete | `s3_api_handler.go` `DeleteObject`, `deleteOneForBatch`, `deleteVersionForBatch` | Commit removal/marker in DB before reclaiming any generation; respect retention and version identity. |
| S3 current GET and range | `s3_api_handler.go` `GetObject` | Resolve the physical generation from the same row supplying Size and ETag; keep that generation readable through the response. |
| S3 HEAD and list | `s3_api_handler.go` `HeadObject`, `ListObjects` | HEAD is metadata only; listing must show committed rows and never leak or import hidden generations. |
| S3 version GET/delete | `s3_versioning_handler.go`, `versioning.go` | Resolve historical bytes through a version-row generation pointer; delete a version row before reclaiming its bytes. |
| Console synchronous/async upload | `bucket_handler.go` `storeObject`, `bucket_handler_async.go` | Use the same staging and DB commit path as S3 PUT. |
| Console download and range | `bucket_handler.go` `DownloadObject` | Resolve the row's generation before reading full or partial bytes. |
| Console delete, copy, move, folder delete/move | `bucket_handler.go`, `folder_handler.go`, `storage_objects.go` | Treat every destination as a new generation and commit row changes before physical cleanup; source reads use pinned generations. |
| Console restore/delete version | `versioning_rest.go`, `versioning.go` | Restore by publishing a new current pointer in one DB transaction with history changes; never move the only copy of a committed version before commit. |
| Lifecycle and replication | `lifecycle.go`, `replication.go` | Use the same generation commit/delete primitives and source-pinned reads. |
| Bucket deletion | `bucket_handler.go` `DeleteBucket` | Hold the exclusive bucket lock. Local deletion commits a durable cleanup record with the metadata removal, then removes bytes; startup retries cleanup only for the matching storage root identity, and creation refuses the same name until it finishes. S3 deletion first verifies its stored route identity and refuses ambiguous `NoSuchBucket` responses. |
| S3 reconciliation | `bucket_handler.go` `ListObjects`, `pruneStaleObjectRows` | Recognize committed generations hidden from upstream listings; never prune their rows as “missing” or import staging blobs as user objects. |
| Local/S3 storage and migration | `storage/local.go`, `storage/local_versioning.go`, `storage/s3.go`, `storage/s3_versioning.go`, `storage/local_migrate.go` | Provide create-only immutable generation storage and physical resolution without exposing reserved keys; support legacy `storage_path = key` rows. |

## Commit and recovery contract

1. Give each staged blob a unique, create-only generation ID. No writer may
   replace a generation that a committed `objects` or `object_versions` row
   references. The physical key must be hidden from user API and reconciliation.
2. Add a generation pointer to historical version rows. Preserve the client
   `VersionID` separately: an unversioned current object still needs a
   generation ID, and a later archive may receive a different public ID.
3. Commit the new current row, prior-version row or delete marker, and any
   removal of the previous current row in a single PostgreSQL transaction.
   A failed transaction leaves the old pointer intact; an orphan stage is
   safe to reclaim later. Do not move or delete old bytes before this commit.
4. Current GET, range GET, console download, copy, UploadPartCopy, restore,
   replication, and version GET must open the exact generation associated
   with the metadata they read. A read must keep that generation alive until
   its stream closes. Metadata alone is insufficient if a cleanup sweep can
   delete the blob before the stream opens.
5. Reclaim old and orphan generations only after proving no current/version
   row or live reader references them. A fixed delay without a read-duration
   bound is not a complete safety proof. Keep cleanup retryable across crashes.
6. Legacy rows may continue reading their public-key blobs. The first
   mutation must stage new bytes and atomically switch the row to a
   generation; it must not destroy the legacy blob before commit. Backfill
   and garbage collection need separate migration and rollback tests.

## Diagnostic checkpoint on this branch

- The original six subprocess diagnostics above are green. Each child enters
  a production route, exits at the storage/metadata boundary, and a fresh
  reader verifies the committed bytes and ETag.
- `TestIntegrationGenerationCrashMultipart` is green for local storage: a
  completed immutable blob remains invisible if the writer dies before the
  metadata transaction. The same tracked upload and its parts remain
  available for a retry after the subprocess exits.
- `TestIntegrationS3MultipartPublishesHiddenGeneration` checks a real API
  route against a fake S3 upstream: initiate, part upload, and complete target
  a hidden generation, completion is create-only, and the previous version
  remains readable. It also retries after upstream Complete succeeds and the
  API stops before the metadata transaction. Tracking status, current pointer,
  and version history commit together; a forced tracking failure rolls all
  three back. Local completion rejects a wrong part ETag and serializes part
  replacement with completion. `TestIntegrationS3PutStreamsIntoHiddenGeneration`
  checks the corresponding normal PUT path.
- S3-backed generation PUTs and multipart parts use a seekable scratch file
  for HTTP request signing. On Unix the file is unlinked immediately, and on
  Linux the declared byte count is
  reserved before reading the client body; insufficient scratch capacity
  rejects the write without changing a committed object. Set
  `BKT_S3_GENERATION_SPOOL_DIR` to an existing dedicated directory when `/tmp`
  cannot hold the maximum simultaneous uploads. The default per-request limit
  is 5 GiB, so capacity planning must account for concurrent writers. The
  unlinked file is reclaimed by the kernel when a process dies. Windows
  removes the named file after a normal upload; crash cleanup there remains
  a platform limitation.
- The exact PostgreSQL-backed `go test -count=1 ./...` run passed after these
  increments. This suite does not yet exercise every mutation in the path
  inventory or prove safe reclamation of generations held by live readers.

The diagnostic subprocesses force exits at storage/metadata boundaries. As
implementation paths change, move the hooks to the actual production commit
boundaries rather than making a test green by bypassing a new code path. The
gate passes only when the full PostgreSQL-backed suite, two-process races,
all crash boundaries, legacy migration, and S3/local backend conformance are
green on the exact release candidate.
