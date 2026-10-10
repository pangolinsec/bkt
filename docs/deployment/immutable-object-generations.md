# Immutable object generations: crash-consistency work inventory

Status: **release blocker**. The tests in `object_replica_integration_pg_test.go`
and `object_generation_integration_pg_test.go` expose committed metadata pointing
to different bytes after a writer dies between a filesystem operation and a
database update. The cross-replica advisory-lock work prevents concurrent
mutations; it does not make one mutation atomic across PostgreSQL and storage.

## Current commit boundary

`objects` holds the current object metadata and a `storage_path` field, but
every current read uses the public `key` to open bytes. Writers also set
`storage_path = key`. Local storage replaces the blob at that key with a
same-directory rename. Its rename is atomic as a filesystem operation; the
subsequent metadata upsert is a separate PostgreSQL transaction. A crash in
the gap leaves new bytes under the old ETag. Delete and version archive have
the inverse failure: they remove or move the bytes while the old row still
claims to be current. PostgreSQL locks serialize replicas but cannot close
this cross-system transaction gap.

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
| Bucket deletion | `bucket_handler.go` `DeleteBucket` | Hold the exclusive bucket lock, commit deletion, then reclaim unreferenced generations; a crash cannot leave a row pointing to removed bytes. |
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

## Red diagnostic gates as of this branch

- `TestIntegrationCrashBetweenBytesAndMetadata`: in-place PUT serves
  `new-bytes` under the committed old ETag.
- `TestIntegrationGenerationCrashCopy`: copy replacement serves source bytes
  under the committed destination ETag.
- `TestIntegrationGenerationCrashDelete`: byte-first delete leaves a live row
  whose GET returns 500.
- `TestIntegrationGenerationCrashRange`: range GET serves a partial new blob
  under the old ETag.
- `TestIntegrationGenerationCrashArchive`: version archive moves the current
  blob and inserts a history row before the replacement commits.
- `TestIntegrationGenerationCrashRestore`: restore writes historical bytes
  under the still-current metadata and adds an uncommitted history row.

The diagnostic subprocesses force exits at storage/metadata boundaries. As
implementation paths change, move the hooks to the actual production commit
boundaries rather than making a test green by bypassing a new code path. The
gate passes only when the full PostgreSQL-backed suite, two-process races,
all crash boundaries, legacy migration, and S3/local backend conformance are
green on the exact release candidate.
