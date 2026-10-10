package api

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Replication: periodic bkt-to-bkt mirroring. A source bucket with
// ReplicateTo set has its CURRENT objects mirrored into the target bucket on
// every sweep: missing/changed objects (by ETag) are copied, and objects
// absent from the source are removed from the target. The target is fully
// managed by replication — async, idempotent, and self-healing after
// downtime. Versions/markers are not replicated (the target keeps its own
// history if IT has versioning enabled).
//
// Safety: on a versioned target, overwrites archive the target's current
// version first (prepareVersionedWrite) and removals create delete markers,
// so replication never destroys target history — in particular a WORM
// (retention) target keeps every retained version. A target with retention
// but without versioning (not normally reachable) is never written to. The
// target's quota is enforced for copies (reserveBucketQuota). Every per-key
// copy/delete runs under the same per-key write locks as client writes. Self-replication and cycles are
// refused both when configured and here.
//
// Authorization: replication acts on behalf of the user who configured it
// (ReplicationConfiguredBy) with that user's CURRENT permissions, evaluated
// per key: a copy needs s3:GetObject on the source key and s3:PutObject on
// the target key; a mirror-delete needs s3:DeleteObject on the target key.
// Denied keys are skipped (and counted). The configure-time check
// (authorizeReplication) is only a first gate. If the configurer is deleted
// or locked, replication for that bucket is disabled. Legacy configurations
// without a recorded configurer run as the bucket owner only while the owner
// is an admin (and are skipped otherwise until re-saved).
//
// Target identity: the target's ID is recorded at configure time
// (ReplicateToID). A target that no longer exists, or that was deleted and
// re-created under the same name, disables the configuration instead of
// pouring the source's data into someone else's new bucket.
//
// Sweeps never block on busy keys: locks are try-acquired with a short
// timeout and busy keys are left for the next sweep.

// sweepLockTimeout bounds how long a background sweep waits for one key's
// write lock (a large upload may hold it for a long time). A var for tests.
var sweepLockTimeout = 2 * time.Second

const (
	replicationBatchLimit = 500
	// sweepMaxBusyKeys stops a bucket's pass after this many busy keys so a
	// sweep cannot spend minutes waiting on locks; the rest is retried later.
	sweepMaxBusyKeys = 20
)

// legacyReplicationWarned de-duplicates the per-bucket warning for legacy
// configurations that cannot run (no recorded configurer, non-admin owner).
var legacyReplicationWarned sync.Map

// RunReplicationSweep mirrors every replicating bucket once.
func RunReplicationSweep(cfg *config.Config) {
	h := NewBucketHandler(cfg)
	var sources []models.Bucket
	if err := database.DB.Where("replicate_to != ''").Find(&sources).Error; err != nil {
		logger.Warn("Replication sweep: failed to list buckets", map[string]interface{}{"error": err.Error()})
		return
	}
	for i := range sources {
		src := &sources[i]
		if replicationCreatesCycle(src.Name, src.ReplicateTo, replicateToLookup) {
			logger.Warn("Replication: skipping self-replication or cycle", map[string]interface{}{"source": src.Name, "target": src.ReplicateTo})
			continue
		}
		var dst models.Bucket
		if err := database.DB.Where("name = ?", src.ReplicateTo).First(&dst).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				disableReplication(src, "target bucket no longer exists")
			} else {
				logger.Warn("Replication: failed to load target", map[string]interface{}{"source": src.Name, "target": src.ReplicateTo, "error": err.Error()})
			}
			continue
		}
		if reason := replicationTargetMismatch(src, &dst); reason != "" {
			disableReplication(src, reason)
			continue
		}
		if dst.RetentionDays > 0 && dst.Versioning != models.VersioningEnabled {
			logger.Warn("Replication: target has retention without versioning — refusing to write", map[string]interface{}{"source": src.Name, "target": dst.Name})
			continue
		}
		srcEval, dstEval, ok := replicationEvaluators(h.policyService, src, &dst)
		if !ok {
			continue
		}
		if src.ReplicateToID == nil {
			// Legacy config that passed the dangling check: pin the target now.
			database.DB.Model(&models.Bucket{}).Where("id = ? AND replicate_to = ?", src.ID, dst.Name).
				Update("replicate_to_id", dst.ID)
		}
		replicateBucket(h, src, &dst, srcEval, dstEval)
	}
}

// replicationTargetMismatch reports (non-empty reason) when src's
// replication target is not the bucket it was configured with: the recorded
// target ID differs (deleted and re-created under the same name), or — for
// legacy configs without a recorded ID — the target was created after the
// configuration was last saved, so it cannot be the configured bucket.
func replicationTargetMismatch(src, dst *models.Bucket) string {
	if src.ReplicateToID != nil {
		if *src.ReplicateToID != dst.ID {
			return "target bucket was deleted and re-created since replication was configured"
		}
		return ""
	}
	configuredAt := src.UpdatedAt
	if src.ReplicationConfiguredAt != nil {
		configuredAt = *src.ReplicationConfiguredAt
	}
	if dst.CreatedAt.After(configuredAt) {
		return "target bucket was created after replication was configured"
	}
	return ""
}

// replicationPrincipal returns the user whose permissions replication runs
// with: the recorded configurer, or — for legacy configs — the bucket owner
// if the owner is currently an unlocked admin. ok=false means skip.
func replicationPrincipal(src *models.Bucket) (uuid.UUID, bool) {
	if src.ReplicationConfiguredBy != nil {
		return *src.ReplicationConfiguredBy, true
	}
	var owner models.User
	if err := database.DB.Select("id", "is_admin", "is_locked").First(&owner, "id = ?", src.OwnerID).Error; err == nil &&
		owner.IsAdmin && !owner.IsLocked {
		return owner.ID, true
	}
	if _, warned := legacyReplicationWarned.LoadOrStore(src.ID, true); !warned {
		logger.Warn("Replication: legacy configuration has no recorded configurer and the bucket owner is not an admin — skipping; re-save the replication setting to resume", map[string]interface{}{"source": src.Name, "target": src.ReplicateTo})
	}
	return uuid.Nil, false
}

// replicationEvaluators loads the principal's permissions on source and
// target once for this sweep. A deleted or locked configurer disables the
// configuration. ok=false means skip this source for this sweep.
func replicationEvaluators(ps *services.PolicyService, src, dst *models.Bucket) (srcEval, dstEval *services.AccessEvaluator, ok bool) {
	principal, ok := replicationPrincipal(src)
	if !ok {
		return nil, nil, false
	}
	srcEval, err := ps.NewAccessEvaluator(principal, src.Name)
	if err != nil {
		logger.Warn("Replication: failed to load permissions", map[string]interface{}{"source": src.Name, "error": err.Error()})
		return nil, nil, false
	}
	if srcEval.UserMissing() || srcEval.UserLocked() {
		if src.ReplicationConfiguredBy != nil {
			disableReplication(src, "the user who configured replication was deleted or locked")
		}
		return nil, nil, false
	}
	dstEval, err = ps.NewAccessEvaluator(principal, dst.Name)
	if err != nil {
		logger.Warn("Replication: failed to load permissions", map[string]interface{}{"target": dst.Name, "error": err.Error()})
		return nil, nil, false
	}
	return srcEval, dstEval, true
}

// disableReplication clears src's replication configuration (only if it
// still points at the same target, so a concurrent reconfiguration wins).
func disableReplication(src *models.Bucket, reason string) {
	res := database.DB.Model(&models.Bucket{}).
		Where("id = ? AND replicate_to = ?", src.ID, src.ReplicateTo).
		Updates(map[string]interface{}{
			"replicate_to":              "",
			"replicate_to_id":           nil,
			"replication_configured_by": nil,
			"replication_configured_at": nil,
		})
	if res.Error != nil {
		logger.Warn("Replication: failed to disable configuration", map[string]interface{}{"source": src.Name, "error": res.Error.Error()})
		return
	}
	if res.RowsAffected > 0 {
		logger.Warn("Replication disabled", map[string]interface{}{"source": src.Name, "target": src.ReplicateTo, "reason": reason})
	}
}

func replicateBucket(h *BucketHandler, src, dst *models.Bucket, srcEval, dstEval *services.AccessEvaluator) {
	srcBackend, err := h.getStorageBackend(src)
	if err != nil {
		logger.Warn("Replication: source storage init failed", map[string]interface{}{"bucket": src.Name, "error": err.Error()})
		return
	}
	dstBackend, err := h.getStorageBackend(dst)
	if err != nil {
		logger.Warn("Replication: target storage init failed", map[string]interface{}{"bucket": dst.Name, "error": err.Error()})
		return
	}

	// Diff via the DB — both sides are bkt buckets, so this is two queries.
	srcObjs := []models.Object{}
	if err := database.DB.Where("bucket_id = ?", src.ID).Find(&srcObjs).Error; err != nil {
		logger.Warn("Replication: failed to list source", map[string]interface{}{"bucket": src.Name, "error": err.Error()})
		return
	}
	dstObjs := []models.Object{}
	if err := database.DB.Where("bucket_id = ?", dst.ID).Find(&dstObjs).Error; err != nil {
		logger.Warn("Replication: failed to list target", map[string]interface{}{"bucket": dst.Name, "error": err.Error()})
		return
	}
	dstByKey := make(map[string]*models.Object, len(dstObjs))
	for i := range dstObjs {
		dstByKey[dstObjs[i].Key] = &dstObjs[i]
	}
	srcKeys := make(map[string]bool, len(srcObjs))

	copied, removed, denied, busy := 0, 0, 0, 0
	for i := range srcObjs {
		s := &srcObjs[i]
		srcKeys[s.Key] = true
		if d, ok := dstByKey[s.Key]; ok && d.ETag == s.ETag && d.Size == s.Size {
			continue // already in sync
		}
		if copied >= replicationBatchLimit || busy >= sweepMaxBusyKeys {
			continue // cap per sweep; the next sweep continues
		}
		if !replicationCopyAllowed(srcEval, dstEval, s.Key) {
			denied++
			continue
		}
		done, wasBusy := replicateObject(srcBackend, dstBackend, src, dst, s.Key)
		if done {
			copied++
		} else if wasBusy {
			busy++
		}
	}

	for key := range dstByKey {
		if srcKeys[key] {
			continue
		}
		if removed >= replicationBatchLimit || busy >= sweepMaxBusyKeys {
			break
		}
		if !dstEval.Allowed(services.ActionDeleteObject, key) {
			denied++
			continue
		}
		done, wasBusy := mirrorDelete(dstBackend, src, dst, key)
		if done {
			removed++
		} else if wasBusy {
			busy++
		}
	}

	if copied > 0 || removed > 0 {
		logger.Info("Replication: synced", map[string]interface{}{
			"source": src.Name, "target": dst.Name, "copied": copied, "removed": removed,
		})
	}
	if denied > 0 {
		logger.Warn("Replication: keys skipped — the configuring user lacks permission", map[string]interface{}{
			"source": src.Name, "target": dst.Name, "denied": denied,
		})
	}
	if busy > 0 {
		logger.Info("Replication: busy keys deferred to the next sweep", map[string]interface{}{
			"source": src.Name, "target": dst.Name, "busy": busy,
		})
	}
}

// replicationCopyAllowed reports whether the replication principal may copy
// key: read it from the source and write it into the target.
func replicationCopyAllowed(srcEval, dstEval *services.AccessEvaluator, key string) bool {
	return srcEval.Allowed(services.ActionGetObject, key) && dstEval.Allowed(services.ActionPutObject, key)
}

// lockKeysAcrossBuckets locks one key in each of two buckets. Locks are taken
// in the same global order lockObjectKeys uses within a bucket (by
// bucket + "\x00" + key), so this cannot deadlock against single-bucket
// multi-key lockers or another cross-bucket locker.
func lockKeysAcrossBuckets(bucketA, keyA, bucketB, keyB string) func() {
	idA, idB := bucketA+"\x00"+keyA, bucketB+"\x00"+keyB
	if idA == idB {
		return lockObjectKeys(bucketA, keyA)
	}
	if idB < idA {
		bucketA, keyA, bucketB, keyB = bucketB, keyB, bucketA, keyA
	}
	first := lockObjectKeys(bucketA, keyA)
	second := lockObjectKeys(bucketB, keyB)
	return func() {
		second()
		first()
	}
}

func acrossBucketLockIDs(bucketA, keyA, bucketB, keyB string) []string {
	a, b := bucketA+"\x00"+keyA, bucketB+"\x00"+keyB
	if a == b {
		return []string{a}
	}
	ids := []string{a, b}
	sort.Strings(ids)
	return ids
}

func lockKeysAcrossBucketsChecked(ctx context.Context, bucketA *models.Bucket, keyA string, bucketB *models.Bucket, keyB string) (func(), error) {
	unlock, err := lockObjectIDsChecked(ctx, acrossBucketLockIDs(bucketA.Name, keyA, bucketB.Name, keyB))
	if err != nil {
		return nil, err
	}
	if err := validateLockedBuckets(ctx, bucketA, bucketB); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

func tryLockKeysAcrossBucketsChecked(bucketA *models.Bucket, keyA string, bucketB *models.Bucket, keyB string, timeout time.Duration) (func(), bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	unlock, err := lockKeysAcrossBucketsChecked(ctx, bucketA, keyA, bucketB, keyB)
	if err != nil {
		return func() {}, false
	}
	return unlock, true
}

// tryLockKeysAcrossBuckets is lockKeysAcrossBuckets with a deadline covering
// both acquisitions (same global order). On timeout it holds nothing and
// returns ok=false; on success the unlock func releases both.
func tryLockKeysAcrossBuckets(bucketA, keyA, bucketB, keyB string, timeout time.Duration) (unlock func(), ok bool) {
	idA, idB := bucketA+"\x00"+keyA, bucketB+"\x00"+keyB
	if idA == idB {
		return tryLockObjectKeys(bucketA, timeout, keyA)
	}
	if idB < idA {
		bucketA, keyA, bucketB, keyB = bucketB, keyB, bucketA, keyA
	}
	deadline := time.Now().Add(timeout)
	first, ok := tryLockObjectKeys(bucketA, timeout, keyA)
	if !ok {
		return func() {}, false
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		remaining = time.Millisecond
	}
	second, ok := tryLockObjectKeys(bucketB, remaining, keyB)
	if !ok {
		first()
		return func() {}, false
	}
	return func() {
		second()
		first()
	}, true
}

// replicateObject copies the source's current object for key into the target
// under both keys' write locks (so neither the source bytes nor the target's
// current version can change mid-copy). Both rows are re-read under the lock.
// Reports whether a copy was committed, and whether the keys were busy (lock
// not acquired within sweepLockTimeout; retried next sweep).
func replicateObject(srcBackend, dstBackend storage.StorageBackend, src, dst *models.Bucket, key string) (copied, busy bool) {
	unlock, ok := tryLockKeysAcrossBucketsChecked(src, key, dst, key, sweepLockTimeout)
	if !ok {
		return false, true
	}
	defer unlock()
	return replicateObjectLocked(srcBackend, dstBackend, src, dst, key), false
}

func replicateObjectLocked(srcBackend, dstBackend storage.StorageBackend, src, dst *models.Bucket, key string) bool {

	var s models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", src.ID, key).First(&s).Error; err != nil {
		return false // removed from the source since the listing; the next sweep mirrors it
	}
	var d models.Object
	hadPrior := database.DB.Where("bucket_id = ? AND key = ?", dst.ID, key).First(&d).Error == nil
	if hadPrior && d.ETag == s.ETag && d.Size == s.Size {
		return false // already in sync
	}

	// Reserve the target's quota for the copy (released once the row is
	// committed or the copy failed).
	reservation, qerr := reserveBucketQuota(dst, s.Size)
	if qerr != nil {
		logger.Warn("Replication: target quota exceeded", map[string]interface{}{"bucket": dst.Name, "key": key, "error": qerr.Error()})
		return false
	}
	defer reservation.release()

	rc, err := srcBackend.GetObject(src.Name, key)
	if err != nil {
		logger.Warn("Replication: read failed", map[string]interface{}{"bucket": src.Name, "key": key, "error": err.Error()})
		return false
	}
	defer rc.Close() //nolint:errcheck // best-effort close of read stream

	// On a versioned target, archive the current version before the
	// overwrite so target history (and any retained data) survives.
	archivedVID, verr := prepareVersionedWrite(dstBackend, dst, key)
	if verr != nil {
		logger.Warn("Replication: failed to version target object", map[string]interface{}{"bucket": dst.Name, "key": key, "error": verr.Error()})
		return false
	}
	if werr := dstBackend.PutObject(dst.Name, key, rc, s.Size, s.ContentType, jsonPtrToMap(s.Metadata)); werr != nil {
		rollbackVersionedWrite(dstBackend, dst, key, archivedVID)
		logger.Warn("Replication: write failed", map[string]interface{}{"bucket": dst.Name, "key": key, "error": werr.Error()})
		return false
	}
	obj := models.Object{
		BucketID: dst.ID, Key: key, Size: s.Size, ContentType: s.ContentType,
		ContentDisposition: s.ContentDisposition, CacheControl: s.CacheControl,
		ETag: s.ETag, SHA256: s.SHA256, StoragePath: key, Metadata: s.Metadata, Tags: s.Tags,
		VersionID: newCurrentVersionID(dst),
	}
	if err := upsertCurrentObject(&obj); err != nil {
		discardFailedWrite(dstBackend, dst, key, archivedVID, hadPrior)
		logger.Warn("Replication: failed to record target object", map[string]interface{}{"bucket": dst.Name, "key": key, "error": err.Error()})
		return false
	}
	return true
}

// mirrorDelete removes key from the target (it no longer exists in the
// source) under the target key's write lock. On a versioned target this goes
// through the versioned path (bytes archived + delete marker), so target
// history — including retained data — is kept. Reports whether it removed,
// and whether the key was busy (retried next sweep).
func mirrorDelete(dstBackend storage.StorageBackend, src, dst *models.Bucket, key string) (removed, busy bool) {
	unlock, ok := tryLockObjectKeysChecked(dst, sweepLockTimeout, key)
	if !ok {
		return false, true
	}
	defer unlock()
	return mirrorDeleteLocked(dstBackend, src, dst, key), false
}

func mirrorDeleteLocked(dstBackend storage.StorageBackend, src, dst *models.Bucket, key string) bool {

	if currentObjectExists(src.ID, key) {
		return false // (re)created in the source since the listing
	}
	var d models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", dst.ID, key).First(&d).Error; err != nil {
		return false // already gone
	}
	if _, handled, derr := versionedDeleteCurrent(dstBackend, dst, &d); handled {
		if derr != nil {
			logger.Warn("Replication: versioned mirror-delete failed", map[string]interface{}{"bucket": dst.Name, "key": key, "error": derr.Error()})
			return false
		}
		return true
	}
	// Unversioned target: a permanent delete. Never remove data that
	// retention still protects (defensive — retention requires versioning,
	// so this is not normally reachable).
	if retentionBlocks(dst, d.UpdatedAt) {
		return false
	}
	if err := dstBackend.DeleteObject(dst.Name, key); err != nil {
		logger.Warn("Replication: mirror-delete failed", map[string]interface{}{"bucket": dst.Name, "key": key, "error": err.Error()})
		return false
	}
	database.DB.Delete(&models.Object{}, "id = ?", d.ID)
	return true
}
