package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ── Per-key write serialization ──────────────────────────────────────────────
//
// Every mutation of an object's current state (write, overwrite, delete, move)
// is a multi-step sequence — archive the current version, write the bytes,
// update the metadata row — that must not interleave with another mutation of
// the same key. Production mutators use lockObjectKeysChecked (or its bounded
// try variant), which combines this process's key semaphore with a PostgreSQL
// session advisory lock shared by all replicas. The local-only helpers below
// exist for unit tests of key ordering and must not guard production writes.

// Each key's lock is a 1-slot channel semaphore (rather than a sync.Mutex) so
// acquisition can be abandoned: tryLockObjectKeys gives up after a timeout,
// which background sweeps (lifecycle, replication) use so a slow client
// upload holding a key cannot stall them. Entries are reference-counted and
// removed from the registry when no holder or waiter remains.
type keyLockEntry struct {
	sem  chan struct{}
	refs int
}

var objectKeyLocks = struct {
	sync.Mutex
	m map[string]*keyLockEntry
}{m: map[string]*keyLockEntry{}}

// objectLockIDs returns the de-duplicated, globally ordered lock ids for keys
// of one bucket.
func objectLockIDs(bucketName string, keys []string) []string {
	ids := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		id := bucketName + "\x00" + k
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// acquireKeyLockEntry registers interest in id (refcount) and returns its entry.
func acquireKeyLockEntry(id string) *keyLockEntry {
	objectKeyLocks.Lock()
	defer objectKeyLocks.Unlock()
	e := objectKeyLocks.m[id]
	if e == nil {
		e = &keyLockEntry{sem: make(chan struct{}, 1)}
		objectKeyLocks.m[id] = e
	}
	e.refs++
	return e
}

// releaseKeyLockEntry drops interest in id, removing the entry when unused.
func releaseKeyLockEntry(id string, e *keyLockEntry) {
	objectKeyLocks.Lock()
	e.refs--
	if e.refs == 0 {
		delete(objectKeyLocks.m, id)
	}
	objectKeyLocks.Unlock()
}

// unlockKeyEntries releases held locks in reverse order.
func unlockKeyEntries(ids []string, entries []*keyLockEntry) {
	for i := len(entries) - 1; i >= 0; i-- {
		<-entries[i].sem
		releaseKeyLockEntry(ids[i], entries[i])
	}
}

// lockObjectKeys is the local-only key-ordering primitive for unit tests.
// Production writers must use lockObjectKeysChecked so they serialize across
// replicas. Keys are de-duplicated and ordered; unlock is idempotent.
func lockObjectKeys(bucketName string, keys ...string) func() {
	ids := objectLockIDs(bucketName, keys)
	entries := make([]*keyLockEntry, len(ids))
	for i, id := range ids {
		e := acquireKeyLockEntry(id)
		e.sem <- struct{}{}
		entries[i] = e
	}
	var once sync.Once
	return func() { once.Do(func() { unlockKeyEntries(ids, entries) }) }
}

// advisoryKey is namespaced separately from the migration advisory lock.
// Hashing the complete bucket/key pair keeps arbitrary S3 keys out of SQL and
// gives every replica the same lock order and identity.
func advisoryKey(id string) int64 {
	sum := sha256.Sum256([]byte("bkt/object-write/v1\x00" + id))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// lockObjectIDsChecked combines the local lock with PostgreSQL session locks.
// A session lock remains held across the filesystem write and DB commit; a
// transaction-scoped lock would end before the streamed body was committed.
// Failure to acquire the shared lock is returned to callers, never treated
// as permission to mutate an unlocked object.
func lockObjectIDsChecked(ids []string) (func(), error) {
	entries := make([]*keyLockEntry, len(ids))
	for i, id := range ids {
		e := acquireKeyLockEntry(id)
		e.sem <- struct{}{}
		entries[i] = e
	}
	localUnlock := func() { unlockKeyEntries(ids, entries) }
	if database.AdvisoryDB == nil {
		localUnlock()
		return nil, fmt.Errorf("object write lock: advisory database not initialized")
	}
	conn, err := database.AdvisoryDB.Conn(context.Background())
	if err != nil {
		localUnlock()
		return nil, fmt.Errorf("object write lock: database connection: %w", err)
	}
	locked := 0
	for _, id := range ids {
		if _, err = conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", advisoryKey(id)); err != nil {
			break
		}
		locked++
	}
	if err != nil {
		unlockAdvisory(conn, ids[:locked])
		_ = conn.Close()
		localUnlock()
		return nil, fmt.Errorf("object write lock: acquire: %w", err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			unlockAdvisory(conn, ids)
			_ = conn.Close()
			localUnlock()
		})
	}, nil
}

func unlockAdvisory(conn *sql.Conn, ids []string) {
	failed := false
	for i := len(ids) - 1; i >= 0; i-- {
		var released bool
		if err := conn.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryKey(ids[i])).Scan(&released); err != nil || !released {
			failed = true
			logger.Warn("Failed to release object write advisory lock", map[string]interface{}{"error": fmt.Sprintf("err=%v released=%v", err, released)})
		}
	}
	if failed {
		// A session with an unreleased advisory lock must never be returned to
		// the pool, where its next user would inherit that lock.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

func lockObjectKeysChecked(bucketName string, keys ...string) (func(), error) {
	return lockObjectIDsChecked(objectLockIDs(bucketName, keys))
}

// tryLockObjectIDsChecked uses the same global ordering but never lets a
// background sweep block indefinitely behind an upload in another replica.
func tryLockObjectIDsChecked(ids []string, timeout time.Duration) (func(), bool) {
	deadline := time.Now().Add(timeout)
	entries := make([]*keyLockEntry, 0, len(ids))
	for _, id := range ids {
		e := acquireKeyLockEntry(id)
		select {
		case e.sem <- struct{}{}:
			entries = append(entries, e)
		default:
			remaining := time.Until(deadline)
			if remaining <= 0 {
				releaseKeyLockEntry(id, e)
				unlockKeyEntries(ids[:len(entries)], entries)
				return func() {}, false
			}
			timer := time.NewTimer(remaining)
			select {
			case e.sem <- struct{}{}:
				entries = append(entries, e)
			case <-timer.C:
				releaseKeyLockEntry(id, e)
				timer.Stop()
				unlockKeyEntries(ids[:len(entries)], entries)
				return func() {}, false
			}
			timer.Stop()
		}
	}
	localUnlock := func() { unlockKeyEntries(ids, entries) }
	if database.AdvisoryDB == nil {
		localUnlock()
		return func() {}, false
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	conn, err := database.AdvisoryDB.Conn(ctx)
	if err != nil {
		localUnlock()
		return func() {}, false
	}
	locked := 0
	for _, id := range ids {
		acquired := false
		for {
			err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", advisoryKey(id)).Scan(&acquired)
			if err != nil || acquired {
				break
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if remaining > 10*time.Millisecond {
				remaining = 10 * time.Millisecond
			}
			time.Sleep(remaining)
		}
		if err != nil || !acquired {
			unlockAdvisory(conn, ids[:locked])
			_ = conn.Close()
			localUnlock()
			return func() {}, false
		}
		locked++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			unlockAdvisory(conn, ids)
			_ = conn.Close()
			localUnlock()
		})
	}, true
}

func tryLockObjectKeysChecked(bucketName string, timeout time.Duration, keys ...string) (func(), bool) {
	return tryLockObjectIDsChecked(objectLockIDs(bucketName, keys), timeout)
}

// tryLockObjectKeys is lockObjectKeys with a deadline: it acquires all keys in
// the same global order, or — if they cannot all be acquired within timeout —
// releases whatever it took and returns ok=false holding nothing. On success
// the returned unlock func is idempotent; on failure it is a no-op.
func tryLockObjectKeys(bucketName string, timeout time.Duration, keys ...string) (unlock func(), ok bool) {
	ids := objectLockIDs(bucketName, keys)
	entries := make([]*keyLockEntry, 0, len(ids))
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for _, id := range ids {
		e := acquireKeyLockEntry(id)
		// Uncontended fast path first: once the timer has fired, a select
		// with both cases ready would pick at random.
		select {
		case e.sem <- struct{}{}:
			entries = append(entries, e)
			continue
		default:
		}
		select {
		case e.sem <- struct{}{}:
			entries = append(entries, e)
		case <-timer.C:
			releaseKeyLockEntry(id, e)
			unlockKeyEntries(ids[:len(entries)], entries)
			return func() {}, false
		}
	}
	var once sync.Once
	return func() { once.Do(func() { unlockKeyEntries(ids, entries) }) }, true
}

// ── Metadata commit helpers ──────────────────────────────────────────────────

// upsertCurrentObject records obj as the current state of its key with a
// single INSERT ... ON CONFLICT (bucket_id, key) DO UPDATE, so a concurrent or
// pre-existing row can never make the commit fail on the unique index (the
// failure mode that left stale rows after a successful byte write). The
// upsert and the reload of the stored row (so obj.ID/CreatedAt reflect the
// row actually in the table) run in one transaction: any error rolls both
// back, so a caller that treats an error as "not committed" and discards the
// written bytes can never leave a row pointing at deleted bytes.
func upsertCurrentObject(obj *models.Object) error {
	now := time.Now()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = now
	}
	obj.UpdatedAt = now
	if obj.StoragePath == "" {
		obj.StoragePath = obj.Key
	}
	var stored models.Object
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "bucket_id"}, {Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"size", "content_type", "content_disposition", "cache_control", "e_tag", "sha256", "storage_path",
				"metadata", "tags", "version_id", "updated_at",
			}),
		}).Create(obj).Error; err != nil {
			return err
		}
		// Reload into a fresh struct: obj.ID was assigned by BeforeCreate
		// and, on the conflict-update path, is not the stored row's id —
		// First(obj) would add "id = <that id>" to the WHERE clause and find
		// nothing.
		if err := tx.Where("bucket_id = ? AND key = ?", obj.BucketID, obj.Key).First(&stored).Error; err != nil {
			return fmt.Errorf("reload committed object row: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	*obj = stored
	return nil
}

// currentObjectExists reports whether key has a current metadata row.
func currentObjectExists(bucketID uuid.UUID, key string) bool {
	var n int64
	database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", bucketID, key).Count(&n)
	return n > 0
}

// discardFailedWrite undoes a byte write whose metadata commit failed:
//   - versioned overwrite: drop the new bytes and move the archived version
//     back (the key is exactly as before);
//   - brand-new key: drop the new bytes (no row, no orphan);
//   - unversioned overwrite: the previous bytes are already gone; the new
//     bytes are left in place (deleting them would leave the old row pointing
//     at nothing) and the inconsistency is logged.
func discardFailedWrite(backend storage.StorageBackend, bucket *models.Bucket, key, archivedVID string, hadPrior bool) {
	switch {
	case archivedVID != "":
		_ = backend.DeleteObject(bucket.Name, key)
		rollbackVersionedWrite(backend, bucket, key, archivedVID)
	case !hadPrior:
		_ = backend.DeleteObject(bucket.Name, key)
	default:
		logger.Warn("Object bytes overwritten but metadata commit failed; metadata may be stale", map[string]interface{}{
			"bucket": bucket.Name, "key": key,
		})
	}
}

// newCurrentVersionID returns the version id for a new current version ("" =
// the S3 "null" version when versioning is not enabled).
func newCurrentVersionID(bucket *models.Bucket) string {
	if bucket.Versioning == models.VersioningEnabled {
		return uuid.New().String()
	}
	return ""
}

// moveObjectWithinBucket moves src to dstKey inside one bucket, preserving
// versioning semantics: in a versioning-enabled bucket the destination gets a
// fresh current version and the source is deleted the versioned way (bytes
// archived + delete marker), exactly as a CopyObject + DeleteObject pair
// would; otherwise the metadata row is re-keyed and the source bytes removed.
//
// Preconditions (caller): holds lockObjectKeys for src.Key and dstKey; has
// verified dstKey has no current row; has applied permission and retention
// checks. On failure it restores the previous state as far as possible.
func moveObjectWithinBucket(backend storage.StorageBackend, bucket *models.Bucket, src *models.Object, dstKey string) (*models.Object, error) {
	if err := backend.CopyObject(bucket.Name, src.Key, dstKey); err != nil {
		return nil, fmt.Errorf("failed to copy %s: %w", src.Key, err)
	}
	now := time.Now()

	if bucket.Versioning == models.VersioningEnabled {
		dst := models.Object{
			BucketID:           bucket.ID,
			Key:                dstKey,
			Size:               src.Size,
			ContentType:        src.ContentType,
			ContentDisposition: src.ContentDisposition,
			CacheControl:       src.CacheControl,
			ETag:               src.ETag,
			SHA256:             src.SHA256,
			StoragePath:        dstKey,
			Metadata:           src.Metadata,
			Tags:               src.Tags,
			VersionID:          uuid.New().String(),
			CreatedAt:          now,
			UpdatedAt:          now,
		}
		if err := database.DB.Create(&dst).Error; err != nil {
			_ = backend.DeleteObject(bucket.Name, dstKey)
			return nil, fmt.Errorf("failed to record %s: %w", dstKey, err)
		}
		if _, _, err := versionedDeleteCurrent(backend, bucket, src); err != nil {
			database.DB.Delete(&models.Object{}, "id = ?", dst.ID)
			_ = backend.DeleteObject(bucket.Name, dstKey)
			return nil, fmt.Errorf("failed to remove source %s: %w", src.Key, err)
		}
		return &dst, nil
	}

	// Unversioned: re-key the row, then remove the source bytes.
	oldKey, oldPath, oldUpdated := src.Key, src.StoragePath, src.UpdatedAt
	res := database.DB.Model(&models.Object{}).Where("id = ? AND key = ?", src.ID, oldKey).
		Updates(map[string]interface{}{"key": dstKey, "storage_path": dstKey, "updated_at": now})
	if res.Error != nil || res.RowsAffected != 1 {
		_ = backend.DeleteObject(bucket.Name, dstKey)
		if res.Error != nil {
			return nil, fmt.Errorf("failed to update metadata for %s: %w", oldKey, res.Error)
		}
		return nil, fmt.Errorf("source %s changed during move", oldKey)
	}
	if err := backend.DeleteObject(bucket.Name, oldKey); err != nil {
		database.DB.Model(&models.Object{}).Where("id = ?", src.ID).
			Updates(map[string]interface{}{"key": oldKey, "storage_path": oldPath, "updated_at": oldUpdated})
		_ = backend.DeleteObject(bucket.Name, dstKey)
		return nil, fmt.Errorf("failed to delete source %s: %w", oldKey, err)
	}
	moved := *src
	moved.Key = dstKey
	moved.StoragePath = dstKey
	moved.UpdatedAt = now
	return &moved, nil
}

// ── Bucket response views ────────────────────────────────────────────────────

// bucketOwnerView is the owner summary shown to non-admin callers — never the
// full User record (email, SSO identifiers, admin flag).
type bucketOwnerView struct {
	ID       uuid.UUID `json:"id"`
	Username string    `json:"username"`
}

// bucketView is the bucket representation returned to non-admin callers. It
// omits the storage configuration reference (s3_config_id) and the owner's
// user record, and includes the webhook URL (often a bearer secret, e.g. Slack
// incoming webhooks) and the replication target only for callers allowed to
// change them — so the settings form can still prefill for those users.
type bucketView struct {
	ID             uuid.UUID        `json:"id"`
	Name           string           `json:"name"`
	OwnerID        uuid.UUID        `json:"owner_id"`
	IsPublic       bool             `json:"is_public"`
	Region         string           `json:"region"`
	StorageBackend string           `json:"storage_backend"`
	Versioning     string           `json:"versioning"`
	Lifecycle      *string          `json:"lifecycle,omitempty"`
	QuotaBytes     int64            `json:"quota_bytes"`
	RetentionDays  int              `json:"retention_days"`
	WebhookURL     string           `json:"webhook_url,omitempty"`
	WebhookEvents  string           `json:"webhook_events,omitempty"`
	ReplicateTo    string           `json:"replicate_to,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
	Owner          *bucketOwnerView `json:"owner,omitempty"`
	// PublicURLBase is set by GetBucket for public-read buckets.
	PublicURLBase string `json:"public_url_base,omitempty"`
	// Permissions is set by GetBucket (not by the bucket list).
	Permissions *bucketPermissionsView `json:"permissions,omitempty"`
}

func newBucketView(b *models.Bucket, showNotification, showReplication bool) bucketView {
	v := bucketView{
		ID:             b.ID,
		Name:           b.Name,
		OwnerID:        b.OwnerID,
		IsPublic:       b.IsPublic,
		Region:         b.Region,
		StorageBackend: b.StorageBackend,
		Versioning:     b.Versioning,
		Lifecycle:      b.Lifecycle,
		QuotaBytes:     b.QuotaBytes,
		RetentionDays:  b.RetentionDays,
		CreatedAt:      b.CreatedAt,
		UpdatedAt:      b.UpdatedAt,
	}
	if b.Owner.ID != uuid.Nil {
		v.Owner = &bucketOwnerView{ID: b.Owner.ID, Username: b.Owner.Username}
	}
	if showNotification {
		v.WebhookURL = b.WebhookURL
		v.WebhookEvents = b.WebhookEvents
	}
	if showReplication {
		v.ReplicateTo = b.ReplicateTo
	}
	return v
}
