package api

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Lifecycle: an honest subset of S3 lifecycle — one rule per bucket with
// age-based expiry of current objects (optional prefix) and permanent expiry
// of noncurrent versions. Configured via the S3 ?lifecycle subresource or the
// console REST endpoint; applied by a background sweep.

type lifecycleXML struct {
	XMLName xml.Name           `xml:"LifecycleConfiguration"`
	Rules   []lifecycleRuleXML `xml:"Rule"`
}

type lifecycleRuleXML struct {
	ID     string `xml:"ID,omitempty"`
	Status string `xml:"Status"`
	Prefix string `xml:"Prefix,omitempty"`
	Filter struct {
		Prefix string `xml:"Prefix,omitempty"`
	} `xml:"Filter,omitempty"`
	Expiration struct {
		Days int `xml:"Days,omitempty"`
	} `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration struct {
		NoncurrentDays int `xml:"NoncurrentDays,omitempty"`
	} `xml:"NoncurrentVersionExpiration,omitempty"`
}

func parseLifecycleConfig(b *models.Bucket) *models.LifecycleConfig {
	if b.Lifecycle == nil || *b.Lifecycle == "" {
		return nil
	}
	var cfg models.LifecycleConfig
	if err := json.Unmarshal([]byte(*b.Lifecycle), &cfg); err != nil {
		return nil
	}
	if cfg.ExpireDays <= 0 && cfg.NoncurrentExpireDays <= 0 {
		return nil
	}
	return &cfg
}

// storeLifecycleConfig saves (or, with cfg == nil, clears) the bucket's
// lifecycle and records who configured it: the sweep later expires keys only
// with that user's current permissions.
func storeLifecycleConfig(b *models.Bucket, cfg *models.LifecycleConfig, by uuid.UUID) error {
	if cfg == nil {
		return database.DB.Model(b).Updates(map[string]interface{}{
			"lifecycle":               nil,
			"lifecycle_configured_by": nil,
			"lifecycle_configured_at": nil,
		}).Error
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	s := string(raw)
	return database.DB.Model(b).Updates(map[string]interface{}{
		"lifecycle":               &s,
		"lifecycle_configured_by": by,
		"lifecycle_configured_at": time.Now(),
	}).Error
}

// requestUserID is the authenticated user of an S3 or console request.
func requestUserID(c *gin.Context) uuid.UUID {
	if v, ok := c.Get("user_id"); ok {
		if id, ok := v.(uuid.UUID); ok {
			return id
		}
	}
	return uuid.Nil
}

// bucketForConfigAction loads the bucket and requires admin or the given
// bucket-configuration policy action (ownership alone grants nothing).
func (h *S3APIHandler) bucketForConfigAction(c *gin.Context, action string) (*models.Bucket, bool) {
	bucketName := c.Param("bucket")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return nil, false
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return nil, false
	}
	if !authorizeBucketConfig(h.policyService, userUUID, bucket.Name, action) {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return nil, false
	}
	return &bucket, true
}

// GetBucketLifecycle handles GET /{bucket}?lifecycle.
func (h *S3APIHandler) GetBucketLifecycle(c *gin.Context) {
	bucket, ok := h.bucketForConfigAction(c, services.ActionGetLifecycleConfiguration)
	if !ok {
		return
	}
	cfg := parseLifecycleConfig(bucket)
	if cfg == nil {
		h.s3Error(c, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", bucket.Name, http.StatusNotFound)
		return
	}
	out := lifecycleXML{}
	rule := lifecycleRuleXML{ID: "bkt-rule", Status: "Enabled", Prefix: cfg.Prefix}
	rule.Expiration.Days = cfg.ExpireDays
	rule.NoncurrentVersionExpiration.NoncurrentDays = cfg.NoncurrentExpireDays
	out.Rules = append(out.Rules, rule)
	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, out)
}

// PutBucketLifecycle handles PUT /{bucket}?lifecycle (single-rule subset).
func (h *S3APIHandler) PutBucketLifecycle(c *gin.Context) {
	bucket, ok := h.bucketForConfigAction(c, services.ActionPutLifecycleConfiguration)
	if !ok {
		return
	}
	body, err := readBoundedBody(c.Request.Body, 256*1024)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			h.s3Error(c, "MaxMessageLengthExceeded", "Your request was too big", "", http.StatusBadRequest)
			return
		}
		h.s3Error(c, "InvalidRequest", "Failed to read request body", "", http.StatusBadRequest)
		return
	}
	var in lifecycleXML
	if err := xml.Unmarshal(body, &in); err != nil {
		h.s3Error(c, "MalformedXML", "The XML you provided was not well-formed", "", http.StatusBadRequest)
		return
	}
	enabled := []lifecycleRuleXML{}
	for _, r := range in.Rules {
		if strings.EqualFold(r.Status, "Enabled") {
			enabled = append(enabled, r)
		}
	}
	if len(enabled) == 0 {
		if err := storeLifecycleConfig(bucket, nil, uuid.Nil); err != nil {
			h.s3Error(c, "InternalError", "Failed to store lifecycle", "", http.StatusInternalServerError)
			return
		}
		c.Status(http.StatusOK)
		return
	}
	if len(enabled) > 1 {
		h.s3Error(c, "NotImplemented", "bkt supports a single lifecycle rule per bucket", "", http.StatusNotImplemented)
		return
	}
	r := enabled[0]
	prefix := r.Prefix
	if prefix == "" {
		prefix = r.Filter.Prefix
	}
	cfg := &models.LifecycleConfig{
		ExpireDays:           r.Expiration.Days,
		Prefix:               prefix,
		NoncurrentExpireDays: r.NoncurrentVersionExpiration.NoncurrentDays,
	}
	if cfg.ExpireDays <= 0 && cfg.NoncurrentExpireDays <= 0 {
		h.s3Error(c, "MalformedXML", "Rule must set Expiration.Days or NoncurrentVersionExpiration.NoncurrentDays", "", http.StatusBadRequest)
		return
	}
	if err := storeLifecycleConfig(bucket, cfg, requestUserID(c)); err != nil {
		h.s3Error(c, "InternalError", "Failed to store lifecycle", "", http.StatusInternalServerError)
		return
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// DeleteBucketLifecycle handles DELETE /{bucket}?lifecycle.
func (h *S3APIHandler) DeleteBucketLifecycle(c *gin.Context) {
	bucket, ok := h.bucketForConfigAction(c, services.ActionPutLifecycleConfiguration)
	if !ok {
		return
	}
	if err := storeLifecycleConfig(bucket, nil, uuid.Nil); err != nil {
		h.s3Error(c, "InternalError", "Failed to delete lifecycle", "", http.StatusInternalServerError)
		return
	}
	c.Status(http.StatusNoContent)
}

// SetBucketLifecycleREST handles PUT /api/buckets/:name/lifecycle for the
// console: {"expire_days": N, "prefix": "p/", "noncurrent_expire_days": M}.
// Zero/omitted for both disables lifecycle.
// @Summary Set bucket lifecycle
// @Description Configures age-based expiry for a bucket (single rule): current objects after expire_days, noncurrent versions after noncurrent_expire_days. Both zero clears the configuration. Requires admin or s3:PutLifecycleConfiguration on the bucket.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.SuccessResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/lifecycle [put]
func (h *BucketHandler) SetBucketLifecycleREST(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	var req models.LifecycleConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid request", Message: err.Error()})
		return
	}
	if req.ExpireDays < 0 || req.NoncurrentExpireDays < 0 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Days must not be negative"})
		return
	}
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}
	if !authorizeBucketConfig(h.policyService, userUUID, bucket.Name, services.ActionPutLifecycleConfiguration) {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Permission denied", Message: "Managing lifecycle requires admin or " + services.ActionPutLifecycleConfiguration + " on the bucket"})
		return
	}
	var cfg *models.LifecycleConfig
	if req.ExpireDays > 0 || req.NoncurrentExpireDays > 0 {
		cfg = &req
	}
	if err := storeLifecycleConfig(&bucket, cfg, userUUID); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to store lifecycle"})
		return
	}
	_ = h.auditService.LogSuccess(c, userUUID, "", "bucket.lifecycle", "bucket", bucket.ID.String(), bucket.Name,
		map[string]interface{}{"expire_days": req.ExpireDays, "prefix": req.Prefix, "noncurrent_expire_days": req.NoncurrentExpireDays})
	c.JSON(http.StatusOK, models.SuccessResponse{Message: "Lifecycle updated"})
}

// Lifecycle authorization: expiry acts on behalf of the user who configured
// the rule (LifecycleConfiguredBy) with that user's CURRENT permissions,
// evaluated per key: expiring a current object or a noncurrent version needs
// s3:DeleteObject on the key. Denied keys are skipped (counted, logged once
// per bucket per sweep). If the configurer is deleted or locked, lifecycle
// for the bucket is paused (the configuration is kept; re-saving it by an
// authorized user resumes it). Legacy configurations without a recorded
// configurer run as the bucket owner only while the owner is an active admin
// (and are skipped otherwise until re-saved) — the same rule as replication.

// legacyLifecycleWarned de-duplicates the per-bucket warning for legacy
// configurations that cannot run (no recorded configurer, non-admin owner).
var legacyLifecycleWarned sync.Map

const (
	lifecycleBatch = 1000
	// lifecycleMaxScan bounds candidates examined per phase per bucket and
	// sweep, so keys the configurer may not delete cannot starve the rest.
	lifecycleMaxScan = 10 * lifecycleBatch
)

// lifecyclePrincipalFor picks the user lifecycle runs as: the recorded
// configurer, else (legacy) the bucket owner if currently an active admin
// (owner == nil: not found). ok=false means skip.
func lifecyclePrincipalFor(configuredBy *uuid.UUID, owner *models.User) (uuid.UUID, bool) {
	if configuredBy != nil {
		return *configuredBy, true
	}
	if owner != nil && owner.IsAdmin && !owner.IsLocked {
		return owner.ID, true
	}
	return uuid.Nil, false
}

// lifecyclePrincipal resolves lifecyclePrincipalFor from the DB, warning once
// per bucket per process when a legacy configuration cannot run.
func lifecyclePrincipal(b *models.Bucket) (uuid.UUID, bool) {
	var owner *models.User
	if b.LifecycleConfiguredBy == nil {
		var o models.User
		if err := database.DB.Select("id", "is_admin", "is_locked").First(&o, "id = ?", b.OwnerID).Error; err == nil {
			owner = &o
		}
	}
	id, ok := lifecyclePrincipalFor(b.LifecycleConfiguredBy, owner)
	if !ok {
		if _, warned := legacyLifecycleWarned.LoadOrStore(b.ID, true); !warned {
			logger.Warn("Lifecycle: legacy configuration has no recorded configurer and the bucket owner is not an active admin — skipping; re-save the lifecycle rule to resume", map[string]interface{}{"bucket": b.Name})
		}
	}
	return id, ok
}

// lifecycleEvaluator loads the principal's permissions on the bucket once
// for this sweep. ok=false means skip the bucket this sweep; a deleted or
// locked configurer pauses lifecycle (the configuration is kept).
func lifecycleEvaluator(ps *services.PolicyService, b *models.Bucket) (*services.AccessEvaluator, bool) {
	principal, ok := lifecyclePrincipal(b)
	if !ok {
		return nil, false
	}
	eval, err := ps.NewAccessEvaluator(principal, b.Name)
	if err != nil {
		logger.Warn("Lifecycle: failed to load permissions", map[string]interface{}{"bucket": b.Name, "error": err.Error()})
		return nil, false
	}
	if eval.UserMissing() || eval.UserLocked() {
		logger.Warn("Lifecycle paused: the user who configured it was deleted or locked — re-save the lifecycle rule to resume", map[string]interface{}{"bucket": b.Name})
		return nil, false
	}
	return eval, true
}

// lifecycleExpiryAllowed reports whether the lifecycle principal may expire
// key (current object or noncurrent version).
func lifecycleExpiryAllowed(eval *services.AccessEvaluator, key string) bool {
	return eval.Allowed(services.ActionDeleteObject, key)
}

// lifecycleCurrentCandidates returns up to limit current objects last
// written before cutoff (under prefix), in key order after afterKey.
func lifecycleCurrentCandidates(bucketID uuid.UUID, prefix string, cutoff time.Time, afterKey string, limit int) ([]models.Object, error) {
	var out []models.Object
	q := database.DB.Where("bucket_id = ? AND updated_at < ? AND key > ?", bucketID, cutoff, afterKey)
	if prefix != "" {
		q = q.Where("key LIKE ?", validation.EscapeLikeWildcards(prefix)+"%")
	}
	return out, q.Order("key").Limit(limit).Find(&out).Error
}

// lifecycleVersionCandidates returns up to limit versions that stopped being
// current before cutoff (under prefix), oldest first; with after set, only
// those strictly after (afterAt, afterID) in that order.
func lifecycleVersionCandidates(bucketID uuid.UUID, prefix string, cutoff time.Time, after bool, afterAt time.Time, afterID uuid.UUID, limit int) ([]models.ObjectVersion, error) {
	var out []models.ObjectVersion
	q := database.DB.Where("bucket_id = ? AND versioned_at < ?", bucketID, cutoff)
	if after {
		q = q.Where("(versioned_at, id) > (?, ?)", afterAt, afterID)
	}
	if prefix != "" {
		q = q.Where("key LIKE ?", validation.EscapeLikeWildcards(prefix)+"%")
	}
	return out, q.Order("versioned_at ASC, id ASC").Limit(limit).Find(&out).Error
}

// lifecyclePhase counts one expiry phase of one bucket.
type lifecyclePhase struct {
	expired, busy, denied int
}

// RunLifecycleSweep applies every bucket's lifecycle rules once. Called from
// the background scheduler; also invocable directly in tests.
func RunLifecycleSweep(cfg *config.Config) {
	h := NewBucketHandler(cfg)
	var buckets []models.Bucket
	if err := database.DB.Where("lifecycle IS NOT NULL").Find(&buckets).Error; err != nil {
		logger.Warn("Lifecycle sweep: failed to list buckets", map[string]interface{}{"error": err.Error()})
		return
	}
	for i := range buckets {
		b := &buckets[i]
		lc := parseLifecycleConfig(b)
		if lc == nil {
			continue
		}
		eval, ok := lifecycleEvaluator(h.policyService, b)
		if !ok {
			continue
		}
		backend, err := h.getStorageBackend(b)
		if err != nil {
			logger.Warn("Lifecycle sweep: storage init failed", map[string]interface{}{"bucket": b.Name, "error": err.Error()})
			continue
		}
		denied := 0

		// Expire current objects.
		if lc.ExpireDays > 0 {
			cutoff := time.Now().AddDate(0, 0, -lc.ExpireDays)
			var ph lifecyclePhase
			last, attempted := "", 0
		currentScan:
			for scanned := 0; scanned < lifecycleMaxScan && attempted < lifecycleBatch; {
				expired, err := lifecycleCurrentCandidates(b.ID, lc.Prefix, cutoff, last, lifecycleBatch)
				if err != nil || len(expired) == 0 {
					break
				}
				scanned += len(expired)
				last = expired[len(expired)-1].Key
				for i := range expired {
					if ph.busy >= sweepMaxBusyKeys {
						break currentScan // the rest is retried next sweep
					}
					if !lifecycleExpiryAllowed(eval, expired[i].Key) {
						ph.denied++
						continue
					}
					attempted++
					done, wasBusy := expireCurrentObject(backend, b, expired[i].Key, cutoff)
					if done {
						ph.expired++
					} else if wasBusy {
						ph.busy++
					}
				}
				if len(expired) < lifecycleBatch {
					break
				}
			}
			if ph.expired > 0 {
				logger.Info("Lifecycle: expired current objects", map[string]interface{}{"bucket": b.Name, "count": ph.expired})
			}
			if ph.busy > 0 {
				logger.Info("Lifecycle: busy keys deferred to the next sweep", map[string]interface{}{"bucket": b.Name, "busy": ph.busy})
			}
			denied += ph.denied
		}

		// Expire noncurrent versions permanently (oldest first, so a marker
		// delete cannot resurrect content that is itself due for expiry).
		// Each candidate is re-checked against the key's state at the time
		// it is processed (earlier deletions in this pass change it); see
		// noncurrentExpiryAllowed for the rules — notably a key's LATEST
		// delete marker is only removed once it is the sole version left.
		if lc.NoncurrentExpireDays > 0 {
			cutoff := time.Now().AddDate(0, 0, -lc.NoncurrentExpireDays)
			var ph lifecyclePhase
			var lastAt time.Time
			var lastID uuid.UUID
			attempted := 0
		versionScan:
			for scanned := 0; scanned < lifecycleMaxScan && attempted < lifecycleBatch; {
				vers, err := lifecycleVersionCandidates(b.ID, lc.Prefix, cutoff, scanned > 0, lastAt, lastID, lifecycleBatch)
				if err != nil || len(vers) == 0 {
					break
				}
				scanned += len(vers)
				lastAt, lastID = vers[len(vers)-1].VersionedAt, vers[len(vers)-1].ID
				for i := range vers {
					if ph.busy >= sweepMaxBusyKeys {
						break versionScan // the rest is retried next sweep
					}
					if !lifecycleExpiryAllowed(eval, vers[i].Key) {
						ph.denied++
						continue
					}
					attempted++
					done, wasBusy := expireNoncurrentVersion(backend, b, &vers[i])
					if done {
						ph.expired++
					} else if wasBusy {
						ph.busy++
					}
				}
				if len(vers) < lifecycleBatch {
					break
				}
			}
			if ph.expired > 0 {
				logger.Info("Lifecycle: expired noncurrent versions", map[string]interface{}{"bucket": b.Name, "count": ph.expired})
			}
			if ph.busy > 0 {
				logger.Info("Lifecycle: busy keys deferred to the next sweep", map[string]interface{}{"bucket": b.Name, "busy": ph.busy})
			}
			denied += ph.denied
		}

		if denied > 0 {
			logger.Warn("Lifecycle: keys skipped — the user who configured lifecycle may not s3:DeleteObject them", map[string]interface{}{"bucket": b.Name, "skipped": denied})
		}
	}
}

// expireCurrentObject expires one current object under the key's write lock.
// The row is re-read under the lock: a concurrent overwrite or delete since
// the candidate query must not be undone by a stale expiry. The lock is
// try-acquired (sweepLockTimeout); a busy key reports busy=true and is left
// for the next sweep rather than stalling the whole sweep.
func expireCurrentObject(backend storage.StorageBackend, b *models.Bucket, key string, cutoff time.Time) (expired, busy bool) {
	unlock, ok := tryLockObjectKeysChecked(b, sweepLockTimeout, key)
	if !ok {
		return false, true
	}
	defer unlock()

	var obj models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", b.ID, key).First(&obj).Error; err != nil {
		return false, false // already gone
	}
	if !obj.UpdatedAt.Before(cutoff) {
		return false, false // rewritten since the candidate query
	}
	if _, handled, derr := versionedDeleteCurrent(backend, b, &obj); handled {
		if derr != nil {
			logger.Warn("Lifecycle: versioned expiry failed", map[string]interface{}{"bucket": b.Name, "key": key, "error": derr.Error()})
			return false, false
		}
		return true, false
	}
	if retentionBlocks(b, obj.UpdatedAt) {
		return false, false // defensive: retention requires versioning, so not normally reachable
	}
	// For generation-capable storage, the row is the visibility pointer. Keep
	// its bytes available to readers that pinned the row before this delete.
	if _, generationAware := backend.(storage.GenerationStorage); generationAware {
		generationCommitBoundary("lifecycle-expire")
	} else if derr := backend.DeleteObject(b.Name, key); derr != nil {
		logger.Warn("Lifecycle: expiry failed", map[string]interface{}{"bucket": b.Name, "key": key, "error": derr.Error()})
		return false, false
	}
	result := database.DB.Delete(&models.Object{}, "id = ?", obj.ID)
	if result.Error != nil || result.RowsAffected != 1 {
		logger.Warn("Lifecycle: metadata expiry failed", map[string]interface{}{"bucket": b.Name, "key": key, "error": result.Error, "rows": result.RowsAffected})
		return false, false
	}
	return true, false
}

// expireNoncurrentVersion permanently removes one noncurrent version under
// the key's write lock when noncurrentVersionExpirable allows it (evaluated
// under the lock). Reports whether the version was removed, and whether the
// key was busy (lock not acquired within sweepLockTimeout; retried later).
func expireNoncurrentVersion(backend storage.StorageBackend, b *models.Bucket, v *models.ObjectVersion) (removed, busy bool) {
	unlock, ok := tryLockObjectKeysChecked(b, sweepLockTimeout, v.Key)
	if !ok {
		return false, true
	}
	defer unlock()

	var fresh models.ObjectVersion
	if err := database.DB.Where("id = ?", v.ID).First(&fresh).Error; err != nil {
		return false, false // already removed
	}
	if !noncurrentVersionExpirable(b, &fresh) {
		return false, false // retained content, or a latest delete marker that still hides versions
	}
	if err := deleteSpecificVersion(backend, b, fresh.Key, fresh.VersionID); err != nil {
		logger.Warn("Lifecycle: version expiry failed", map[string]interface{}{"bucket": b.Name, "key": fresh.Key, "version": fresh.VersionID, "error": err.Error()})
		return false, false
	}
	return true, false
}

// noncurrentVersionExpirable gathers the key's current state from the DB and
// applies noncurrentExpiryAllowed to one object_versions entry.
func noncurrentVersionExpirable(b *models.Bucket, v *models.ObjectVersion) bool {
	retained := !v.IsDeleteMarker && retentionBlocks(b, v.ContentModifiedAt)
	isLatestMarker, otherVersionsRemain := false, false
	if v.IsDeleteMarker {
		var current int64
		if err := database.DB.Model(&models.Object{}).
			Where("bucket_id = ? AND key = ?", b.ID, v.Key).Count(&current).Error; err != nil {
			return false // fail safe: keep the marker
		}
		var newer int64
		if err := database.DB.Model(&models.ObjectVersion{}).
			Where("bucket_id = ? AND key = ? AND versioned_at > ? AND id <> ?", b.ID, v.Key, v.VersionedAt, v.ID).
			Count(&newer).Error; err != nil {
			return false
		}
		isLatestMarker = current == 0 && newer == 0
		var others int64
		if err := database.DB.Model(&models.ObjectVersion{}).
			Where("bucket_id = ? AND key = ? AND id <> ?", b.ID, v.Key, v.ID).Count(&others).Error; err != nil {
			return false
		}
		otherVersionsRemain = others > 0 || current > 0
	}
	return noncurrentExpiryAllowed(v.IsDeleteMarker, retained, isLatestMarker, otherVersionsRemain)
}
