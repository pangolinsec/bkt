package api

import (
	"errors"
	"net/http"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Console (REST) versioning endpoints backing the UI's version history.

type versionEntryJSON struct {
	VersionID      string    `json:"version_id"`
	IsLatest       bool      `json:"is_latest"`
	IsDeleteMarker bool      `json:"is_delete_marker"`
	Size           int64     `json:"size"`
	ContentType    string    `json:"content_type,omitempty"`
	ETag           string    `json:"etag,omitempty"`
	LastModified   time.Time `json:"last_modified"`
}

// loadBucketForVersioning does the shared bucket lookup + object-read authz.
func (h *BucketHandler) loadBucketForVersioning(c *gin.Context, action string) (*models.Bucket, string, uuid.UUID, bool) {
	bucketName := c.Param("name")
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "key query parameter is required"})
		return nil, "", uuid.Nil, false
	}
	// bkt's internal version keyspace is never an object.
	if validation.IsReservedObjectKey(key) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid object key"})
		return nil, "", uuid.Nil, false
	}
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Version operations are never covered by public-read (AWS:
	// s3:GetObjectVersion / s3:ListBucketVersions), so only policies decide.
	allowed, err := h.policyService.CheckObjectAccessWithoutPublicRead(userUUID, bucketName, key, action)
	if err != nil || !allowed {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Permission denied"})
		return nil, "", uuid.Nil, false
	}
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return nil, "", uuid.Nil, false
	}
	return &bucket, key, userUUID, true
}

// ListObjectVersionsREST handles GET /api/buckets/:name/objects/versions?key=K
// @Summary List an object's versions
// @Description Lists the current version, archived versions, and delete markers for one object key, newest first.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Param key query string true "Object key"
// @Success 200 {object} object
// @Security BearerAuth
// @Router /api/buckets/{name}/object-versions [get]
func (h *BucketHandler) ListObjectVersionsREST(c *gin.Context) {
	bucket, key, _, ok := h.loadBucketForVersioning(c, services.ActionGetObject)
	if !ok {
		return
	}

	entries := []versionEntryJSON{}
	var current models.Object
	if database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).First(&current).Error == nil {
		vid := current.VersionID
		if vid == "" {
			vid = "null"
		}
		entries = append(entries, versionEntryJSON{
			VersionID: vid, IsLatest: true, Size: current.Size,
			ContentType: current.ContentType, ETag: current.ETag,
			LastModified: current.UpdatedAt,
		})
	}
	vers := []models.ObjectVersion{}
	database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).
		Order(versionOrder).Limit(500).Find(&vers)
	for _, v := range vers {
		lm := v.ContentModifiedAt
		if v.IsDeleteMarker {
			lm = v.VersionedAt
		}
		entries = append(entries, versionEntryJSON{
			VersionID: v.VersionID, IsDeleteMarker: v.IsDeleteMarker,
			Size: v.Size, ContentType: v.ContentType, ETag: v.ETag,
			LastModified: lm,
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"bucket":     bucket.Name,
		"key":        key,
		"versioning": bucket.Versioning,
		"versions":   entries,
	})
}

// RestoreObjectVersion handles POST /api/buckets/:name/objects/restore
// {key, version_id}: makes an archived version the current one by COPYING it
// forward (history is preserved; the previous current is archived first).
// @Summary Restore an object version
// @Description Copies an archived version forward as the new current version. The previous current version is archived. History is preserved.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.SuccessResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/restore [post]
func (h *BucketHandler) RestoreObjectVersion(c *gin.Context) {
	bucketName := c.Param("name")
	var req struct {
		Key       string `json:"key" binding:"required"`
		VersionID string `json:"version_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid request", Message: err.Error()})
		return
	}
	if validation.IsReservedObjectKey(req.Key) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid object key"})
		return
	}
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)
	if allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, req.Key, services.ActionPutObject); err != nil || !allowed {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Permission denied"})
		return
	}
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}
	if err := validation.ValidateObjectKey(req.Key); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid object key", Message: err.Error()})
		return
	}

	// Serialize with every other mutation of this key (archive + write +
	// commit must not interleave). The versioning helpers do not lock.
	unlock, lockErr := lockObjectKeysChecked(c.Request.Context(), &bucket, req.Key)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock object"})
		return
	}
	defer unlock()

	var ver models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, req.Key, req.VersionID).
		First(&ver).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Version not found"})
		return
	}
	if ver.IsDeleteMarker {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Cannot restore a delete marker"})
		return
	}

	backend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to initialize storage"})
		return
	}
	generations, ok := backend.(storage.GenerationStorage)
	if !ok {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Storage does not support immutable generations"})
		return
	}

	// The restored bytes become current and count against the quota.
	reservation, qerr := reserveBucketQuota(&bucket, ver.Size)
	if qerr != nil {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Quota exceeded", Message: qerr.Error()})
		return
	}
	defer reservation.release()

	// Read the exact historical generation and stage a distinct current
	// generation. The requested history row remains untouched.
	rc, err := getVersionBytes(backend, bucket.Name, &ver)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to read version"})
		return
	}
	body := newGuardedBody(rc, ver.Size, 0, reservation)
	generationID := uuid.NewString()
	_, stageErr := generations.PutGeneration(bucket.Name, req.Key, generationID, body, ver.Size, ver.ContentType, jsonPtrToMap(ver.Metadata))
	closeErr := rc.Close()
	if stageErr != nil || !body.Complete() || closeErr != nil {
		if stageErr == nil {
			_ = generations.DeleteGeneration(bucket.Name, req.Key, generationID)
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to restore version"})
		return
	}
	generationCommitBoundary("restore")

	obj := models.Object{
		BucketID: bucket.ID, Key: req.Key, Size: ver.Size,
		ContentType: ver.ContentType, ContentDisposition: ver.ContentDisposition,
		CacheControl: ver.CacheControl, ETag: ver.ETag, StoragePath: req.Key, GenerationID: generationID,
		Metadata: ver.Metadata, Tags: ver.Tags, VersionID: newCurrentVersionID(&bucket),
	}
	if err := commitStagedGeneration(generations, &bucket, &obj); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to record restored object"})
		return
	}
	c.JSON(http.StatusOK, models.SuccessResponse{Message: "Version restored"})
}

// DeleteObjectVersionREST handles DELETE /api/buckets/:name/objects/versions?key=K&version_id=V —
// permanent removal of one version (or delete marker).
// @Summary Permanently delete an object version
// @Description Permanently removes one version or delete marker. Removing the current version or a latest delete marker promotes the next-newest surviving version.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Param key query string true "Object key"
// @Param version_id query string true "Version ID"
// @Success 200 {object} models.SuccessResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/object-versions [delete]
func (h *BucketHandler) DeleteObjectVersionREST(c *gin.Context) {
	bucket, key, _, ok := h.loadBucketForVersioning(c, services.ActionDeleteObject)
	if !ok {
		return
	}
	versionID := c.Query("version_id")
	if versionID == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "version_id query parameter is required"})
		return
	}
	backend, err := h.getStorageBackend(bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to initialize storage"})
		return
	}
	unlock, lockErr := lockObjectKeysChecked(c.Request.Context(), bucket, key)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock object"})
		return
	}
	defer unlock()
	if err := deleteSpecificVersion(backend, bucket, key, versionID); err != nil {
		switch {
		case errors.Is(err, errVersionNotFound):
			c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Version not found"})
		case errors.Is(err, errUnderRetention):
			c.JSON(http.StatusConflict, models.ErrorResponse{Error: "Version is under retention", Message: err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to delete version", Message: err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, models.SuccessResponse{Message: "Version deleted"})
}

// SetBucketVersioning handles PUT /api/buckets/:name/versioning {"versioning": "enabled"|"suspended"}
// (admin, or s3:PutBucketVersioning on the bucket).
// @Summary Set bucket versioning
// @Description Enables or suspends versioning on a bucket. Requires admin or s3:PutBucketVersioning on the bucket.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.SuccessResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/versioning [put]
func (h *BucketHandler) SetBucketVersioning(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	var req struct {
		Versioning string `json:"versioning" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid request", Message: err.Error()})
		return
	}
	if req.Versioning != models.VersioningEnabled && req.Versioning != models.VersioningSuspended {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "versioning must be 'enabled' or 'suspended'"})
		return
	}
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}
	if !authorizeBucketConfig(h.policyService, userUUID, bucket.Name, services.ActionPutBucketVersioning) {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Permission denied", Message: "Changing versioning requires admin or " + services.ActionPutBucketVersioning + " on the bucket"})
		return
	}
	unlockBucket, lockErr := lockBucketExclusive(c.Request.Context(), &bucket)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock bucket", Message: lockErr.Error()})
		return
	}
	defer unlockBucket()
	if err := refreshLockedBucket(c.Request.Context(), &bucket); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to reload bucket", Message: err.Error()})
		return
	}
	if req.Versioning == models.VersioningSuspended && bucket.RetentionDays > 0 {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error:   "Retention active",
			Message: "Versioning cannot be suspended while a retention period is set — clear retention first (possible only once no data is still under retention)",
		})
		return
	}
	if err := database.DB.Model(&bucket).Update("versioning", req.Versioning).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to update versioning"})
		return
	}
	_ = h.auditService.LogSuccess(c, userUUID, "", "bucket.versioning", "bucket", bucket.ID.String(), bucket.Name,
		map[string]interface{}{"versioning": req.Versioning})
	c.JSON(http.StatusOK, models.SuccessResponse{Message: "Versioning " + req.Versioning})
}
