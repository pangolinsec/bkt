package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Console folder endpoints. A "folder" is a key prefix ending in "/": the
// objects under it plus, optionally, a zero-byte "<prefix>" marker object
// (the S3 convention used by aws-cli, rclone, s3fs and the AWS console) or a
// legacy "<prefix>.keep" placeholder written by older console versions.

// maxFolderDeleteObjects bounds a single console folder delete; larger
// folders are refused (409) rather than half-deleted by one request.
const maxFolderDeleteObjects = 10000

// maxFolderDeleteEvents caps the webhook events one folder delete emits.
const maxFolderDeleteEvents = 1000

// withQueryObjectKey adapts a handler that reads the object key from the
// "/*key" path wildcard so it can be mounted on a route that takes the key
// from the "key" query parameter instead. Keys in a URL path are not
// addressable byte-for-byte: browsers and HTTP clients resolve "." and ".."
// segments (including their %2e spellings) and "%" sequences are decoded, so
// the console sends every key as a query parameter.
func withQueryObjectKey(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		key, ok := c.GetQuery("key")
		if !ok || key == "" {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "key query parameter is required"})
			return
		}
		// The wrapped handlers strip exactly one leading "/" from the
		// wildcard value (gin includes it), so prepend one.
		c.Params = append(c.Params, gin.Param{Key: "key", Value: "/" + key})
		next(c)
	}
}

// folderPrefixParam reads and validates the "prefix" query parameter of the
// folder endpoints, writing a 400 response when it is not a folder prefix.
func folderPrefixParam(c *gin.Context) (string, bool) {
	prefix := c.Query("prefix")
	if prefix == "" || !strings.HasSuffix(prefix, "/") {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid folder prefix",
			Message: "prefix must be a folder prefix ending in '/'",
		})
		return "", false
	}
	if err := validation.ValidateObjectKey(prefix); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "Invalid folder prefix", Message: err.Error()})
		return "", false
	}
	return prefix, true
}

// currentObjectsUnder returns up to limit current objects whose key starts
// with prefix, in key order.
func currentObjectsUnder(bucketID uuid.UUID, prefix string, limit int) ([]models.Object, error) {
	var rows []models.Object
	if err := database.DB.Where("bucket_id = ? AND key LIKE ?", bucketID, validation.EscapeLikeWildcards(prefix)+"%").
		Order("key ASC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := rows[:0]
	for _, r := range rows {
		if strings.HasPrefix(r.Key, prefix) { // defensive: LIKE collation quirks
			out = append(out, r)
		}
	}
	return out, nil
}

// GetFolderSummary handles GET /api/buckets/:name/folders?prefix=P — how many
// current objects (including the folder's own marker) a folder delete would
// remove.
// @Summary Summarize a folder
// @Description Counts the current objects under a folder prefix (including a "<prefix>" marker object). Requires ListBucket on the bucket.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Param prefix query string true "Folder prefix, ending in '/'"
// @Success 200 {object} object
// @Security BearerAuth
// @Router /api/buckets/{name}/folders [get]
func (h *BucketHandler) GetFolderSummary(c *gin.Context) {
	prefix, ok := folderPrefixParam(c)
	if !ok {
		return
	}
	bucketName := c.Param("name")
	userUUID := c.MustGet("user_id").(uuid.UUID)
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}
	if allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket); err != nil || !allowed {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Permission denied", Message: "You don't have permission to list objects in this bucket"})
		return
	}
	var count int64
	if err := database.DB.Model(&models.Object{}).
		Where("bucket_id = ? AND key LIKE ?", bucket.ID, validation.EscapeLikeWildcards(prefix)+"%").
		Count(&count).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to count objects", Message: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"prefix":       prefix,
		"object_count": count,
		"max_objects":  maxFolderDeleteObjects,
		"too_large":    count > maxFolderDeleteObjects,
		"versioning":   bucket.Versioning,
	})
}

// DeleteFolder handles DELETE /api/buckets/:name/folders?prefix=P: deletes
// every current object under P — including the "P" marker and a legacy
// "P.keep" placeholder — exactly as the single-object delete would (delete
// markers on versioned buckets, WORM retention honored). Objects the caller
// may not delete are skipped and counted.
// @Summary Delete a folder
// @Description Deletes every current object under a folder prefix (at most 10000). Versioned buckets get delete markers. Objects under retention or without DeleteObject permission are skipped and counted.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Param prefix query string true "Folder prefix, ending in '/'"
// @Success 200 {object} object "{deleted, skipped_retention, denied}"
// @Failure 403 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/folders [delete]
func (h *BucketHandler) DeleteFolder(c *gin.Context) {
	prefix, ok := folderPrefixParam(c)
	if !ok {
		return
	}
	bucketName := c.Param("name")
	userUUID := c.MustGet("user_id").(uuid.UUID)

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}

	candidates, err := currentObjectsUnder(bucket.ID, prefix, maxFolderDeleteObjects+1)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to list folder", Message: err.Error()})
		return
	}
	if len(candidates) > maxFolderDeleteObjects {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error: "Folder too large",
			Message: fmt.Sprintf("The console deletes at most %d objects at once; delete this folder with an S3 client (e.g. aws s3 rm --recursive) or delete its subfolders first",
				maxFolderDeleteObjects),
		})
		return
	}

	// One evaluator (policies loaded and parsed once) for all per-key checks.
	evaluator, err := h.policyService.NewAccessEvaluator(userUUID, bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Policy check failed", Message: err.Error()})
		return
	}
	allowedKeys := make([]string, 0, len(candidates))
	denied := 0
	for _, obj := range candidates {
		if evaluator.Allowed(services.ActionDeleteObject, obj.Key) {
			allowedKeys = append(allowedKeys, obj.Key)
		} else {
			denied++
		}
	}
	if denied > 0 && len(allowedKeys) == 0 {
		c.JSON(http.StatusForbidden, models.ErrorResponse{
			Error:   "Permission denied",
			Message: "You don't have permission to delete the objects in this folder",
		})
		return
	}

	backend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to initialize storage backend", Message: err.Error()})
		return
	}

	deleted, skippedRetention, eventsSent := 0, 0, 0
	for _, key := range allowedKeys {
		removed, retained, derr := deleteCurrentObjectLocked(c.Request.Context(), backend, &bucket, key)
		if derr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":             "Failed to delete folder",
				"message":           fmt.Sprintf("%s: %v", key, derr),
				"deleted":           deleted,
				"skipped_retention": skippedRetention,
				"denied":            denied,
			})
			return
		}
		if retained {
			skippedRetention++
			continue
		}
		if !removed {
			continue // deleted concurrently
		}
		deleted++
		if eventsSent < maxFolderDeleteEvents {
			notifyObjectEvent(&bucket, services.EventObjectRemoved, key, 0, "", "")
			eventsSent++
		}
	}
	if deleted > eventsSent {
		logger.Warn("Folder delete: webhook events capped", map[string]interface{}{
			"bucket": bucketName, "prefix": prefix, "deleted": deleted, "events_sent": eventsSent,
		})
	}

	_ = h.auditService.LogSuccess(c, userUUID, "", "folder.delete", "bucket", bucket.ID.String(), bucket.Name,
		map[string]interface{}{"prefix": prefix, "deleted": deleted, "skipped_retention": skippedRetention, "denied": denied})

	c.JSON(http.StatusOK, gin.H{
		"prefix":            prefix,
		"deleted":           deleted,
		"skipped_retention": skippedRetention,
		"denied":            denied,
	})
}

// deleteCurrentObjectLocked deletes the current version of key under the
// key's write lock, the same way the single-object delete does: a versioned
// bucket archives the bytes and records a delete marker; otherwise the delete
// is permanent and WORM retention forbids it (retained=true). removed=false
// with no error means the key no longer had a current version.
func deleteCurrentObjectLocked(ctx context.Context, backend storage.StorageBackend, bucket *models.Bucket, key string) (removed, retained bool, err error) {
	unlock, lockErr := lockObjectKeysChecked(ctx, bucket, key)
	if lockErr != nil {
		return false, false, lockErr
	}
	defer unlock()

	var obj models.Object
	if database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).First(&obj).Error != nil {
		return false, false, nil
	}
	if _, handled, derr := versionedDeleteCurrent(backend, bucket, &obj); handled {
		if derr != nil {
			return false, false, derr
		}
		return true, false, nil
	}
	if retentionBlocks(bucket, obj.UpdatedAt) {
		return false, true, nil
	}
	if err := backend.DeleteObject(bucket.Name, key); err != nil {
		return false, false, fmt.Errorf("failed to delete object from storage: %w", err)
	}
	if err := database.DB.Delete(&models.Object{}, "id = ?", obj.ID).Error; err != nil {
		return false, false, fmt.Errorf("failed to delete object metadata: %w", err)
	}
	return true, false, nil
}
