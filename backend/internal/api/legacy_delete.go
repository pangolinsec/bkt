package api

import (
	"fmt"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"
)

// A generation-aware backend can still contain current objects written before
// generations existed. Their public key must be removed after the metadata
// delete commits, or S3 listing reconciliation will import the key again.
// Keep the old row if that cleanup fails so a failed delete does not silently
// turn a still-present legacy object into an unindexed object.
func deleteLegacyCurrentAfterCommit(backend storage.StorageBackend, bucket *models.Bucket, obj *models.Object) error {
	if obj.GenerationID != "" {
		return nil
	}
	if _, ok := backend.(storage.GenerationStorage); !ok {
		return nil
	}
	if err := backend.DeleteObject(bucket.Name, obj.Key); err != nil {
		if restoreErr := database.DB.Create(obj).Error; restoreErr != nil {
			return fmt.Errorf("delete legacy bytes: %w; restore metadata: %v", err, restoreErr)
		}
		return fmt.Errorf("delete legacy bytes: %w", err)
	}
	return nil
}
