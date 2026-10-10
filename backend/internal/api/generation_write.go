package api

import (
	"errors"
	"fmt"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// commitStagedGeneration publishes an already durable, create-only blob. The
// caller must hold the object's cross-replica key lock through this commit.
// Staged blobs are intentionally retained on a failed/ambiguous DB commit:
// deleting one could remove bytes that a successful commit already names.
func commitStagedGeneration(backend storage.GenerationStorage, bucket *models.Bucket, obj *models.Object) error {
	if obj.GenerationID == "" {
		return fmt.Errorf("cannot commit an object without a generation")
	}
	var previous models.Object
	priorErr := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, obj.Key).First(&previous).Error
	if priorErr != nil && !errors.Is(priorErr, gorm.ErrRecordNotFound) {
		return fmt.Errorf("load current object before generation commit: %w", priorErr)
	}
	var archived *models.ObjectVersion
	if priorErr == nil && bucket.Versioning == models.VersioningEnabled {
		oldGeneration := previous.GenerationID
		if oldGeneration == "" {
			// A pre-migration row still names the public mutable storage slot.
			// Copy it into an immutable generation without moving or deleting the
			// only bytes visible to the committed current row.
			legacyBackend, ok := backend.(storage.StorageBackend)
			if !ok {
				return fmt.Errorf("generation backend cannot read legacy current object")
			}
			rc, err := getCurrentBytes(legacyBackend, bucket.Name, &previous)
			if err != nil {
				return fmt.Errorf("open legacy current object: %w", err)
			}
			oldGeneration = uuid.NewString()
			_, stageErr := backend.PutGeneration(bucket.Name, previous.Key, oldGeneration, rc, previous.Size, previous.ContentType, jsonPtrToMap(previous.Metadata))
			closeErr := rc.Close()
			if stageErr != nil {
				return fmt.Errorf("stage legacy current object: %w", stageErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close legacy current object: %w", closeErr)
			}
		}
		versionID := previous.VersionID
		if versionID == "" {
			versionID = uuid.NewString()
		}
		archived = &models.ObjectVersion{
			BucketID: bucket.ID, Key: previous.Key, VersionID: versionID,
			GenerationID: oldGeneration, Size: previous.Size, ETag: previous.ETag,
			ContentType: previous.ContentType, ContentDisposition: previous.ContentDisposition,
			CacheControl: previous.CacheControl, Metadata: previous.Metadata, Tags: previous.Tags,
			VersionedAt: time.Now(), ContentModifiedAt: previous.UpdatedAt,
		}
	}
	return database.DB.Transaction(func(tx *gorm.DB) error {
		if archived != nil {
			if err := tx.Create(archived).Error; err != nil {
				return fmt.Errorf("archive prior generation row: %w", err)
			}
		}
		if err := upsertCurrentObjectInTx(tx, obj); err != nil {
			return fmt.Errorf("commit current generation row: %w", err)
		}
		return nil
	})
}
