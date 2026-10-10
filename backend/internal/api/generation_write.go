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

// generationCommitBoundary is a no-op in production. Subprocess crash tests
// replace it to terminate at the real storage/metadata boundary, so a route
// refactor cannot make a diagnostic green by bypassing its old test path.
var generationCommitBoundary = func(string) {}

// commitStagedGeneration publishes an already durable, create-only blob. The
// caller must hold the object's cross-replica key lock through this commit.
// Staged blobs are intentionally retained on a failed/ambiguous DB commit:
// deleting one could remove bytes that a successful commit already names.
func commitStagedGeneration(backend storage.GenerationStorage, bucket *models.Bucket, obj *models.Object) error {
	return commitStagedGenerationWith(backend, bucket, obj, nil)
}

// commitStagedGenerationWith lets a caller commit operation bookkeeping in
// the same transaction as the visible object and history pointers.
func commitStagedGenerationWith(backend storage.GenerationStorage, bucket *models.Bucket, obj *models.Object, after func(*gorm.DB) error) error {
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
		var err error
		archived, err = stageArchivedCurrent(backend, bucket, &previous)
		if err != nil {
			return err
		}
		generationCommitBoundary("archive")
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
		if after != nil {
			return after(tx)
		}
		return nil
	})
}

// stageArchivedCurrent creates a history row without changing the current
// row or its bytes. Legacy current bytes are copied into a generation first.
func stageArchivedCurrent(backend storage.GenerationStorage, bucket *models.Bucket, previous *models.Object) (*models.ObjectVersion, error) {
	oldGeneration := previous.GenerationID
	if oldGeneration == "" {
		legacyBackend, ok := backend.(storage.StorageBackend)
		if !ok {
			return nil, fmt.Errorf("generation backend cannot read legacy current object")
		}
		rc, err := getCurrentBytes(legacyBackend, bucket.Name, previous)
		if err != nil {
			return nil, fmt.Errorf("open legacy current object: %w", err)
		}
		oldGeneration = uuid.NewString()
		_, stageErr := backend.PutGeneration(bucket.Name, previous.Key, oldGeneration, rc, previous.Size, previous.ContentType, jsonPtrToMap(previous.Metadata))
		closeErr := rc.Close()
		if stageErr != nil {
			return nil, fmt.Errorf("stage legacy current object: %w", stageErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close legacy current object: %w", closeErr)
		}
	}
	versionID := previous.VersionID
	if versionID == "" {
		versionID = uuid.NewString()
	}
	return &models.ObjectVersion{
		BucketID: bucket.ID, Key: previous.Key, VersionID: versionID,
		GenerationID: oldGeneration, Size: previous.Size, ETag: previous.ETag,
		ContentType: previous.ContentType, ContentDisposition: previous.ContentDisposition,
		CacheControl: previous.CacheControl, Metadata: previous.Metadata, Tags: previous.Tags,
		VersionedAt: time.Now(), ContentModifiedAt: previous.UpdatedAt,
	}, nil
}
