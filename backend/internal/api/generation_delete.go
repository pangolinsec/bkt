package api

import (
	"errors"
	"fmt"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// deleteSpecificVersionWithGenerations removes the addressed row and promotes
// any surviving content in one DB transaction. Physical bytes are retained
// until a separate reader-safe garbage collector proves them unreferenced.
func deleteSpecificVersionWithGenerations(backend storage.StorageBackend, generations storage.GenerationStorage, bucket *models.Bucket, key, versionID string) error {
	var current models.Object
	currentErr := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).First(&current).Error
	if currentErr != nil && !errors.Is(currentErr, gorm.ErrRecordNotFound) {
		return currentErr
	}
	if currentErr == nil {
		curID := current.VersionID
		if curID == "" {
			curID = "null"
		}
		if versionID == curID {
			if retentionBlocks(bucket, current.UpdatedAt) {
				return &retentionError{fmt.Sprintf("object is under retention for %d days and cannot be permanently deleted yet", bucket.RetentionDays)}
			}
			latest, promoted, err := stagedNewestSurvivor(backend, generations, bucket, key, uuid.Nil)
			if err != nil {
				return err
			}
			return database.DB.Transaction(func(tx *gorm.DB) error {
				if err := tx.Delete(&models.Object{}, "id = ?", current.ID).Error; err != nil {
					return err
				}
				if promoted != nil {
					if err := tx.Create(promoted).Error; err != nil {
						return err
					}
					return tx.Delete(&models.ObjectVersion{}, "id = ?", latest.ID).Error
				}
				return nil
			})
		}
	}

	var addressed models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, key, versionID).First(&addressed).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errVersionNotFound
		}
		return err
	}
	if !addressed.IsDeleteMarker && retentionBlocks(bucket, addressed.ContentModifiedAt) {
		return &retentionError{fmt.Sprintf("version is under retention for %d days and cannot be permanently deleted yet", bucket.RetentionDays)}
	}
	var latest models.ObjectVersion
	var promoted *models.Object
	if addressed.IsDeleteMarker && currentErr != nil {
		var err error
		latest, promoted, err = stagedNewestSurvivor(backend, generations, bucket, key, addressed.ID)
		if err != nil {
			return err
		}
	}
	return database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&models.ObjectVersion{}, "id = ?", addressed.ID).Error; err != nil {
			return err
		}
		if promoted != nil {
			if err := tx.Create(promoted).Error; err != nil {
				return err
			}
			return tx.Delete(&models.ObjectVersion{}, "id = ?", latest.ID).Error
		}
		return nil
	})
}

// stagedNewestSurvivor returns the newest surviving version and, if it has
// content, a current row that already points at durable immutable bytes.
func stagedNewestSurvivor(backend storage.StorageBackend, generations storage.GenerationStorage, bucket *models.Bucket, key string, excluded uuid.UUID) (models.ObjectVersion, *models.Object, error) {
	query := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key)
	if excluded != uuid.Nil {
		query = query.Where("id <> ?", excluded)
	}
	var latest models.ObjectVersion
	if err := query.Order(versionOrder).First(&latest).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ObjectVersion{}, nil, nil
		}
		return models.ObjectVersion{}, nil, err
	}
	if latest.IsDeleteMarker {
		return latest, nil, nil
	}
	generationID := latest.GenerationID
	if generationID == "" {
		rc, err := getVersionBytes(backend, bucket.Name, &latest)
		if err != nil {
			return models.ObjectVersion{}, nil, fmt.Errorf("open legacy version before promotion: %w", err)
		}
		generationID = uuid.NewString()
		_, stageErr := generations.PutGeneration(bucket.Name, key, generationID, rc, latest.Size, latest.ContentType, jsonPtrToMap(latest.Metadata))
		closeErr := rc.Close()
		if stageErr != nil {
			return models.ObjectVersion{}, nil, fmt.Errorf("stage legacy version before promotion: %w", stageErr)
		}
		if closeErr != nil {
			return models.ObjectVersion{}, nil, fmt.Errorf("close legacy version before promotion: %w", closeErr)
		}
	}
	return latest, &models.Object{
		BucketID: bucket.ID, Key: key, Size: latest.Size, ContentType: latest.ContentType,
		ContentDisposition: latest.ContentDisposition, CacheControl: latest.CacheControl,
		ETag: latest.ETag, StoragePath: key, GenerationID: generationID,
		Metadata: latest.Metadata, Tags: latest.Tags, VersionID: latest.VersionID,
		CreatedAt: latest.ContentModifiedAt, UpdatedAt: latest.ContentModifiedAt,
	}, nil
}
