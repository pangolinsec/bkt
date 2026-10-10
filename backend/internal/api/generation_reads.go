package api

import (
	"fmt"
	"io"

	"bkt/internal/models"
	"bkt/internal/storage"
)

// Current and historical rows written before immutable generations have no
// GenerationID and continue to resolve through their existing storage slot.
func getCurrentBytes(backend storage.StorageBackend, bucket string, obj *models.Object) (io.ReadCloser, error) {
	if obj.GenerationID == "" {
		return backend.GetObject(bucket, obj.Key)
	}
	g, ok := backend.(storage.GenerationStorage)
	if !ok {
		return nil, fmt.Errorf("storage backend does not support immutable generations")
	}
	return g.GetGeneration(bucket, obj.Key, obj.GenerationID)
}

func getCurrentRange(backend storage.StorageBackend, bucket string, obj *models.Object, start, length int64) (io.ReadCloser, error) {
	if obj.GenerationID == "" {
		return storage.GetObjectRange(backend, bucket, obj.Key, start, length)
	}
	g, ok := backend.(storage.GenerationStorage)
	if !ok {
		return nil, fmt.Errorf("storage backend does not support immutable generations")
	}
	return g.GetGenerationRange(bucket, obj.Key, obj.GenerationID, start, length)
}

func getVersionBytes(backend storage.StorageBackend, bucket string, ver *models.ObjectVersion) (io.ReadCloser, error) {
	if ver.GenerationID == "" {
		return backend.GetObjectVersion(bucket, ver.Key, ver.VersionID)
	}
	g, ok := backend.(storage.GenerationStorage)
	if !ok {
		return nil, fmt.Errorf("storage backend does not support immutable generations")
	}
	return g.GetGeneration(bucket, ver.Key, ver.GenerationID)
}
