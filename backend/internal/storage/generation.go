package storage

import "io"

// GenerationStorage stores physical blobs independently of their public key's
// mutable current-object slot. The database stores generationID in an object
// row and chooses visibility with a transaction after PutGeneration succeeds.
// Implementations must never replace an existing generation.
type GenerationStorage interface {
	PutGeneration(bucketName, objectKey, generationID string, data io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectInfo, error)
	GetGeneration(bucketName, objectKey, generationID string) (io.ReadCloser, error)
	GetGenerationRange(bucketName, objectKey, generationID string, start, length int64) (io.ReadCloser, error)
	DeleteGeneration(bucketName, objectKey, generationID string) error
}
