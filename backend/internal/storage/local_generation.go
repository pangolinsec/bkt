package storage

import (
	"crypto/md5" //nolint:gosec // MD5 is the S3 ETag algorithm
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

const generationDirName = ".objgenerations"

func (ls *LocalStorage) generationLocation(bucketName, objectKey, generationID string) (string, string, error) {
	if err := checkBucketName(bucketName); err != nil {
		return "", "", err
	}
	if err := checkObjectKey(objectKey); err != nil {
		return "", "", err
	}
	if parsed, err := uuid.Parse(generationID); err != nil || parsed.String() != generationID {
		return "", "", fmt.Errorf("invalid generation id")
	}
	h := keyHash(objectKey)
	dir := filepath.Join(ls.rootPath, generationDirName, bucketName, h[:2], h[2:4], h)
	return dir, filepath.Join(dir, generationID), nil
}

// PutGeneration first syncs a private temporary file, then publishes it with
// a hard link. Link is create-only, so even a repeated UUID cannot replace
// bytes referenced by a committed database row.
func (ls *LocalStorage) PutGeneration(bucketName, objectKey, generationID string, data io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectInfo, error) {
	_ = metadata
	dir, final, err := ls.generationLocation(bucketName, objectKey, generationID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, uploadTempPrefix+"*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	hash := md5.New() //nolint:gosec // S3 ETag, not a security primitive
	n, err := io.Copy(io.MultiWriter(tmp, hash), data)
	if err != nil {
		return nil, err
	}
	if size >= 0 && n != size {
		return nil, fmt.Errorf("generation size mismatch: wrote %d, expected %d", n, size)
	}
	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Link(tmp.Name(), final); err != nil {
		return nil, fmt.Errorf("failed to publish generation: %w", err)
	}
	// A successful filesystem operation must survive a machine crash before
	// its pointer is made durable in PostgreSQL.
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil || closeErr != nil {
		_ = os.Remove(final)
		if syncErr != nil {
			return nil, syncErr
		}
		return nil, closeErr
	}
	return &ObjectInfo{Key: objectKey, Size: n, ContentType: contentType, LastModified: time.Now().UTC().Format(time.RFC3339), ETag: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (ls *LocalStorage) GetGeneration(bucketName, objectKey, generationID string) (io.ReadCloser, error) {
	_, p, err := ls.generationLocation(bucketName, objectKey, generationID)
	if err != nil {
		return nil, err
	}
	return openObjectFile(p)
}

func (ls *LocalStorage) GetGenerationRange(bucketName, objectKey, generationID string, start, length int64) (io.ReadCloser, error) {
	if start < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	rc, err := ls.GetGeneration(bucketName, objectKey, generationID)
	if err != nil {
		return nil, err
	}
	if _, err := rc.(io.Seeker).Seek(start, io.SeekStart); err != nil {
		rc.Close()
		return nil, err
	}
	return limitedReadCloser{Reader: io.LimitReader(rc, length), Closer: rc}, nil
}

func (ls *LocalStorage) DeleteGeneration(bucketName, objectKey, generationID string) error {
	_, p, err := ls.generationLocation(bucketName, objectKey, generationID)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
