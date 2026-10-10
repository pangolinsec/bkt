package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

// The S3 generation prefix is reserved from public object requests and
// listings. It must stay in sync with validation.ReservedGenerationKeyPrefix.
const s3GenerationPrefix = ".bkt-generations/"

func s3GenerationKey(objectKey, generationID string) (string, error) {
	if err := checkS3UserKey(objectKey); err != nil {
		return "", err
	}
	if parsed, err := uuid.Parse(generationID); err != nil || parsed.String() != generationID {
		return "", fmt.Errorf("invalid generation id")
	}
	return s3GenerationPrefix + keyHash(objectKey) + "/" + generationID, nil
}

func (s3s *S3Storage) PutGeneration(bucketName, objectKey, generationID string, data io.Reader, size int64, contentType string, metadata map[string]string) (*ObjectInfo, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key),
		Body: data, ContentType: aws.String(contentType), Metadata: metadata,
		IfNoneMatch: aws.String("*"),
	}
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
	}
	if s3s.sse {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	// Streaming upload intentionally has no total timeout. The conditional
	// write guarantees an existing generation is never overwritten.
	out, err := s3s.client.PutObject(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to publish generation: %w", err)
	}
	return &ObjectInfo{Key: objectKey, Size: size, ContentType: contentType, LastModified: time.Now().UTC().Format(time.RFC3339), ETag: strings.Trim(aws.ToString(out.ETag), `"`)}, nil
}

func (s3s *S3Storage) GetGeneration(bucketName, objectKey, generationID string) (io.ReadCloser, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	out, err := s3s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("failed to get generation: %w", err)
	}
	return out.Body, nil
}

func (s3s *S3Storage) GetGenerationRange(bucketName, objectKey, generationID string, start, length int64) (io.ReadCloser, error) {
	if start < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	if length == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	out, err := s3s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get generation range: %w", err)
	}
	return limitedReadCloser{Reader: io.LimitReader(out.Body, length), Closer: out.Body}, nil
}

func (s3s *S3Storage) DeleteGeneration(bucketName, objectKey, generationID string) error {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return err
	}
	ctx, cancel := s3MetaCtx()
	defer cancel()
	_, err = s3s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("failed to delete generation: %w", err)
	}
	return nil
}
