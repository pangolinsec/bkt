package storage

import (
	"context"
	"fmt"
	"io"
	"os"
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

// CreateGenerationMultipartUpload starts the upstream multipart protocol at
// the immutable physical key, never at the public object key. The generation
// ID must be persisted with the upload ID before either is returned to a client.
func (s3s *S3Storage) CreateGenerationMultipartUpload(bucketName, objectKey, generationID, contentType string, metadata map[string]string) (string, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return "", err
	}
	ctx, cancel := s3MetaCtx()
	defer cancel()
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key),
		ContentType: aws.String(contentType), Metadata: metadata,
	}
	if s3s.sse {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	out, err := s3s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to create generation multipart upload: %w", err)
	}
	return aws.ToString(out.UploadId), nil
}

func (s3s *S3Storage) UploadGenerationPart(bucketName, objectKey, generationID, uploadID string, partNumber int, data io.Reader, size int64) (string, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return "", err
	}
	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("invalid part number %d", partNumber)
	}
	// The API body is guarded and intentionally nonseekable. The AWS signer
	// needs to hash a seekable request body on HTTP S3-compatible backends.
	// Spool one already-bounded part, then sign/upload exactly those bytes.
	tmp, err := os.CreateTemp("", "bkt-s3-generation-part-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	n, err := io.Copy(tmp, data)
	if err != nil {
		return "", fmt.Errorf("failed to stage generation part: %w", err)
	}
	if size >= 0 && n != size {
		return "", fmt.Errorf("generation part size mismatch: wrote %d, expected %d", n, size)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	input := &s3.UploadPartInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key),
		UploadId: aws.String(uploadID), PartNumber: aws.Int32(int32(partNumber)), Body: tmp,
	}
	input.ContentLength = aws.Int64(n)
	// A guarded API body is not seekable. The SDK's optional automatic
	// checksum trailer cannot stream it to an HTTP S3-compatible endpoint;
	// the API already verifies the request body before its last byte escapes.
	out, err := s3s.client.UploadPart(context.Background(), input, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload generation part: %w", err)
	}
	return strings.Trim(aws.ToString(out.ETag), `"`), nil
}

func (s3s *S3Storage) CompleteGenerationMultipartUpload(bucketName, objectKey, generationID, uploadID string, parts []CompletedPart) (*ObjectInfo, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	awsParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		if part.PartNumber < 1 || part.PartNumber > 10000 {
			return nil, fmt.Errorf("invalid part number %d", part.PartNumber)
		}
		awsParts[i] = types.CompletedPart{PartNumber: aws.Int32(int32(part.PartNumber)), ETag: aws.String(`"` + strings.Trim(part.ETag, `"`) + `"`)}
	}
	ctx, cancel := s3LongCtx()
	defer cancel()
	if _, err := s3s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key), UploadId: aws.String(uploadID),
		IfNoneMatch:     aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: awsParts},
	}); err != nil {
		return nil, fmt.Errorf("failed to complete generation multipart upload: %w", err)
	}
	// Read authoritative size and ETag before publishing the DB pointer.
	metaCtx, metaCancel := s3MetaCtx()
	defer metaCancel()
	head, err := s3s.client.HeadObject(metaCtx, &s3.HeadObjectInput{Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("failed to read completed generation: %w", err)
	}
	return &ObjectInfo{Key: objectKey, Size: aws.ToInt64(head.ContentLength), ContentType: aws.ToString(head.ContentType), LastModified: time.Now().UTC().Format(time.RFC3339), ETag: strings.Trim(aws.ToString(head.ETag), `"`)}, nil
}

func (s3s *S3Storage) AbortGenerationMultipartUpload(bucketName, objectKey, generationID, uploadID string) error {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return err
	}
	ctx, cancel := s3MetaCtx()
	defer cancel()
	_, err = s3s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key), UploadId: aws.String(uploadID)})
	if err != nil {
		return fmt.Errorf("failed to abort generation multipart upload: %w", err)
	}
	return nil
}

func (s3s *S3Storage) ListGenerationParts(bucketName, objectKey, generationID, uploadID string) ([]PartInfo, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := s3MetaCtx()
	defer cancel()
	out, err := s3s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key), UploadId: aws.String(uploadID)})
	if err != nil {
		return nil, fmt.Errorf("failed to list generation parts: %w", err)
	}
	parts := make([]PartInfo, len(out.Parts))
	for i, part := range out.Parts {
		parts[i] = PartInfo{PartNumber: int(aws.ToInt32(part.PartNumber)), Size: aws.ToInt64(part.Size), ETag: strings.Trim(aws.ToString(part.ETag), `"`)}
		if part.LastModified != nil {
			parts[i].LastModified = *part.LastModified
		}
	}
	return parts, nil
}

func (s3s *S3Storage) GenerationPartSizes(bucketName, objectKey, generationID, uploadID string) (map[int]int64, error) {
	key, err := s3GenerationKey(objectKey, generationID)
	if err != nil {
		return nil, err
	}
	paginator := s3.NewListPartsPaginator(s3s.client, &s3.ListPartsInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	sizes := make(map[int]int64)
	for paginator.HasMorePages() {
		ctx, cancel := s3MetaCtx()
		page, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("failed to list generation part sizes: %w", err)
		}
		for _, part := range page.Parts {
			sizes[int(aws.ToInt32(part.PartNumber))] = aws.ToInt64(part.Size)
		}
	}
	return sizes, nil
}
