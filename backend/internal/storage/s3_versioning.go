package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"

	"bkt/internal/logger"
)

// s3VersionPrefix is where bkt-managed versions live inside the real bucket.
// It is excluded from ListObjects so version bytes never appear as objects.
// Keep in sync with validation.ReservedObjectKeyPrefix (asserted by a test):
// user object keys under this prefix are rejected at every entry point.
const s3VersionPrefix = ".bkt-versions/"

// checkS3UserKey rejects keys inside the reserved version keyspace, so no
// user-facing operation can read, overwrite, or delete archived version bytes
// by addressing them as ordinary objects.
func checkS3UserKey(objectKey string) error {
	if objectKey == "" {
		return fmt.Errorf("invalid empty object key")
	}
	if strings.HasPrefix(objectKey, s3VersionPrefix) || strings.HasPrefix(objectKey, s3GenerationPrefix) {
		return fmt.Errorf("object key is under a reserved bkt prefix")
	}
	return nil
}

func s3VersionKey(objectKey, versionID string) (string, error) {
	if _, err := uuid.Parse(versionID); err != nil {
		return "", fmt.Errorf("invalid version id")
	}
	if err := checkS3UserKey(objectKey); err != nil {
		return "", err
	}
	return s3VersionPrefix + objectKey + "/" + versionID, nil
}

// s3CopySource builds an x-amz-copy-source value ("bucket/key") with the key
// percent-encoded per the SigV4 URI-encoding rules: every byte except the
// unreserved set A-Z a-z 0-9 - _ . ~ is escaped, and "/" separators are kept.
// url.PathEscape is not enough: it leaves "+" (which S3 decodes as a space),
// and escapes the bucket/key separator.
func s3CopySource(actualBucket, key string) string {
	return actualBucket + "/" + awsURIEncode(key, false)
}

// awsURIEncode percent-encodes s per the AWS SigV4 rules. When encodeSlash is
// false, "/" is left as-is.
func awsURIEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) * 3)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.', ch == '~':
			b.WriteByte(ch)
		case ch == '/' && !encodeSlash:
			b.WriteByte(ch)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[ch>>4])
			b.WriteByte(hexDigits[ch&0x0F])
		}
	}
	return b.String()
}

// serverSideMove copies srcKey to dstKey within the bucket and deletes srcKey.
// With noReplace it refuses to overwrite an existing dstKey (used when
// archiving, so an archived version is never silently replaced). The check is
// a HEAD before the copy — not atomic against other processes, but handlers
// serialize writes per key in-process, and archive destinations embed a fresh
// random version id, so a collision is practically impossible: the HEAD is
// defense in depth. When the upstream credential lacks s3:ListBucket, AWS
// answers a HEAD of a missing key with 403 instead of 404; that is treated as
// "unknown — proceed" rather than failing every archive.
func (s3s *S3Storage) serverSideMove(bucketName, srcKey, dstKey string, noReplace bool) error {
	actual := s3s.getBucketName(bucketName)
	if noReplace {
		if err := s3s.checkMoveDestinationFree(actual, dstKey); err != nil {
			return err
		}
	}
	ctx, cancel := s3LongCtx() // server-side copy: time grows with object size
	defer cancel()
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(actual),
		Key:        aws.String(dstKey),
		CopySource: aws.String(s3CopySource(actual, srcKey)),
	}
	if s3s.sse {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	_, err := s3s.client.CopyObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to copy for version move: %w", err)
	}
	dctx, dcancel := s3MetaCtx()
	defer dcancel()
	_, err = s3s.client.DeleteObject(dctx, &s3.DeleteObjectInput{
		Bucket: aws.String(actual),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		return fmt.Errorf("failed to remove source after version move: %w", err)
	}
	return nil
}

// checkMoveDestinationFree HEADs dstKey and returns an error when it exists
// (or the check failed for a reason other than "not found" / "forbidden").
func (s3s *S3Storage) checkMoveDestinationFree(actualBucket, dstKey string) error {
	ctx, cancel := s3MetaCtx()
	defer cancel()
	_, herr := s3s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(actualBucket),
		Key:    aws.String(dstKey),
	})
	return classifyMoveDestinationHead(herr, actualBucket, dstKey)
}

// classifyMoveDestinationHead interprets the no-replace HEAD's outcome.
func classifyMoveDestinationHead(herr error, actualBucket, dstKey string) error {
	if herr == nil {
		return fmt.Errorf("archived version already exists")
	}
	var nf *types.NotFound
	var nsk *types.NoSuchKey
	status := s3HTTPStatus(herr)
	if errors.As(herr, &nf) || errors.As(herr, &nsk) || status == 404 ||
		strings.Contains(herr.Error(), "NotFound") {
		return nil
	}
	if status == 403 {
		// Without s3:ListBucket, AWS reports a missing key as 403.
		logger.Debug("Archive destination HEAD returned 403 (upstream credential likely lacks s3:ListBucket); proceeding", map[string]interface{}{
			"bucket": actualBucket, "key": dstKey,
		})
		return nil
	}
	return fmt.Errorf("failed to check archived version: %w", herr)
}

func (s3s *S3Storage) ArchiveObjectVersion(bucketName, objectKey, versionID string) error {
	vk, err := s3VersionKey(objectKey, versionID)
	if err != nil {
		return err
	}
	return s3s.serverSideMove(bucketName, objectKey, vk, true)
}

func (s3s *S3Storage) PromoteObjectVersion(bucketName, objectKey, versionID string) error {
	vk, err := s3VersionKey(objectKey, versionID)
	if err != nil {
		return err
	}
	return s3s.serverSideMove(bucketName, vk, objectKey, false)
}

func (s3s *S3Storage) GetObjectVersion(bucketName, objectKey, versionID string) (io.ReadCloser, error) {
	vk, err := s3VersionKey(objectKey, versionID)
	if err != nil {
		return nil, err
	}
	// Streaming read: no total deadline (the body outlives this call).
	out, err := s3s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)),
		Key:    aws.String(vk),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	return out.Body, nil
}

func (s3s *S3Storage) DeleteObjectVersion(bucketName, objectKey, versionID string) error {
	vk, err := s3VersionKey(objectKey, versionID)
	if err != nil {
		return err
	}
	ctx, cancel := s3MetaCtx()
	defer cancel()
	_, err = s3s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s3s.getBucketName(bucketName)),
		Key:    aws.String(vk),
	})
	if err != nil {
		return fmt.Errorf("failed to delete version: %w", err)
	}
	return nil
}
