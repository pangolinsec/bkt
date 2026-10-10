package validation

import (
	"crypto/md5" //nolint:gosec // MD5 is the S3 ETag algorithm (content fingerprint, not a security control)
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/gabriel-vasile/mimetype"
)

// S3 bucket naming rules: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
var (
	bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]*[a-z0-9]$`)
	ipAddressRegex  = regexp.MustCompile(`^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$`)
	regionRegex     = regexp.MustCompile(`^[a-z]{2}-[a-z]+-[0-9]{1,2}$`)
)

// ValidateBucketName validates bucket name according to S3 naming rules
func ValidateBucketName(name string) error {
	// Length check (3-63 characters)
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}

	// Must start and end with lowercase letter or number
	// Can contain lowercase letters, numbers, and hyphens
	if !bucketNameRegex.MatchString(name) {
		return fmt.Errorf("bucket name must start and end with a lowercase letter or number, and can only contain lowercase letters, numbers, and hyphens")
	}

	// Must not be formatted as an IP address
	if ipAddressRegex.MatchString(name) {
		return fmt.Errorf("bucket name must not be formatted as an IP address")
	}

	// Must not contain consecutive hyphens
	if strings.Contains(name, "--") {
		return fmt.Errorf("bucket name must not contain consecutive hyphens")
	}

	// Must not start with "xn--" (reserved for internationalized domain names)
	if strings.HasPrefix(name, "xn--") {
		return fmt.Errorf("bucket name must not start with 'xn--' prefix")
	}

	// Must not end with "-s3alias" (reserved suffix)
	if strings.HasSuffix(name, "-s3alias") {
		return fmt.Errorf("bucket name must not end with '-s3alias' suffix")
	}

	return nil
}

// MaxObjectKeyBytes is S3's object key length limit (UTF-8 bytes).
const MaxObjectKeyBytes = 1024

// ValidateObjectKey applies S3's own object key rules plus bkt's reserved
// keyspace, for every storage backend: a key is any non-empty, valid UTF-8
// string of at most 1024 bytes without NUL bytes, outside the reserved
// ".bkt-versions/" prefix. Keys are opaque: "..", a leading "/", "\", "//",
// "." segments or very long segments are ordinary characters. No storage
// backend uses a key as a filesystem path (the local backend stores objects
// under the SHA-256 of the key), so no path-safety rule belongs here — and
// code that derives something path- or header-like from a key (download file
// names, folder views) must sanitize it itself.
func ValidateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key cannot be empty")
	}
	if len(key) > MaxObjectKeyBytes {
		return fmt.Errorf("object key cannot exceed %d bytes", MaxObjectKeyBytes)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("object key must be valid UTF-8")
	}
	// NUL bytes are not representable in the metadata database (Postgres
	// text) and break many clients.
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("object key cannot contain null bytes")
	}
	if IsReservedObjectKey(key) {
		return fmt.Errorf("object keys under %q are reserved", ReservedObjectKeyPrefix)
	}
	return nil
}

// ReservedObjectKeyPrefix is the keyspace the S3 storage backend uses for
// archived object versions inside the real bucket (see storage/s3_versioning.go).
// User keys under it would read, overwrite or delete version bytes directly.
const ReservedObjectKeyPrefix = ".bkt-versions/"

// ReservedGenerationKeyPrefix is the hidden S3 storage for immutable blobs.
const ReservedGenerationKeyPrefix = ".bkt-generations/"

// IsReservedObjectKey reports whether key lies in bkt's internal keyspace.
// Read and delete paths use it (in addition to write paths via
// ValidateObjectKey) so internal data can never be addressed as an object.
func IsReservedObjectKey(key string) bool {
	return strings.HasPrefix(key, ReservedObjectKeyPrefix) || strings.HasPrefix(key, ReservedGenerationKeyPrefix)
}

// ValidateIPAddress checks if a string is a valid IP address
func ValidateIPAddress(ip string) bool {
	return net.ParseIP(ip) != nil
}

// EscapeLikeWildcards escapes special characters in LIKE patterns to prevent SQL injection
func EscapeLikeWildcards(input string) string {
	// Escape backslash first (must be first to avoid double-escaping)
	escaped := strings.ReplaceAll(input, "\\", "\\\\")
	// Escape percent sign (matches any sequence of characters)
	escaped = strings.ReplaceAll(escaped, "%", "\\%")
	// Escape underscore (matches any single character)
	escaped = strings.ReplaceAll(escaped, "_", "\\_")
	return escaped
}

// DetectContentType detects the actual content type by reading the first 3KB of the file.
// Uses the mimetype library which correctly identifies executables, archives, and media formats
// via magic bytes — more accurate than Go's built-in http.DetectContentType.
// Returns the detected MIME type and the bytes read (caller must prepend them back to the stream).
func DetectContentType(reader io.Reader) (contentType string, firstBytes []byte, err error) {
	// mimetype needs up to 3KB for reliable detection
	buffer := make([]byte, 3072)
	n, readErr := io.ReadFull(reader, buffer)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return "", nil, fmt.Errorf("failed to read file content: %w", readErr)
	}
	firstBytes = buffer[:n]
	contentType = mimetype.Detect(firstBytes).String()
	// Normalize: strip any parameters (e.g. "text/plain; charset=utf-8" → "text/plain")
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = strings.TrimSpace(contentType[:idx])
	}
	return contentType, firstBytes, nil
}

// IsSafeContentType checks if a content type is considered safe for upload.
// This function can be extended to block dangerous file types.
func IsSafeContentType(contentType string) bool {
	// Normalize content type (remove parameters like charset)
	normalized := strings.ToLower(strings.Split(contentType, ";")[0])
	normalized = strings.TrimSpace(normalized)

	// Block potentially dangerous executable types
	dangerousTypes := []string{
		"application/x-msdownload",                      // .exe (mimetype library)
		"application/x-msdos-program",                   // .com, .exe
		"application/x-executable",                      // Linux ELF executables
		"application/x-sharedlib",                       // .so shared libraries
		"application/x-mach-binary",                     // Mach-O binaries (macOS)
		"application/vnd.microsoft.portable-executable", // PE executables
		"application/x-elf",                             // ELF binaries
		"application/x-dosexec",                         // DOS executables
	}

	for _, dangerous := range dangerousTypes {
		if normalized == dangerous {
			return false
		}
	}

	return true
}

// ValidateRegion validates AWS/S3 region format
// Accepts standard AWS region format (e.g., "us-east-1", "eu-west-2")
// or allows empty string for default region
func ValidateRegion(region string) error {
	// Empty region is allowed (will use default)
	if region == "" {
		return nil
	}

	// Check if region matches standard AWS format: <continent>-<direction>-<number>
	// Examples: us-east-1, eu-west-2, ap-southeast-1
	if !regionRegex.MatchString(region) {
		return fmt.Errorf("region must match AWS format (e.g., us-east-1, eu-west-2)")
	}

	// Limit region length to prevent DoS
	if len(region) > 20 {
		return fmt.Errorf("region name too long (max 20 characters)")
	}

	return nil
}

// CalculateSHA256 calculates the SHA256 hash of the data from a reader
func CalculateSHA256(reader io.Reader) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", fmt.Errorf("failed to calculate SHA256: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// CalculateMD5 calculates the MD5 hash of the data from a reader (used for ETag)
func CalculateMD5(reader io.Reader) (string, error) {
	hash := md5.New() //nolint:gosec // MD5 is the S3 ETag algorithm (content fingerprint, not a security control)
	if _, err := io.Copy(hash, reader); err != nil {
		return "", fmt.Errorf("failed to calculate MD5: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
