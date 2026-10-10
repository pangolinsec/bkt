package storage

import (
	"crypto/md5" //nolint:gosec // MD5 is the S3 ETag algorithm (content fingerprint, not a security control)
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"bkt/internal/logger"

	"github.com/google/uuid"
)

// On-disk layout of the local backend ("blob layout", see
// docs/deployment/backup-restore.md):
//
//	<root>/.objects/<bucket>/<h[0:2]>/<h[2:4]>/<h>        current object bytes
//	<root>/.objects/<bucket>/<h[0:2]>/<h[2:4]>/<h>.key    raw object key
//	<root>/.objversions/<bucket>/<h>/<versionID>          archived version bytes
//	<root>/.objversions/<bucket>/<h>/.key                 raw object key
//	<root>/.multipart/<uploadID>/meta.json, part.NNNNN    multipart staging
//
// h is the lowercase hex SHA-256 of the raw object key bytes. Object keys are
// therefore never used as filesystem paths: every S3-valid key ("p" next to
// "p/q", "m/" next to "m", "../x", "/abs", "a\\b", "a//b", 300-byte segments,
// ...) maps to a fixed-shape path of hex characters, and no key can address
// another key's bytes, another bucket or anything outside the root.
//
// The ".key" sidecars hold the key for listings (ListObjects) and for disaster
// recovery; reads and writes never need them. A sidecar is written atomically
// before its blob is first committed, its content never changes for a given h,
// and it is removed together with the blob (see dropSidecarUnlessBlob).
//
// Bucket names cannot start with '.', so the dot-directories can never collide
// with a bucket; <root>/<bucket> and <root>/.versions are the legacy
// (key-as-path) layout, migrated once at startup (see MigrateLocalLayout).
const (
	objectsDirName     = ".objects"
	objVersionsDirName = ".objversions"
	legacyVersionsDir  = ".versions"
	multipartDirName   = ".multipart"
	keySidecarSuffix   = ".key" // object sidecar: <h>.key next to the blob
	versionSidecarName = ".key" // version sidecar: .objversions/<bucket>/<h>/.key
	layoutMarkerName   = ".layout-v2"

	uploadTempPrefix   = ".tmp-upload-"
	assembleTempPrefix = ".tmp-assemble-"
	sidecarTempPrefix  = ".tmp-key-"
)

// LocalStorage implements StorageBackend using local filesystem
type LocalStorage struct {
	rootPath string
	syncDir  func(string) error // nil uses syncLocalDirectory; tests inject failures
}

// NewLocalStorage creates a new local storage backend
func NewLocalStorage(rootPath string) *LocalStorage {
	return &LocalStorage{
		rootPath: rootPath,
	}
}

// keyHash is the blob name of an object key: lowercase hex SHA-256 of its raw
// bytes.
func keyHash(objectKey string) string {
	sum := sha256.Sum256([]byte(objectKey))
	return hex.EncodeToString(sum[:])
}

// isBlobName reports whether name has the shape of a keyHash.
func isBlobName(name string) bool {
	if len(name) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// checkBucketName is the storage layer's defense in depth for the one
// caller-supplied value that is still used as a path segment. Real bucket
// names are validated (validation.ValidateBucketName) long before they get
// here; this only guarantees a single, non-hidden path segment.
func checkBucketName(bucketName string) error {
	if bucketName == "" || strings.HasPrefix(bucketName, ".") || strings.ContainsAny(bucketName, "/\\\x00") {
		return fmt.Errorf("invalid bucket name")
	}
	return nil
}

func checkObjectKey(objectKey string) error {
	if objectKey == "" {
		return fmt.Errorf("invalid empty object key")
	}
	return nil
}

// objectLoc is where one key's current bytes live.
type objectLoc struct {
	dir     string // fan-out directory (created on demand, never pruned)
	blob    string // object bytes
	sidecar string // raw key
}

func (ls *LocalStorage) objectsBucketDir(bucketName string) (string, error) {
	if err := checkBucketName(bucketName); err != nil {
		return "", err
	}
	return filepath.Join(ls.rootPath, objectsDirName, bucketName), nil
}

func (ls *LocalStorage) objectLocation(bucketName, objectKey string) (objectLoc, error) {
	if err := checkObjectKey(objectKey); err != nil {
		return objectLoc{}, err
	}
	bucketDir, err := ls.objectsBucketDir(bucketName)
	if err != nil {
		return objectLoc{}, err
	}
	return objectLocIn(bucketDir, objectKey), nil
}

func objectLocIn(bucketDir, objectKey string) objectLoc {
	h := keyHash(objectKey)
	dir := filepath.Join(bucketDir, h[0:2], h[2:4])
	blob := filepath.Join(dir, h)
	return objectLoc{dir: dir, blob: blob, sidecar: blob + keySidecarSuffix}
}

// writeSmallFileAtomic writes content to p (temp file in dir, fsync, rename).
func writeSmallFileAtomic(dir, p, content string) error {
	tmp, err := os.CreateTemp(dir, sidecarTempPrefix+"*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(name)
		}
	}()
	if _, err := tmp.WriteString(content); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, p); err != nil {
		return err
	}
	committed = true
	return nil
}

// ensureSidecar writes the key sidecar p unless it already exists. Its content
// is a pure function of its path (h = sha256(key)), so concurrent writers are
// harmless. Errors wrap the underlying *PathError (ENOENT stays detectable for
// withDirRetry).
func ensureSidecar(dir, p, key string) error {
	if _, err := os.Lstat(p); err == nil {
		return nil
	} else if !isNotExist(err) {
		return fmt.Errorf("failed to check key sidecar: %w", err)
	}
	if err := writeSmallFileAtomic(dir, p, key); err != nil {
		return fmt.Errorf("failed to write key sidecar: %w", err)
	}
	return nil
}

// prepareObject creates the key's fan-out directory and its sidecar, ahead of
// committing bytes to loc.blob.
func prepareObject(loc objectLoc, key string) error {
	if err := os.MkdirAll(loc.dir, 0750); err != nil {
		return fmt.Errorf("failed to create object directory: %w", err)
	}
	return ensureSidecar(loc.dir, loc.sidecar, key)
}

// finishObject runs after a blob was committed (renamed into place): it
// re-asserts the sidecar, which a racing delete of the same key may have
// removed between prepareObject and the commit (see dropSidecarUnlessBlob).
func finishObject(loc objectLoc, key string) error {
	return ensureSidecar(loc.dir, loc.sidecar, key)
}

// dropSidecarUnlessBlob removes the key sidecar of a blob that is gone
// (deleted, archived, or never committed after a failed write), so deleted
// keys leave nothing behind. If a racing writer committed the blob meanwhile,
// the sidecar is restored: together with finishObject this keeps "blob exists
// ⇒ sidecar exists" under any interleaving of one writer and one remover.
func dropSidecarUnlessBlob(loc objectLoc, key string) {
	if _, err := os.Lstat(loc.blob); err == nil {
		return
	}
	if err := os.Remove(loc.sidecar); err != nil && !isNotExist(err) {
		return
	}
	if _, err := os.Lstat(loc.blob); err == nil {
		_ = ensureSidecar(loc.dir, loc.sidecar, key)
	}
}

// openObjectFile opens an object's bytes for reading ("object not found" for
// a missing blob).
func openObjectFile(p string) (*os.File, error) {
	f, err := os.Open(p) //nolint:gosec // p is <root>/.objects/<bucket>/<sha256 fan-out>, never derived from key text
	if err != nil {
		if isNotExist(err) {
			return nil, fmt.Errorf("object not found")
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

// statObject stats a blob, reporting os.ErrNotExist for a missing one.
func statObject(p string) (os.FileInfo, error) {
	info, err := os.Stat(p)
	if err != nil {
		if isNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, os.ErrNotExist
	}
	return info, nil
}

// writeAtomic streams data into a temp file in dir (which must exist) and
// renames it into place only after a successful Sync+Close, so a failed or
// interrupted write never leaves a truncated object at the live path. It
// returns the hex MD5 of the bytes written (computed in the same pass — no
// second read).
func writeAtomic(dir, finalPath string, data io.Reader) (string, error) {
	tmp, err := os.CreateTemp(dir, uploadTempPrefix+"*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	hash := md5.New() //nolint:gosec // MD5 is the S3 ETag algorithm (content fingerprint, not a security control)
	if _, err := io.Copy(io.MultiWriter(tmp, hash), data); err != nil {
		return "", fmt.Errorf("failed to write data: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("failed to flush data: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return "", fmt.Errorf("failed to commit object: %w", err)
	}
	committed = true
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// writeObject commits data as key's current bytes (temp file in the key's
// fan-out directory, fsync, rename).
func writeObject(loc objectLoc, key string, data io.Reader) error {
	if err := prepareObject(loc, key); err != nil {
		return err
	}
	if _, err := writeAtomic(loc.dir, loc.blob, data); err != nil {
		dropSidecarUnlessBlob(loc, key)
		return err
	}
	return finishObject(loc, key)
}

// CreateBucket creates the bucket's object directory.
func (ls *LocalStorage) CreateBucket(bucketName, region string) error {
	bucketDir, err := ls.objectsBucketDir(bucketName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(bucketDir, 0750); err != nil {
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}
	return nil
}

// DeleteBucket removes a bucket's objects and archived versions, including
// anything left in the legacy (pre-blob-layout) locations. It syncs each
// parent directory after removal before callers may clear durable cleanup
// metadata: otherwise a power loss could resurrect the bucket name.
func (ls *LocalStorage) DeleteBucket(bucketName string) error {
	if err := checkBucketName(bucketName); err != nil {
		return err
	}
	var firstErr error
	for _, p := range []string{
		filepath.Join(ls.rootPath, objectsDirName, bucketName),
		filepath.Join(ls.rootPath, objVersionsDirName, bucketName),
		filepath.Join(ls.rootPath, bucketName),
		filepath.Join(ls.rootPath, legacyVersionsDir, bucketName),
	} {
		if err := os.RemoveAll(p); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to delete bucket directory: %w", err)
		}
		parent := filepath.Dir(p)
		if _, err := os.Stat(parent); err != nil {
			if os.IsNotExist(err) && parent != filepath.Clean(ls.rootPath) {
				continue // this layout's parent never existed
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to stat bucket parent directory: %w", err)
			}
			continue
		}
		syncDir := ls.syncDir
		if syncDir == nil {
			syncDir = syncLocalDirectory
		}
		if err := syncDir(parent); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to sync bucket parent directory: %w", err)
		}
	}
	return firstErr
}

func syncLocalDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// BucketExists reports whether the bucket has a directory, in the blob layout
// or (for a bucket the startup migration has not converted) the legacy one.
func (ls *LocalStorage) BucketExists(bucketName string) (bool, error) {
	if err := checkBucketName(bucketName); err != nil {
		return false, err
	}
	for _, p := range []string{
		filepath.Join(ls.rootPath, objectsDirName, bucketName),
		filepath.Join(ls.rootPath, bucketName),
	} {
		info, err := os.Stat(p)
		if err == nil {
			if info.IsDir() {
				return true, nil
			}
			continue
		}
		if !isNotExist(err) {
			return false, fmt.Errorf("failed to check bucket: %w", err)
		}
	}
	return false, nil
}

// PutObject stores an object atomically.
func (ls *LocalStorage) PutObject(bucketName, objectKey string, data io.Reader, size int64, contentType string, metadata map[string]string) error {
	_ = metadata // user metadata is served from the database for the local backend
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return err
	}
	return writeObject(loc, objectKey, data)
}

// GetObject retrieves an object's bytes.
func (ls *LocalStorage) GetObject(bucketName, objectKey string) (io.ReadCloser, error) {
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return nil, err
	}
	return openObjectFile(loc.blob)
}

// DeleteObject removes an object (a missing object is not an error).
func (ls *LocalStorage) DeleteObject(bucketName, objectKey string) error {
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return err
	}
	if err := os.Remove(loc.blob); err != nil && !isNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	dropSidecarUnlessBlob(loc, objectKey)
	return nil
}

// ListObjects lists all objects in a bucket with the given prefix, sorted by
// key. Keys come from the ".key" sidecars. (The local backend's listings are
// served from the database; this is used for reconciliation.)
func (ls *LocalStorage) ListObjects(bucketName, prefix string) ([]ObjectInfo, error) {
	bucketDir, err := ls.objectsBucketDir(bucketName)
	if err != nil {
		return nil, err
	}
	objects := make([]ObjectInfo, 0)

	walkErr := filepath.WalkDir(bucketDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A file deleted by a concurrent request between readdir and
			// lstat is simply no longer part of the listing.
			if p != bucketDir && isNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || !isBlobName(d.Name()) {
			return nil // fan-out dirs, sidecars, temp files, the layout marker
		}
		info, err := d.Info()
		if err != nil {
			if isNotExist(err) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		raw, err := os.ReadFile(p + keySidecarSuffix) //nolint:gosec // path built from a walked blob name under the bucket dir
		if err != nil {
			if isNotExist(err) {
				// Deleted concurrently (blob and sidecar go together).
				if _, serr := os.Lstat(p); serr == nil {
					logger.Warn("Local object without key sidecar skipped in listing", map[string]interface{}{"bucket": bucketName, "blob": p})
				}
				return nil
			}
			return err
		}
		key := string(raw)
		if key == "" || keyHash(key) != d.Name() {
			logger.Warn("Local object key sidecar does not match its blob; skipped in listing", map[string]interface{}{"bucket": bucketName, "blob": p})
			return nil
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Use mod time + size as an ETag surrogate for listing (avoids an
		// expensive MD5 on every file). The real content MD5 is computed
		// on-demand via GetObjectInfo.
		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			ContentType:  contentTypeForKey(key),
			LastModified: info.ModTime().Format(time.RFC3339),
			ETag:         fmt.Sprintf("%x-%x", info.ModTime().Unix(), info.Size()),
		})
		return nil
	})

	if walkErr != nil {
		if isNotExist(walkErr) {
			return objects, nil // Empty list if bucket dir doesn't exist yet
		}
		return nil, fmt.Errorf("failed to list objects: %w", walkErr)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

// contentTypeForKey guesses a content type from the key's extension (the
// database holds the real one for the local backend).
func contentTypeForKey(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

// ObjectExists checks if an object exists in a bucket
func (ls *LocalStorage) ObjectExists(bucketName, objectKey string) (bool, error) {
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return false, err
	}
	if _, err := statObject(loc.blob); err != nil {
		if isNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object: %w", err)
	}
	return true, nil
}

// GetObjectInfo gets metadata about an object (ETag = content MD5).
func (ls *LocalStorage) GetObjectInfo(bucketName, objectKey string) (*ObjectInfo, error) {
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return nil, err
	}
	info, err := statObject(loc.blob)
	if err != nil {
		if isNotExist(err) {
			return nil, fmt.Errorf("object not found")
		}
		return nil, fmt.Errorf("failed to get object info: %w", err)
	}

	etag, err := calculateMD5(loc.blob)
	if err != nil {
		return nil, fmt.Errorf("failed to compute object etag: %w", err)
	}

	return &ObjectInfo{
		Key:          objectKey,
		Size:         info.Size(),
		ContentType:  contentTypeForKey(objectKey),
		LastModified: info.ModTime().Format(time.RFC3339),
		ETag:         etag,
	}, nil
}

// CopyObject copies an object within the same bucket.
func (ls *LocalStorage) CopyObject(bucketName, srcKey, dstKey string) error {
	src, err := ls.objectLocation(bucketName, srcKey)
	if err != nil {
		return err
	}
	dst, err := ls.objectLocation(bucketName, dstKey)
	if err != nil {
		return err
	}

	// Copying an object onto itself (e.g. a metadata-only REPLACE copy) is a
	// no-op for the bytes.
	if srcKey == dstKey {
		if _, err := statObject(src.blob); err != nil {
			return fmt.Errorf("source object not found")
		}
		return nil
	}

	f, err := openObjectFile(src.blob)
	if err != nil {
		if err.Error() == "object not found" {
			return fmt.Errorf("source object not found")
		}
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer f.Close() //nolint:errcheck // best-effort close of read-only file

	// Temp file + rename: the destination is only committed once the source
	// has been fully read.
	return writeObject(dst, dstKey, f)
}

// calculateMD5 calculates the MD5 hash of a file
func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath) //nolint:gosec // storage-internal path (blob, part file), never derived from key text
	if err != nil {
		return "", err
	}
	defer file.Close() //nolint:errcheck // best-effort close of read-only file

	hash := md5.New() //nolint:gosec // MD5 is the S3 ETag algorithm (content fingerprint, not a security control)
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// multipartDir returns the staging directory for a multipart upload. The
// uploadID is validated as a UUID before it is ever used to build a path, so a
// crafted value (e.g. "../..") can never escape the multipart staging area —
// critical because AbortMultipartUpload calls os.RemoveAll on this path.
func (ls *LocalStorage) multipartDir(uploadID string) (string, error) {
	if !isCanonicalUUID(uploadID) {
		return "", fmt.Errorf("invalid upload id")
	}
	return filepath.Join(ls.rootPath, multipartDirName, uploadID), nil
}

// isCanonicalUUID reports whether s is a UUID in its canonical lowercase
// 8-4-4-4-12 form — the only form bkt generates for upload and version ids.
// (uuid.Parse alone also accepts "{...}" — without checking the braces —,
// "urn:uuid:..." and upper-case spellings, which would name other files.)
func isCanonicalUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id.String() == s
}

type multipartMeta struct {
	BucketName  string `json:"bucket_name"`
	ObjectKey   string `json:"object_key"`
	ContentType string `json:"content_type"`
}

func (ls *LocalStorage) CreateMultipartUpload(bucketName, objectKey, contentType string, metadata map[string]string) (string, error) {
	_ = metadata // applied from the tracking row at complete; DB is source of truth locally
	if _, err := ls.objectLocation(bucketName, objectKey); err != nil {
		return "", err
	}
	uploadID := uuid.New().String()
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return "", fmt.Errorf("failed to create multipart dir: %w", err)
	}
	meta := multipartMeta{BucketName: bucketName, ObjectKey: objectKey, ContentType: contentType}
	data, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), data, 0600); err != nil {
		return "", fmt.Errorf("failed to write multipart meta: %w", err)
	}
	return uploadID, nil
}

func (ls *LocalStorage) UploadPart(bucketName, objectKey, uploadID string, partNumber int, data io.Reader, size int64) (string, error) {
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("multipart upload not found: %s", uploadID)
	}
	partPath := filepath.Join(dir, fmt.Sprintf("part.%05d", partNumber))
	// Atomic part write: concurrent retries of the same part number can't
	// produce a torn file with a valid-looking MD5.
	etag, err := writeAtomic(dir, partPath, data)
	if err != nil {
		return "", err
	}
	return etag, nil
}

func (ls *LocalStorage) CompleteMultipartUpload(bucketName, objectKey, uploadID string, parts []CompletedPart) error {
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("multipart upload not found: %s", uploadID)
	}
	if len(parts) == 0 {
		return fmt.Errorf("cannot complete multipart upload with no parts")
	}

	sorted := make([]CompletedPart, len(parts))
	copy(sorted, parts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PartNumber < sorted[j].PartNumber })
	// A part number listed twice would be concatenated twice, silently
	// corrupting the assembled object (AWS rejects this as InvalidPartOrder).
	for i := 1; i < len(sorted); i++ {
		if sorted[i].PartNumber == sorted[i-1].PartNumber {
			return fmt.Errorf("duplicate part number %d in complete request", sorted[i].PartNumber)
		}
	}

	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return err
	}
	if err := prepareObject(loc, objectKey); err != nil {
		return err
	}

	// Assemble into a temp file in the key's fan-out directory, then rename
	// into place: a failure part-way through never leaves a partial object.
	tmp, err := os.CreateTemp(loc.dir, assembleTempPrefix+"*")
	if err != nil {
		dropSidecarUnlessBlob(loc, objectKey)
		return fmt.Errorf("failed to create temp object: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			dropSidecarUnlessBlob(loc, objectKey)
		}
	}()

	for _, part := range sorted {
		partPath := filepath.Join(dir, fmt.Sprintf("part.%05d", part.PartNumber))
		f, err := os.Open(partPath) //nolint:gosec // dir validated by multipartDir(); part name is fixed-format
		if err != nil {
			return fmt.Errorf("failed to open part %d: %w", part.PartNumber, err)
		}
		_, copyErr := io.Copy(tmp, f)
		_ = f.Close()
		if copyErr != nil {
			return fmt.Errorf("failed to assemble part %d: %w", part.PartNumber, copyErr)
		}
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("failed to flush assembled object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close assembled object: %w", err)
	}
	if err := os.Rename(tmpName, loc.blob); err != nil {
		return fmt.Errorf("failed to commit assembled object: %w", err)
	}
	committed = true
	if err := finishObject(loc, objectKey); err != nil {
		return err
	}

	_ = os.RemoveAll(dir)
	return nil
}

func (ls *LocalStorage) AbortMultipartUpload(bucketName, objectKey, uploadID string) error {
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return err
	}
	// os.RemoveAll succeeds on a missing path; treat an unknown, well-formed
	// uploadID as a no-op success (matches S3 abort idempotency).
	return os.RemoveAll(dir)
}

func (ls *LocalStorage) ListParts(bucketName, objectKey, uploadID string) ([]PartInfo, error) {
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("multipart upload not found: %s", uploadID)
	}

	var parts []PartInfo
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "part.") {
			continue
		}
		numStr := strings.TrimPrefix(entry.Name(), "part.")
		partNum, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		partPath := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		etag, _ := calculateMD5(partPath)
		parts = append(parts, PartInfo{
			PartNumber:   partNum,
			Size:         info.Size(),
			ETag:         etag,
			LastModified: info.ModTime(),
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

// GetObjectRange implements RangeReader: it opens the object file and seeks
// to start, returning a reader limited to length bytes.
func (ls *LocalStorage) GetObjectRange(bucketName, objectKey string, start, length int64) (io.ReadCloser, error) {
	if start < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	loc, err := ls.objectLocation(bucketName, objectKey)
	if err != nil {
		return nil, err
	}
	f, err := openObjectFile(loc.blob)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	return limitedReadCloser{Reader: io.LimitReader(f, length), Closer: f}, nil
}

// PartSizes implements PartSizer by stat-ing the staged part files.
func (ls *LocalStorage) PartSizes(bucketName, objectKey, uploadID string) (map[int]int64, error) {
	dir, err := ls.multipartDir(uploadID)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("multipart upload not found: %s", uploadID)
	}
	sizes := make(map[int]int64)
	for _, entry := range entries {
		numStr, ok := strings.CutPrefix(entry.Name(), "part.")
		if !ok {
			continue
		}
		partNum, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		sizes[partNum] = info.Size()
	}
	return sizes, nil
}
