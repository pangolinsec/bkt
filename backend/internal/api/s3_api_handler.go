package api

import (
	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/middleware"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// S3APIHandler handles S3-compatible API requests
type S3APIHandler struct {
	config        *config.Config
	policyService *services.PolicyService
	bucketHandler *BucketHandler
}

func NewS3APIHandler(cfg *config.Config) *S3APIHandler {
	return &S3APIHandler{
		config:        cfg,
		policyService: services.NewPolicyService(),
		bucketHandler: NewBucketHandler(cfg),
	}
}

// S3 XML response structures
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Buckets struct {
	Bucket []BucketInfo `xml:"Bucket"`
}

type BucketInfo struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

type ListBucketResult struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Name    string   `xml:"Name"`
	Prefix  string   `xml:"Prefix"`
	// V1 fields
	Marker     string `xml:"Marker,omitempty"`
	NextMarker string `xml:"NextMarker,omitempty"`
	// V2 fields (populated only when list-type=2)
	KeyCount              int    `xml:"KeyCount,omitempty"`
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	StartAfter            string `xml:"StartAfter,omitempty"`

	Delimiter      string         `xml:"Delimiter,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []ObjectInfo   `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

type ObjectInfo struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
	Owner        Owner     `xml:"Owner"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

// DeleteRequest is the XML body for POST /?delete
type DeleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Quiet   bool           `xml:"Quiet"`
	Objects []DeleteObject `xml:"Object"`
}

type DeleteObject struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId"` //nolint:revive // S3 XML element name
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []DeletedObject `xml:"Deleted"`
	Errors  []DeleteError   `xml:"Error"`
}

type DeletedObject struct {
	Key                   string `xml:"Key"`
	VersionId             string `xml:"VersionId,omitempty"` //nolint:revive // S3 XML element name
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId string `xml:"DeleteMarkerVersionId,omitempty"` //nolint:revive // S3 XML element name
}

type DeleteError struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"` //nolint:revive // S3 XML element name
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

// ListBuckets handles GET / (list all buckets)
func (h *S3APIHandler) ListBuckets(c *gin.Context) {
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}
	isAdmin := c.GetBool("is_admin")
	userVal, _ := c.Get("user")
	userModel, isUser := userVal.(*models.User)
	if !isUser || userModel == nil {
		h.s3Error(c, "AccessDenied", "Access Denied", "", http.StatusForbidden)
		return
	}

	var allBuckets []models.Bucket
	if err := database.DB.Preload("Owner").Find(&allBuckets).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to list buckets", "", http.StatusInternalServerError)
		return
	}

	// Use batch permission check to avoid N+1 queries (fixes CRITICAL performance issue)
	var accessibleBuckets []models.Bucket
	if isAdmin {
		// Admin bypass - return all buckets
		accessibleBuckets = allBuckets
	} else {
		// Batch check which buckets user can list
		var err error
		accessibleBuckets, err = h.policyService.FilterAccessibleBuckets(userUUID, allBuckets, services.ActionListBucket)
		if err != nil {
			h.s3Error(c, "InternalError", "Failed to check bucket permissions", "", http.StatusInternalServerError)
			return
		}
	}

	// Build XML response

	bucketInfos := make([]BucketInfo, len(accessibleBuckets))
	for i, bucket := range accessibleBuckets {
		bucketInfos[i] = BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreatedAt,
		}
	}

	response := ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: Owner{
			ID:          userModel.ID.String(),
			DisplayName: userModel.Username,
		},
		Buckets: Buckets{
			Bucket: bucketInfos,
		},
	}

	c.XML(http.StatusOK, response)
}

// LocationConstraintResult is the response body for GET /{bucket}?location.
type LocationConstraintResult struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

// VersioningConfigurationResult is the response body for GET /{bucket}?versioning.
// An empty Status means versioning has never been enabled.
type VersioningConfigurationResult struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// bucketRegion returns the region reported for buckets. bkt is single-region;
// this reflects the configured S3 region (default us-east-1).
func (h *S3APIHandler) bucketRegion() string {
	if h.config != nil && h.config.Storage.S3.Region != "" {
		return h.config.Storage.S3.Region
	}
	return "us-east-1"
}

// listObjects answers ListObjects (V1) / ListObjectsV2 (list-type=2) for a
// bucket the caller was already authorized to list (see ListObjects in
// s3_bucket_subresources.go, which routes GET /{bucket}).
func (h *S3APIHandler) listObjects(c *gin.Context, bucket *models.Bucket) {
	bucketName := bucket.Name
	isV2 := c.Query("list-type") == "2"
	prefix := c.DefaultQuery("prefix", "")
	delimiter := c.Query("delimiter")

	maxKeys := 1000
	if mk := c.Query("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed > 0 && parsed <= 1000 {
			maxKeys = parsed
		}
	}

	// Determine start-after key for pagination
	startAfterKey := ""
	continuationToken := ""
	if isV2 {
		continuationToken = c.Query("continuation-token")
		if continuationToken != "" {
			// continuation-token is base64(lastKey) from the previous page
			if decoded, err := base64.StdEncoding.DecodeString(continuationToken); err == nil {
				startAfterKey = string(decoded)
			}
		} else {
			startAfterKey = c.Query("start-after")
		}
	} else {
		startAfterKey = c.Query("marker")
	}

	// Query maxKeys+1 so we can detect truncation
	query := database.DB.Where("bucket_id = ?", bucket.ID)
	if prefix != "" {
		escapedPrefix := validation.EscapeLikeWildcards(prefix)
		query = query.Where("key LIKE ?", escapedPrefix+"%")
	}
	if startAfterKey != "" {
		query = query.Where("key > ?", startAfterKey)
	}

	var objects []models.Object
	if err := query.Limit(maxKeys + 1).Order("key ASC").Find(&objects).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to list objects", bucketName, http.StatusInternalServerError)
		return
	}

	isTruncated := len(objects) > maxKeys
	if isTruncated {
		objects = objects[:maxKeys]
	}

	// Build contents and common prefixes
	contents := make([]ObjectInfo, 0, len(objects))
	commonPrefixes := make(map[string]bool)

	for _, obj := range objects {
		if delimiter != "" {
			keyAfterPrefix := strings.TrimPrefix(obj.Key, prefix)
			if idx := strings.Index(keyAfterPrefix, delimiter); idx >= 0 {
				commonPrefixes[prefix+keyAfterPrefix[:idx+1]] = true
				continue
			}
		}
		// Every object is listed — including "dir/.keep" placeholders older
		// console versions created. Hiding them made such folders impossible
		// to remove with S3 tools (`aws s3 rm --recursive` never saw the
		// placeholder, so the console kept showing the folder).
		contents = append(contents, ObjectInfo{
			Key:          obj.Key,
			LastModified: obj.UpdatedAt,
			ETag:         `"` + obj.ETag + `"`,
			Size:         obj.Size,
			StorageClass: "STANDARD",
			Owner: Owner{
				ID:          bucket.OwnerID.String(),
				DisplayName: bucket.Owner.Username,
			},
		})
	}

	commonPrefixList := make([]CommonPrefix, 0, len(commonPrefixes))
	for p := range commonPrefixes {
		commonPrefixList = append(commonPrefixList, CommonPrefix{Prefix: p})
	}

	response := ListBucketResult{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucketName,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixList,
	}

	if isV2 {
		response.ContinuationToken = continuationToken
		response.StartAfter = c.Query("start-after")
		response.KeyCount = len(contents)
		if isTruncated && len(objects) > 0 {
			response.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(objects[len(objects)-1].Key))
		}
	} else {
		response.Marker = startAfterKey
		if isTruncated && len(objects) > 0 {
			response.NextMarker = objects[len(objects)-1].Key
		}
	}

	c.XML(http.StatusOK, response)
}

// GetObject handles GET /{bucket}/{key+} (download object or ListParts if ?uploadId present)
func (h *S3APIHandler) GetObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := c.Param("key")

	// Trim leading slash (Gin's * wildcard includes it)
	objectKey = strings.TrimPrefix(objectKey, "/")

	// If key is empty, this is a bucket-level request (listing or sub-resource)
	if objectKey == "" {
		h.ListObjects(c)
		return
	}

	// Sub-resources: ListParts (?uploadId), ?tagging, ?acl; anything else
	// but versionId / partNumber / response-* is 501 (never content).
	if h.dispatchObjectGet(c) {
		return
	}
	part, ok := h.parsePartNumber(c)
	if !ok {
		return
	}

	// bkt's internal version keyspace is never addressable as an object.
	if validation.IsReservedObjectKey(objectKey) {
		h.s3Error(c, "NoSuchKey", "The specified key does not exist", objectKey, http.StatusNotFound)
		return
	}

	// Anonymous public reads (see middleware.S3AuthMiddleware) have no user;
	// everyone else must be an authenticated caller.
	anonymous := c.GetBool(middleware.CtxS3Anonymous)
	var userUUID uuid.UUID
	if !anonymous {
		var authed bool
		if userUUID, authed = h.s3Caller(c); !authed {
			return
		}
	}

	// Get bucket
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}

	// Check permissions
	var allowed bool
	if anonymous {
		allowed = h.anonymousReadAllowed(c, &bucket, objectKey)
	} else {
		allowed = h.signedReadAllowed(c, userUUID, bucketName, objectKey)
	}
	if !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return
	}

	// A non-current version addressed explicitly?
	if vid := c.Query("versionId"); vid != "" {
		var cur models.Object
		curErr := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&cur).Error
		curVID := cur.VersionID
		if curVID == "" {
			curVID = "null"
		}
		if curErr != nil || vid != curVID {
			h.serveObjectVersion(c, &bucket, objectKey, vid, false)
			return
		}
		// Addressed version IS the current one — fall through to the normal path.
	}

	// Get object metadata
	var object models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error; err != nil {
		h.s3Error(c, "NoSuchKey", "The specified key does not exist", objectKey, http.StatusNotFound)
		return
	}
	if object.VersionID != "" {
		c.Header("x-amz-version-id", object.VersionID)
	}

	// Get storage backend
	storageBackend, err := h.bucketHandler.getStorageBackend(&bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	// Common S3-compatible headers
	c.Header("Content-Type", object.ContentType)
	c.Header("ETag", fmt.Sprintf(`"%s"`, object.ETag))
	c.Header("Last-Modified", object.UpdatedAt.UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	c.Header("x-amz-request-id", uuid.New().String())
	writeUserMetadataHeaders(c, &object)
	writeStoredContentHeaders(c, object.ContentDisposition, object.CacheControl)

	// Honor a Range request if present. We advertise Accept-Ranges, so clients
	// (notably the AWS CLI/SDK, which split downloads >8MB into byte-range GETs)
	// expect 206 Partial Content. Without this they receive the full object for
	// every range and concatenate the copies into a corrupt, oversized file.
	rangeHeader := c.GetHeader("Range")
	if part == 1 && object.Size > 0 {
		// partNumber=1 of a single-part object: the whole object, answered
		// like a ranged read (206 + Content-Range), as S3 does.
		rangeHeader = "bytes=0-"
	}
	if start, length, ok, satisfiable := parseRange(rangeHeader, object.Size); ok {
		if !satisfiable {
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", object.Size))
			h.s3Error(c, "InvalidRange", "The requested range is not satisfiable", objectKey, http.StatusRequestedRangeNotSatisfiable)
			return
		}
		// Native ranged read: an S3 Range GET / a file seek, rather than
		// streaming and discarding everything before the start offset.
		rangeReader, err := storage.GetObjectRange(storageBackend, bucketName, objectKey, start, length)
		if err != nil {
			h.s3Error(c, "InternalError", "Failed to read object range", objectKey, http.StatusInternalServerError)
			return
		}
		defer rangeReader.Close() //nolint:errcheck // best-effort close of read stream
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, object.Size))
		contentType := applyResponseOverrides(c, object.ContentType)
		c.DataFromReader(http.StatusPartialContent, length, contentType, rangeReader, nil)
		return
	}

	// Get object from storage
	file, err := storageBackend.GetObject(bucketName, objectKey)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to retrieve object", objectKey, http.StatusInternalServerError)
		return
	}
	defer file.Close() //nolint:errcheck // best-effort close of read stream

	// Stream the full object — cap at object.Size so we never emit more bytes
	// than Content-Length declares (multipart-assembled files on disk may carry
	// trailing bytes; without the limit the client receives a corrupt, oversized body).
	c.Header("Content-Length", strconv.FormatInt(object.Size, 10))
	contentType := applyResponseOverrides(c, object.ContentType)
	c.DataFromReader(http.StatusOK, object.Size, contentType, io.LimitReader(file, object.Size), nil)
}

// anonymousReadAllowed decides an anonymous (unsigned) read of the CURRENT
// version of an object. The bucket's is_public flag is the grant — user and
// bucket-policy Allows are irrelevant — but an explicit bucket-policy Deny
// for Principal "*" (or no Principal) still applies. The middleware already
// admitted only object GET/HEADs with response-* parameters; this re-checks
// against the freshly loaded bucket (defense in depth).
func (h *S3APIHandler) anonymousReadAllowed(c *gin.Context, bucket *models.Bucket, objectKey string) bool {
	if !bucket.IsPublic || objectKey == "" || !middleware.PublicReadQueryAllowed(c.Request.URL.RawQuery) {
		return false
	}
	return !h.policyService.AnonymousObjectAccessDenied(bucket, objectKey, services.ActionGetObject)
}

// signedReadAllowed authorizes a signed object GET/HEAD as s3:GetObject. On a
// public-read bucket CheckObjectAccess allows it unless a policy explicitly
// denies it; public-read covers only the current version, so a
// versionId-addressed read (AWS s3:GetObjectVersion) needs a policy grant.
func (h *S3APIHandler) signedReadAllowed(c *gin.Context, userUUID uuid.UUID, bucketName, objectKey string) bool {
	check := h.policyService.CheckObjectAccess
	if c.Query("versionId") != "" {
		check = h.policyService.CheckObjectAccessWithoutPublicRead
	}
	allowed, _ := check(userUUID, bucketName, objectKey, services.ActionGetObject)
	return allowed
}

// responseHeaderOverrides maps the GetObject response-* query parameters to
// the response header each one overrides (S3 semantics).
var responseHeaderOverrides = []struct{ param, header string }{
	{"response-content-type", "Content-Type"},
	{"response-content-disposition", "Content-Disposition"},
	{"response-cache-control", "Cache-Control"},
	{"response-expires", "Expires"},
	{"response-content-language", "Content-Language"},
	{"response-content-encoding", "Content-Encoding"},
}

// applyResponseOverrides sets the headers requested via response-* query
// parameters on a successful GetObject and returns the Content-Type to send.
// Control characters are dropped so a value cannot split the header.
// S3ObjectResponseHeaders still inspects the final headers, so an override
// cannot make active content render inline on an anonymous request.
func applyResponseOverrides(c *gin.Context, contentType string) string {
	for _, o := range responseHeaderOverrides {
		v := strings.Map(func(r rune) rune {
			if r < 0x20 || r == 0x7f {
				return -1
			}
			return r
		}, c.Query(o.param))
		if v == "" {
			continue
		}
		if o.header == "Content-Type" {
			contentType = v
		}
		c.Header(o.header, v)
	}
	return contentType
}

// parseRange parses a single-range HTTP Range header of the form
// "bytes=start-end", "bytes=start-", or "bytes=-suffixLength" against an object
// of the given size. It returns the absolute start offset and the number of
// bytes to send. ok is false when there is no (or a multi-range/unparseable)
// header, in which case the caller streams the full object. satisfiable is
// false when the range falls entirely outside the object (→ 416).
func parseRange(header string, size int64) (start, length int64, ok, satisfiable bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return 0, 0, false, false
	}
	spec := strings.TrimPrefix(header, prefix)
	// Multi-range requests (comma-separated) are not supported; fall back to full.
	if strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	startStr, endStr := spec[:dash], spec[dash+1:]

	if startStr == "" {
		// Suffix range: last N bytes.
		suffix, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, suffix, true, size > 0
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, false
	}
	if start >= size {
		return 0, 0, true, false // valid syntax, unsatisfiable
	}
	end := size - 1
	if endStr != "" {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < start {
			return 0, 0, false, false
		}
		if end > size-1 {
			end = size - 1
		}
	}
	return start, end - start + 1, true, true
}

// etagConditionMatches applies an HTTP entity-tag condition to the current
// object. S3 ETags are strong quoted tags; a weak tag cannot authorize a write.
func etagConditionMatches(condition, etag string, exists bool) bool {
	if !exists {
		return false
	}
	for _, candidate := range strings.Split(condition, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == `"`+etag+`"` {
			return true
		}
	}
	return false
}

// checkWritePreconditions must run while the caller holds the key's write
// lock, before archiving or replacing bytes. The returned bool is whether a
// current object exists; ok is false after an S3 error has been written.
func (h *S3APIHandler) checkWritePreconditions(c *gin.Context, bucket *models.Bucket, key string) (exists, ok bool) {
	var current models.Object
	err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).First(&current).Error
	exists = err == nil
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		h.s3Error(c, "InternalError", "Failed to check current object", key, http.StatusInternalServerError)
		return false, false
	}
	if condition := c.GetHeader("If-Match"); condition != "" && !etagConditionMatches(condition, current.ETag, exists) {
		h.s3Error(c, "PreconditionFailed", "If-Match precondition failed", key, http.StatusPreconditionFailed)
		return false, false
	}
	if condition := c.GetHeader("If-None-Match"); condition != "" && etagConditionMatches(condition, current.ETag, exists) {
		h.s3Error(c, "PreconditionFailed", "If-None-Match precondition failed", key, http.StatusPreconditionFailed)
		return false, false
	}
	return exists, true
}

// checkCopySourcePreconditions evaluates the source's copy-specific headers
// against the metadata loaded under the source key lock. S3 gives a matching
// source ETag priority over an older If-Unmodified-Since date.
func (h *S3APIHandler) checkCopySourcePreconditions(c *gin.Context, source *models.Object, key string) bool {
	if condition := c.GetHeader("x-amz-copy-source-if-match"); condition != "" &&
		!etagConditionMatches(condition, source.ETag, true) {
		h.s3Error(c, "PreconditionFailed", "Source If-Match precondition failed", key, http.StatusPreconditionFailed)
		return false
	}
	if condition := c.GetHeader("x-amz-copy-source-if-none-match"); condition != "" &&
		etagConditionMatches(condition, source.ETag, true) {
		h.s3Error(c, "PreconditionFailed", "Source If-None-Match precondition failed", key, http.StatusPreconditionFailed)
		return false
	}
	modifiedAt := source.UpdatedAt.UTC().Truncate(time.Second)
	for _, item := range []struct {
		header string
		match  bool
	}{
		{"x-amz-copy-source-if-unmodified-since", false},
		{"x-amz-copy-source-if-modified-since", true},
	} {
		value := c.GetHeader(item.header)
		if value == "" {
			continue
		}
		cutoff, err := http.ParseTime(value)
		if err != nil {
			h.s3Error(c, "InvalidArgument", "Invalid source condition date", key, http.StatusBadRequest)
			return false
		}
		if !item.match && c.GetHeader("x-amz-copy-source-if-match") != "" {
			// A matching source ETag wins over If-Unmodified-Since.
			continue
		}
		valid := !modifiedAt.After(cutoff)
		if item.match {
			valid = modifiedAt.After(cutoff)
		}
		if !valid {
			h.s3Error(c, "PreconditionFailed", "Source date precondition failed", key, http.StatusPreconditionFailed)
			return false
		}
	}
	return true
}

// PutObject handles PUT /{bucket}/{key+} (upload, copy, or multipart part)
func (h *S3APIHandler) PutObject(c *gin.Context) {
	// Unsupported sub-resources (?acl, ?retention, ?legal-hold, ...) and
	// half-specified UploadPart requests are refused before anything is written.
	if h.dispatchObjectPutPreflight(c) {
		return
	}
	if copySource := c.GetHeader("x-amz-copy-source"); copySource != "" {
		// With uploadId+partNumber this is UploadPartCopy, not CopyObject.
		if c.Query("uploadId") != "" && c.Query("partNumber") != "" {
			h.UploadPartCopy(c)
			return
		}
		h.CopyObject(c, copySource)
		return
	}

	// Dispatch to UploadPart for multipart uploads
	if c.Query("uploadId") != "" && c.Query("partNumber") != "" {
		h.UploadPart(c)
		return
	}

	// Tagging subresource
	if _, ok := c.GetQuery("tagging"); ok {
		h.PutObjectTagging(c)
		return
	}

	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	// S3 key rules (length, UTF-8, NUL) and the reserved version keyspace.
	if err := validation.ValidateObjectKey(objectKey); err != nil {
		h.s3Error(c, "InvalidArgument", err.Error(), objectKey, http.StatusBadRequest)
		return
	}

	// Get bucket
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}

	// Check permissions
	allowed, _ := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionPutObject)
	if !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return
	}

	// User metadata (x-amz-meta-*) and tags (x-amz-tagging). A PUT replaces
	// both wholesale, matching S3 semantics.
	userMeta, err := extractUserMetadata(c)
	if err != nil {
		h.s3Error(c, "MetadataTooLarge", err.Error(), objectKey, http.StatusBadRequest)
		return
	}
	objTags, err := parseTaggingHeader(c.GetHeader("x-amz-tagging"))
	if err != nil {
		h.s3Error(c, "InvalidTag", err.Error(), objectKey, http.StatusBadRequest)
		return
	}

	// Decode the body (aws-chunked with chunk-signature verification when
	// streaming) and determine its authoritative decoded length. For
	// aws-chunked bodies that is X-Amz-Decoded-Content-Length, never the
	// encoded Content-Length.
	upload, berr := prepareUploadBody(c, h.config.Storage.MaxFileSize)
	if berr != nil {
		h.s3Error(c, berr.code, berr.msg, objectKey, berr.status)
		return
	}
	contentLength := upload.declared
	bodyReader := upload.reader

	var contentType string
	var combinedReader io.Reader

	if h.config.Storage.EnforceContentTypeDetection {
		// Opt-in strict mode: detect the type from magic bytes and reject
		// "unsafe" types. Off by default — see StorageConfig.
		detectedType, firstBytes, derr := validation.DetectContentType(bodyReader)
		if derr != nil {
			// A body that fails verification while being sniffed (bad chunk
			// signature, truncated stream, payload-hash mismatch) is a client
			// error, not an internal one.
			code, msg, status, _ := bodyFailure(&guardedBody{err: derr})
			h.s3Error(c, code, msg, objectKey, status)
			return
		}
		if !validation.IsSafeContentType(detectedType) {
			h.s3Error(c, "InvalidRequest", fmt.Sprintf("File type '%s' is not allowed", detectedType), objectKey, http.StatusBadRequest)
			return
		}
		contentType = detectedType
		combinedReader = io.MultiReader(bytes.NewReader(firstBytes), bodyReader)
	} else {
		// Default S3 behavior: the client's Content-Type is authoritative
		// metadata. Fall back to a sensible default only when it's absent.
		contentType = c.GetHeader("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		combinedReader = bodyReader
	}

	// Get storage backend
	storageBackend, err := h.bucketHandler.getStorageBackend(&bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	// Serialize with other writers of this key: archive → write → metadata
	// must not interleave with a concurrent overwrite or delete.
	unlock, lockErr := lockObjectKeysChecked(bucketName, objectKey)
	if lockErr != nil {
		h.s3Error(c, "InternalError", "Failed to lock object", objectKey, http.StatusInternalServerError)
		return
	}
	defer unlock()
	hadPrior, ok := h.checkWritePreconditions(c, &bucket, objectKey)
	if !ok {
		return
	}

	// Quota: reserve the declared size before any bytes are written; the
	// reservation also tracks the bytes actually read (see guardedBody).
	reservation, qerr := reserveBucketQuota(&bucket, contentLength)
	if qerr != nil {
		h.s3Error(c, "QuotaExceeded", qerr.Error(), objectKey, http.StatusForbidden)
		return
	}
	defer reservation.release()

	// The guarded body enforces the exact declared length, the size cap and
	// the quota, and withholds the final byte until the whole stream (incl.
	// payload hash / chunk signatures) verified — so the backend can never
	// commit an unverified or truncated body.
	body := newGuardedBody(combinedReader, contentLength, h.config.Storage.MaxFileSize, reservation)

	// Versioning: archive the current version before overwriting.
	archivedVID, verr := prepareVersionedWrite(storageBackend, &bucket, objectKey)
	if verr != nil {
		h.s3Error(c, "InternalError", "Failed to version existing object", objectKey, http.StatusInternalServerError)
		return
	}

	err = storageBackend.PutObject(bucketName, objectKey, body, contentLength, contentType, userMeta)
	if err != nil || !body.Complete() {
		if err == nil {
			discardFailedWrite(storageBackend, &bucket, objectKey, archivedVID, hadPrior)
			h.s3Error(c, "InternalError", "Failed to save object", objectKey, http.StatusInternalServerError)
			return
		}
		rollbackVersionedWrite(storageBackend, &bucket, objectKey, archivedVID)
		if code, msg, status, ok := bodyFailure(body); ok {
			h.s3Error(c, code, msg, objectKey, status)
			return
		}
		h.s3Error(c, "InternalError", "Failed to save object", objectKey, http.StatusInternalServerError)
		return
	}

	// Get object info (including ETag)
	objectInfo, err := storageBackend.GetObjectInfo(bucketName, objectKey)
	if err != nil {
		discardFailedWrite(storageBackend, &bucket, objectKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to get object info", objectKey, http.StatusInternalServerError)
		return
	}

	// Record the new current version atomically (upsert).
	object := models.Object{
		BucketID:           bucket.ID,
		Key:                objectKey,
		Size:               objectInfo.Size,
		ContentType:        contentType,
		ContentDisposition: c.GetHeader("Content-Disposition"),
		CacheControl:       c.GetHeader("Cache-Control"),
		ETag:               objectInfo.ETag,
		StoragePath:        objectKey,
		Metadata:           mapToJSONPtr(userMeta),
		Tags:               mapToJSONPtr(objTags),
		VersionID:          newCurrentVersionID(&bucket),
	}
	if err := upsertCurrentObject(&object); err != nil {
		discardFailedWrite(storageBackend, &bucket, objectKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to save object metadata", objectKey, http.StatusInternalServerError)
		return
	}

	notifyObjectEvent(&bucket, services.EventObjectCreated, objectKey, object.Size, object.ETag, object.VersionID)

	// Return success with ETag
	c.Header("ETag", fmt.Sprintf(`"%s"`, object.ETag))
	if object.VersionID != "" {
		c.Header("x-amz-version-id", object.VersionID)
	}
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// DeleteObject handles DELETE /{bucket}/{key+} (delete object or abort multipart)
func (h *S3APIHandler) DeleteObject(c *gin.Context) {
	// Bucket-level DELETEs (empty key) are routed by HandleBucketDelete.
	if strings.TrimPrefix(c.Param("key"), "/") == "" {
		h.HandleBucketDelete(c)
		return
	}

	// AbortMultipartUpload (?uploadId), ?tagging; anything else but
	// ?versionId is 501 — never a delete.
	if h.dispatchObjectDelete(c) {
		return
	}

	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	// Internal version keyspace: behaves like a key that does not exist.
	if validation.IsReservedObjectKey(objectKey) {
		c.Status(http.StatusNoContent)
		return
	}

	// Get bucket
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		// S3 returns 204 even if bucket doesn't exist
		c.Status(http.StatusNoContent)
		return
	}

	// Check permissions
	allowed, _ := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionDeleteObject)
	if !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return
	}

	// Get storage backend
	storageBackend, err := h.bucketHandler.getStorageBackend(&bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to get storage backend", objectKey, http.StatusInternalServerError)
		return
	}

	// Serialize with other writers of this key.
	unlock, lockErr := lockObjectKeysChecked(bucketName, objectKey)
	if lockErr != nil {
		h.s3Error(c, "InternalError", "Failed to lock object", objectKey, http.StatusInternalServerError)
		return
	}
	defer unlock()

	// Version-addressed delete: permanently removes that one version.
	if vid := c.Query("versionId"); vid != "" {
		wasMarker, err := deleteVersionLocked(storageBackend, &bucket, objectKey, vid)
		if err != nil {
			if errors.Is(err, errUnderRetention) {
				h.s3Error(c, "AccessDenied", err.Error(), objectKey, http.StatusForbidden)
				return
			}
			h.s3Error(c, "InternalError", "Failed to delete version", objectKey, http.StatusInternalServerError)
			return
		}
		if wasMarker {
			c.Header("x-amz-delete-marker", "true")
		}
		c.Header("x-amz-version-id", vid)
		c.Header("x-amz-request-id", uuid.New().String())
		c.Status(http.StatusNoContent)
		return
	}

	// Get object metadata
	var object models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error; err != nil {
		// S3 returns 204 even if object doesn't exist
		c.Status(http.StatusNoContent)
		return
	}

	// Versioned delete: archive bytes + record a delete marker.
	if markerID, handled, derr := versionedDeleteCurrent(storageBackend, &bucket, &object); handled {
		if derr != nil {
			h.s3Error(c, "InternalError", "Failed to delete object", objectKey, http.StatusInternalServerError)
			return
		}
		notifyObjectEvent(&bucket, services.EventObjectRemoved, objectKey, 0, "", markerID)
		c.Header("x-amz-delete-marker", "true")
		c.Header("x-amz-version-id", markerID)
		c.Header("x-amz-request-id", uuid.New().String())
		c.Status(http.StatusNoContent)
		return
	}

	// WORM: an unversioned delete is permanent, so retention forbids it.
	if retentionBlocks(&bucket, object.UpdatedAt) {
		h.s3Error(c, "AccessDenied", fmt.Sprintf("Object is under retention for %d days and cannot be deleted yet", bucket.RetentionDays), objectKey, http.StatusForbidden)
		return
	}

	// Delete from storage first - MUST succeed before database delete (prevents inconsistency)
	if err := storageBackend.DeleteObject(bucketName, objectKey); err != nil {
		h.s3Error(c, "InternalError", "Failed to delete object from storage", objectKey, http.StatusInternalServerError)
		return
	}

	// Delete from database only after storage delete succeeds
	if err := database.DB.Delete(&object).Error; err != nil {
		// Critical: storage deleted but database failed - log this for manual cleanup
		h.s3Error(c, "InternalError", "Failed to delete object metadata", objectKey, http.StatusInternalServerError)
		return
	}
	notifyObjectEvent(&bucket, services.EventObjectRemoved, objectKey, 0, "", "")

	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusNoContent)
}

// HeadObject handles HEAD /{bucket}/{key+} (get object metadata)
func (h *S3APIHandler) HeadObject(c *gin.Context) {
	if h.dispatchObjectHead(c) {
		return
	}
	if _, ok := h.parsePartNumber(c); !ok {
		return
	}
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	anonymous := c.GetBool(middleware.CtxS3Anonymous)
	var userUUID uuid.UUID
	if !anonymous {
		if id, ok := c.Get("user_id"); ok {
			userUUID, _ = id.(uuid.UUID)
		}
		if userUUID == uuid.Nil {
			c.Status(http.StatusForbidden)
			return
		}
	}

	if validation.IsReservedObjectKey(objectKey) {
		c.Status(http.StatusNotFound)
		return
	}

	// Get bucket
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Check permissions
	var allowed bool
	if anonymous {
		allowed = h.anonymousReadAllowed(c, &bucket, objectKey)
	} else {
		allowed = h.signedReadAllowed(c, userUUID, bucketName, objectKey)
	}
	if !allowed {
		c.Status(http.StatusForbidden)
		return
	}

	// Get object metadata
	var object models.Object
	err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error

	// A non-current version addressed explicitly?
	if vid := c.Query("versionId"); vid != "" {
		curVID := object.VersionID
		if curVID == "" {
			curVID = "null"
		}
		if err != nil || vid != curVID {
			h.serveObjectVersion(c, &bucket, objectKey, vid, true)
			return
		}
	}
	if err == nil && object.VersionID != "" {
		c.Header("x-amz-version-id", object.VersionID)
	}

	// If exact match not found and key ends with /, it might be a folder
	// Check if any objects exist with this prefix. Not for anonymous reads:
	// they address objects only and must not probe for prefixes.
	if err != nil && !anonymous && strings.HasSuffix(objectKey, "/") {
		var count int64
		database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key LIKE ?", bucket.ID, validation.EscapeLikeWildcards(objectKey)+"%").Count(&count)
		if count > 0 {
			// It's a folder - return folder-like metadata
			c.Header("Content-Type", "application/x-directory")
			c.Header("Content-Length", "0")
			c.Header("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			c.Header("x-amz-request-id", uuid.New().String())
			c.Status(http.StatusOK)
			return
		}
	}

	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Set headers for regular object
	c.Header("Content-Type", object.ContentType)
	c.Header("Content-Length", strconv.FormatInt(object.Size, 10))
	c.Header("ETag", fmt.Sprintf(`"%s"`, object.ETag))
	c.Header("Last-Modified", object.UpdatedAt.UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	c.Header("x-amz-request-id", uuid.New().String())
	writeUserMetadataHeaders(c, &object)
	writeStoredContentHeaders(c, object.ContentDisposition, object.CacheControl)

	c.Status(http.StatusOK)
}

// HeadBucket handles HEAD /{bucket} (check if bucket exists)
func (h *S3APIHandler) HeadBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	// Get bucket
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Check permissions
	allowed, _ := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket)
	if !allowed {
		c.Status(http.StatusForbidden)
		return
	}

	// SDKs use x-amz-bucket-region for region discovery / redirect handling.
	c.Header("x-amz-bucket-region", h.bucketRegion())
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// CopyObject handles PUT /{bucket}/{key} with x-amz-copy-source header
func (h *S3APIHandler) CopyObject(c *gin.Context, copySource string) {
	destBucket := c.Param("bucket")
	destKey := strings.TrimPrefix(c.Param("key"), "/")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	// x-amz-copy-source may be URL-encoded; format is /srcBucket/srcKey
	decoded, err := url.PathUnescape(copySource)
	if err != nil {
		decoded = copySource
	}
	decoded = strings.TrimPrefix(decoded, "/")
	slashIdx := strings.Index(decoded, "/")
	if slashIdx < 0 {
		h.s3Error(c, "InvalidArgument", "Invalid copy source", copySource, http.StatusBadRequest)
		return
	}
	srcBucket := decoded[:slashIdx]
	srcKey := decoded[slashIdx+1:]

	if validation.IsReservedObjectKey(srcKey) {
		h.s3Error(c, "NoSuchKey", "Source key does not exist", srcKey, http.StatusNotFound)
		return
	}
	if err := validation.ValidateObjectKey(destKey); err != nil {
		h.s3Error(c, "InvalidArgument", err.Error(), destKey, http.StatusBadRequest)
		return
	}

	// Permission: GetObject on source
	if allowed, _ := h.policyService.CheckObjectAccess(userUUID, srcBucket, srcKey, services.ActionGetObject); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied on source", srcKey, http.StatusForbidden)
		return
	}
	// Permission: PutObject on destination
	if allowed, _ := h.policyService.CheckObjectAccess(userUUID, destBucket, destKey, services.ActionPutObject); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied on destination", destKey, http.StatusForbidden)
		return
	}

	var srcBucketModel, destBucketModel models.Bucket
	if err := database.DB.Where("name = ?", srcBucket).First(&srcBucketModel).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "Source bucket does not exist", srcBucket, http.StatusNotFound)
		return
	}
	if err := database.DB.Where("name = ?", destBucket).First(&destBucketModel).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "Destination bucket does not exist", destBucket, http.StatusNotFound)
		return
	}
	sameObject := srcBucket == destBucket && srcKey == destKey
	metaReplace := strings.EqualFold(c.GetHeader("x-amz-metadata-directive"), "REPLACE")
	tagReplace := strings.EqualFold(c.GetHeader("x-amz-tagging-directive"), "REPLACE")
	if sameObject && !metaReplace && !tagReplace {
		h.s3Error(c, "InvalidRequest", "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.", destKey, http.StatusBadRequest)
		return
	}

	// Metadata directive: COPY (default) carries the source's user metadata to
	// the destination; REPLACE takes the request's x-amz-meta-* headers instead.
	var replaceMeta map[string]string
	if metaReplace {
		m, merr := extractUserMetadata(c)
		if merr != nil {
			h.s3Error(c, "MetadataTooLarge", merr.Error(), destKey, http.StatusBadRequest)
			return
		}
		replaceMeta = m
	}
	var replaceTags map[string]string
	if tagReplace {
		t, terr := parseTaggingHeader(c.GetHeader("x-amz-tagging"))
		if terr != nil {
			h.s3Error(c, "InvalidTag", terr.Error(), destKey, http.StatusBadRequest)
			return
		}
		replaceTags = t
	}

	srcStorage, err := h.bucketHandler.getStorageBackend(&srcBucketModel)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize source storage", srcKey, http.StatusInternalServerError)
		return
	}
	destStorage, err := h.bucketHandler.getStorageBackend(&destBucketModel)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize destination storage", destKey, http.StatusInternalServerError)
		return
	}

	// Serialize with other writers of the destination, and of the source so
	// it is read in a stable state (a concurrent overwrite/delete/archive of
	// the source must not swap its bytes out mid-copy). Both helpers use the
	// same global lock order, so this cannot deadlock.
	var unlock func()
	var lockErr error
	if srcBucket == destBucket {
		unlock, lockErr = lockObjectKeysChecked(destBucket, srcKey, destKey)
	} else {
		unlock, lockErr = lockKeysAcrossBucketsChecked(srcBucket, srcKey, destBucket, destKey)
	}
	if lockErr != nil {
		h.s3Error(c, "InternalError", "Failed to lock copy keys", destKey, http.StatusInternalServerError)
		return
	}
	defer unlock()
	if _, ok := h.checkWritePreconditions(c, &destBucketModel, destKey); !ok {
		return
	}

	var srcObj models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", srcBucketModel.ID, srcKey).First(&srcObj).Error; err != nil {
		h.s3Error(c, "NoSuchKey", "Source key does not exist", srcKey, http.StatusNotFound)
		return
	}
	if !h.checkCopySourcePreconditions(c, &srcObj, srcKey) {
		return
	}

	destMeta := jsonPtrToMap(srcObj.Metadata)
	if metaReplace {
		destMeta = replaceMeta
	}
	destTags := jsonPtrToMap(srcObj.Tags)
	if tagReplace {
		destTags = replaceTags
	}
	destContentType := srcObj.ContentType
	destDisposition := srcObj.ContentDisposition
	destCacheControl := srcObj.CacheControl
	if metaReplace {
		if ct := c.GetHeader("Content-Type"); ct != "" {
			destContentType = ct
		}
		destDisposition = c.GetHeader("Content-Disposition")
		destCacheControl = c.GetHeader("Cache-Control")
	}

	// Quota: the copy adds srcObj.Size bytes to the destination (a metadata-
	// only self-copy adds nothing).
	var reservation *quotaReservation
	if !sameObject {
		res, qerr := reserveBucketQuota(&destBucketModel, srcObj.Size)
		if qerr != nil {
			h.s3Error(c, "QuotaExceeded", qerr.Error(), destKey, http.StatusForbidden)
			return
		}
		reservation = res
	}
	defer reservation.release()

	hadPrior := currentObjectExists(destBucketModel.ID, destKey)

	// Versioning: archive the destination's current version before overwrite.
	archivedVID, verr := prepareVersionedWrite(destStorage, &destBucketModel, destKey)
	if verr != nil {
		h.s3Error(c, "InternalError", "Failed to version existing object", destKey, http.StatusInternalServerError)
		return
	}
	// Every failure before the destination bytes are written must restore the
	// archived version.
	writeFailed := func(code, msg, resource string) {
		rollbackVersionedWrite(destStorage, &destBucketModel, destKey, archivedVID)
		h.s3Error(c, code, msg, resource, http.StatusInternalServerError)
	}

	switch {
	case sameObject && archivedVID == "":
		// Unversioned metadata-only self-copy: the bytes are unchanged.
	case sameObject:
		// Versioned self-copy: the current bytes were just archived, so copy
		// them forward from version storage as the new current version.
		rc, err := destStorage.GetObjectVersion(destBucket, destKey, archivedVID)
		if err != nil {
			writeFailed("InternalError", "Failed to read source object", srcKey)
			return
		}
		perr := destStorage.PutObject(destBucket, destKey, rc, srcObj.Size, destContentType, destMeta)
		_ = rc.Close()
		if perr != nil {
			writeFailed("InternalError", "Failed to write destination object", destKey)
			return
		}
	case srcBucket == destBucket:
		if err := srcStorage.CopyObject(srcBucket, srcKey, destKey); err != nil {
			writeFailed("InternalError", "Failed to copy object", srcKey)
			return
		}
	default:
		reader, err := srcStorage.GetObject(srcBucket, srcKey)
		if err != nil {
			writeFailed("InternalError", "Failed to read source object", srcKey)
			return
		}
		// The streamed source must be exactly srcObj.Size bytes: a short
		// read fails the write (instead of committing a truncated copy),
		// and so does a source longer than its metadata says.
		src := newGuardedBody(reader, srcObj.Size, 0, nil)
		perr := destStorage.PutObject(destBucket, destKey, src, srcObj.Size, destContentType, destMeta)
		_ = reader.Close()
		if perr != nil || !src.Complete() {
			if perr == nil {
				// The backend reported success on an unverified body.
				discardFailedWrite(destStorage, &destBucketModel, destKey, archivedVID, hadPrior)
				h.s3Error(c, "InternalError", "Source object length does not match its metadata", srcKey, http.StatusInternalServerError)
				return
			}
			writeFailed("InternalError", "Failed to write destination object", destKey)
			return
		}
	}

	// Record the destination as the new current version (atomic upsert).
	info, err := destStorage.GetObjectInfo(destBucket, destKey)
	if err != nil {
		discardFailedWrite(destStorage, &destBucketModel, destKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to read destination object", destKey, http.StatusInternalServerError)
		return
	}
	destObj := models.Object{
		BucketID:           destBucketModel.ID,
		Key:                destKey,
		Size:               info.Size,
		ContentType:        destContentType,
		ContentDisposition: destDisposition,
		CacheControl:       destCacheControl,
		ETag:               info.ETag,
		SHA256:             srcObj.SHA256,
		StoragePath:        destKey,
		Metadata:           mapToJSONPtr(destMeta),
		Tags:               mapToJSONPtr(destTags),
		VersionID:          newCurrentVersionID(&destBucketModel),
	}
	if sameObject {
		destObj.CreatedAt = srcObj.CreatedAt
	}
	if err := upsertCurrentObject(&destObj); err != nil {
		discardFailedWrite(destStorage, &destBucketModel, destKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to save object metadata", destKey, http.StatusInternalServerError)
		return
	}

	notifyObjectEvent(&destBucketModel, services.EventObjectCreated, destKey, destObj.Size, destObj.ETag, destObj.VersionID)

	if destObj.VersionID != "" {
		c.Header("x-amz-version-id", destObj.VersionID)
	}
	c.Header("ETag", fmt.Sprintf(`"%s"`, destObj.ETag))
	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, CopyObjectResult{ETag: fmt.Sprintf(`"%s"`, destObj.ETag), LastModified: destObj.UpdatedAt})
}

// HandleBucketPost dispatches POST /{bucket} based on query params. Only
// ?delete (DeleteObjects) is implemented; anything else is 501.
func (h *S3APIHandler) HandleBucketPost(c *gin.Context) {
	if _, exists := c.GetQuery("delete"); exists {
		h.DeleteObjects(c)
		return
	}
	if _, authed := h.s3Caller(c); !authed {
		return
	}
	h.notImplemented(c, bucketSubresourceName(c.Request.URL.Query()))
}

// DeleteObjects handles POST /{bucket}?delete (bulk delete)
func (h *S3APIHandler) DeleteObjects(c *gin.Context) {
	bucketName := c.Param("bucket")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}

	body, err := readBoundedBody(c.Request.Body, 1<<20) // 1MB max
	if err != nil {
		h.s3Error(c, "MalformedXML", "Failed to read request body", "", http.StatusBadRequest)
		return
	}

	var deleteReq DeleteRequest
	if err := xml.Unmarshal(body, &deleteReq); err != nil {
		h.s3Error(c, "MalformedXML", "Invalid delete request XML", "", http.StatusBadRequest)
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(&bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", "", http.StatusInternalServerError)
		return
	}

	result := DeleteResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}

	for _, obj := range deleteReq.Objects {
		// Same authorization as the single DeleteObject (with or without
		// ?versionId): bkt's policy model has no separate
		// s3:DeleteObjectVersion action.
		allowed, _ := h.policyService.CheckObjectAccess(userUUID, bucketName, obj.Key, services.ActionDeleteObject)
		if !allowed {
			result.Errors = append(result.Errors, DeleteError{Key: obj.Key, VersionId: obj.VersionId, Code: "AccessDenied", Message: "Access Denied"})
			continue
		}
		var deleted DeletedObject
		var errEntry *DeleteError
		if obj.VersionId != "" {
			deleted, errEntry = h.deleteVersionForBatch(storageBackend, &bucket, obj.Key, obj.VersionId)
		} else {
			deleted, errEntry = h.deleteOneForBatch(storageBackend, &bucket, obj.Key)
		}
		if errEntry != nil {
			result.Errors = append(result.Errors, *errEntry)
			continue
		}
		if !deleteReq.Quiet {
			result.Deleted = append(result.Deleted, deleted)
		}
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, result)
}

// deleteOneForBatch deletes one key for DeleteObjects under the key's write
// lock, with the same versioning and retention rules as DeleteObject.
func (h *S3APIHandler) deleteOneForBatch(storageBackend storage.StorageBackend, bucket *models.Bucket, key string) (DeletedObject, *DeleteError) {
	// S3 treats deleting a non-existent key as success; the internal version
	// keyspace is never an object.
	if validation.IsReservedObjectKey(key) {
		return DeletedObject{Key: key}, nil
	}
	unlock, lockErr := lockObjectKeysChecked(bucket.Name, key)
	if lockErr != nil {
		return DeletedObject{}, &DeleteError{Key: key, Code: "InternalError", Message: "Failed to lock object"}
	}
	defer unlock()

	var dbObj models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, key).First(&dbObj).Error; err != nil {
		return DeletedObject{Key: key}, nil
	}

	if markerID, handled, derr := versionedDeleteCurrent(storageBackend, bucket, &dbObj); handled {
		if derr != nil {
			return DeletedObject{}, &DeleteError{Key: key, Code: "InternalError", Message: "Failed to delete"}
		}
		notifyObjectEvent(bucket, services.EventObjectRemoved, key, 0, "", markerID)
		return DeletedObject{Key: key, DeleteMarker: true, DeleteMarkerVersionId: markerID}, nil
	}

	if retentionBlocks(bucket, dbObj.UpdatedAt) {
		return DeletedObject{}, &DeleteError{Key: key, Code: "AccessDenied", Message: "Object is under retention"}
	}
	if err := storageBackend.DeleteObject(bucket.Name, key); err != nil {
		return DeletedObject{}, &DeleteError{Key: key, Code: "InternalError", Message: "Failed to delete"}
	}
	if err := database.DB.Delete(&dbObj).Error; err != nil {
		return DeletedObject{}, &DeleteError{Key: key, Code: "InternalError", Message: "Failed to delete object metadata"}
	}
	notifyObjectEvent(bucket, services.EventObjectRemoved, key, 0, "", "")
	return DeletedObject{Key: key}, nil
}

// deleteVersionLocked permanently deletes one version of key (the caller
// holds the key's write lock), reporting whether that version was a delete
// marker. A version that does not exist is a successful no-op, as on S3;
// retention violations are returned as errUnderRetention.
func deleteVersionLocked(backend storage.StorageBackend, bucket *models.Bucket, key, versionID string) (wasMarker bool, err error) {
	var n int64
	database.DB.Model(&models.ObjectVersion{}).
		Where("bucket_id = ? AND key = ? AND version_id = ? AND is_delete_marker = ?", bucket.ID, key, versionID, true).
		Count(&n)
	if err := deleteSpecificVersion(backend, bucket, key, versionID); err != nil {
		if errors.Is(err, errVersionNotFound) {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}

// deleteVersionForBatch is DeleteObjects' version-addressed delete: the same
// lock, retention rules and semantics as DELETE /{bucket}/{key}?versionId=.
func (h *S3APIHandler) deleteVersionForBatch(backend storage.StorageBackend, bucket *models.Bucket, key, versionID string) (DeletedObject, *DeleteError) {
	if validation.IsReservedObjectKey(key) {
		return DeletedObject{Key: key, VersionId: versionID}, nil
	}
	unlock, lockErr := lockObjectKeysChecked(bucket.Name, key)
	if lockErr != nil {
		return DeletedObject{}, &DeleteError{Key: key, VersionId: versionID, Code: "InternalError", Message: "Failed to lock object"}
	}
	defer unlock()
	wasMarker, err := deleteVersionLocked(backend, bucket, key, versionID)
	if err != nil {
		if errors.Is(err, errUnderRetention) {
			return DeletedObject{}, &DeleteError{Key: key, VersionId: versionID, Code: "AccessDenied", Message: err.Error()}
		}
		return DeletedObject{}, &DeleteError{Key: key, VersionId: versionID, Code: "InternalError", Message: "Failed to delete version"}
	}
	d := DeletedObject{Key: key, VersionId: versionID}
	if wasMarker {
		d.DeleteMarker = true
		d.DeleteMarkerVersionId = versionID
	}
	return d, nil
}

// s3Caller returns the SigV4-authenticated user of an S3 request. When there
// is none — an anonymous public-read request (middleware.CtxS3Anonymous) that
// reached a handler other than the object GET/HEAD path — it answers 403
// AccessDenied and returns false, so every handler fails closed rather than
// panicking or acting without a principal.
func (h *S3APIHandler) s3Caller(c *gin.Context) (uuid.UUID, bool) {
	if v, ok := c.Get("user_id"); ok {
		if id, ok := v.(uuid.UUID); ok && id != uuid.Nil {
			return id, true
		}
	}
	h.s3Error(c, "AccessDenied", "Access Denied", "", http.StatusForbidden)
	return uuid.Nil, false
}

// s3Error sends an S3-compatible XML error response
func (h *S3APIHandler) s3Error(c *gin.Context, code, message, resource string, status int) {
	errorResponse := Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: uuid.New().String(),
	}
	c.XML(status, errorResponse)
}

// CreateBucket handles PUT /{bucket} (create bucket).
//
// Buckets are created through the web UI only, but tools such as rclone and
// SDK helpers probe-create the bucket they are about to write to. For an
// existing bucket this answers like AWS does: 409 BucketAlreadyOwnedByYou
// when the caller has access to it (clients treat that as success), 409
// BucketAlreadyExists otherwise. Creating a new bucket stays AccessDenied.
// BucketOr routes a request addressed as "/bucket/" (trailing slash, empty
// key — e.g. `mc mb`, some SDKs in path-style mode) to the bucket-level
// handler instead of the object handler, which would reject the empty key.
func BucketOr(bucketLevel, objectLevel gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if k := c.Param("key"); k == "" || k == "/" {
			bucketLevel(c)
			return
		}
		objectLevel(c)
	}
}

// DeleteBucketNotSupported answers DELETE /bucket/: like bucket creation,
// bucket deletion is only available from the web console / REST API.
func (h *S3APIHandler) DeleteBucketNotSupported(c *gin.Context) {
	h.s3Error(c, "AccessDenied", "Bucket deletion via S3 API is not supported. Use web UI.", "", http.StatusForbidden)
}

func (h *S3APIHandler) CreateBucket(c *gin.Context) {
	if h.dispatchBucketPut(c) {
		return
	}

	// The answer below reveals whether the bucket exists: callers only.
	if _, authed := h.s3Caller(c); !authed {
		return
	}
	bucketName := c.Param("bucket")
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err == nil {
		if h.callerHasBucketAccess(c, bucketName) {
			h.s3Error(c, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", bucketName, http.StatusConflict)
			return
		}
		h.s3Error(c, "BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.", bucketName, http.StatusConflict)
		return
	}

	h.s3Error(c, "AccessDenied", "Bucket creation via S3 API is not supported. Use web UI.", "", http.StatusForbidden)
}

// callerHasBucketAccess reports whether the caller is an admin or may list
// or write objects in the bucket (write-only credentials also probe-create).
func (h *S3APIHandler) callerHasBucketAccess(c *gin.Context, bucketName string) bool {
	if isAdmin, _ := c.Get("is_admin"); isAdmin == true {
		return true
	}
	userID, ok := c.Get("user_id")
	if !ok {
		return false
	}
	userUUID, ok := userID.(uuid.UUID)
	if !ok {
		return false
	}
	if allowed, _ := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket); allowed {
		return true
	}
	allowed, _ := h.policyService.CheckObjectAccess(userUUID, bucketName, "*", services.ActionPutObject)
	return allowed
}
