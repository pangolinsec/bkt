package api

import (
	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Multipart XML types

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   []PartElement `xml:"Part"`
}

type PartElement struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type ListPartsResult struct {
	XMLName      xml.Name        `xml:"ListPartsResult"`
	Xmlns        string          `xml:"xmlns,attr"`
	Bucket       string          `xml:"Bucket"`
	Key          string          `xml:"Key"`
	UploadId     string          `xml:"UploadId"`
	StorageClass string          `xml:"StorageClass"`
	IsTruncated  bool            `xml:"IsTruncated"`
	Parts        []ListPartEntry `xml:"Part"`
}

type ListMultipartUploadsResult struct {
	XMLName        xml.Name               `xml:"ListMultipartUploadsResult"`
	Xmlns          string                 `xml:"xmlns,attr"`
	Bucket         string                 `xml:"Bucket"`
	KeyMarker      string                 `xml:"KeyMarker"`
	UploadIdMarker string                 `xml:"UploadIdMarker"`
	Prefix         string                 `xml:"Prefix"`
	MaxUploads     int                    `xml:"MaxUploads"`
	IsTruncated    bool                   `xml:"IsTruncated"`
	Uploads        []MultipartUploadEntry `xml:"Upload"`
}

type MultipartUploadEntry struct {
	Key          string    `xml:"Key"`
	UploadId     string    `xml:"UploadId"`
	Initiated    time.Time `xml:"Initiated"`
	StorageClass string    `xml:"StorageClass"`
}

type CopyPartResult struct {
	XMLName      xml.Name  `xml:"CopyPartResult"`
	Xmlns        string    `xml:"xmlns,attr"`
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

type ListPartEntry struct {
	PartNumber   int       `xml:"PartNumber"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

// HandleObjectPost dispatches POST /{bucket}/{key} based on query params
func (h *S3APIHandler) HandleObjectPost(c *gin.Context) {
	if _, exists := c.GetQuery("uploads"); exists {
		h.CreateMultipartUpload(c)
		return
	}
	if uploadID := c.Query("uploadId"); uploadID != "" {
		h.CompleteMultipartUpload(c)
		return
	}
	// ?restore, ?select, ... (and a bare POST) are not implemented.
	if _, authed := h.s3Caller(c); !authed {
		return
	}
	what := bucketSubresourceName(c.Request.URL.Query())
	if what == "" {
		h.s3Error(c, "NotImplemented", "This POST operation is not implemented", strings.TrimPrefix(c.Param("key"), "/"), http.StatusNotImplemented)
		return
	}
	h.objectNotImplemented(c, what)
}

// CreateMultipartUpload handles POST /{bucket}/{key}?uploads
func (h *S3APIHandler) CreateMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	userUUID, authed := h.s3Caller(c)
	if !authed {
		return
	}

	// Same key validation as PutObject: without it a bad key would only be
	// caught at Complete time, after the client uploaded every part.
	if err := validation.ValidateObjectKey(objectKey); err != nil {
		h.s3Error(c, "InvalidArgument", err.Error(), objectKey, http.StatusBadRequest)
		return
	}

	if allowed, _ := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionPutObject); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}

	contentType := c.GetHeader("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// x-amz-meta-* captured at initiate, applied to the object at complete
	// (and passed to the real-S3 backend so the stored object carries it too).
	userMeta, err := extractUserMetadata(c)
	if err != nil {
		h.s3Error(c, "MetadataTooLarge", err.Error(), objectKey, http.StatusBadRequest)
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(&bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	uploadID, err := storageBackend.CreateMultipartUpload(bucketName, objectKey, contentType, userMeta)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to create multipart upload", objectKey, http.StatusInternalServerError)
		return
	}

	// Track in database for cleanup purposes
	mpu := models.MultipartUpload{
		UploadID:           uploadID,
		UserID:             userUUID,
		BucketName:         bucketName,
		ObjectKey:          objectKey,
		ContentType:        contentType,
		ContentDisposition: c.GetHeader("Content-Disposition"),
		CacheControl:       c.GetHeader("Cache-Control"),
		Metadata:           mapToJSONPtr(userMeta),
		Status:             "in-progress",
		ExpiresAt:          time.Now().Add(7 * 24 * time.Hour),
	}
	if err := database.DB.Create(&mpu).Error; err != nil {
		// Without the tracking row every later op on this uploadId would 404,
		// so don't hand out an unusable id — clean up the staging area and fail.
		_ = storageBackend.AbortMultipartUpload(bucketName, objectKey, uploadID)
		h.s3Error(c, "InternalError", "Failed to record multipart upload", objectKey, http.StatusInternalServerError)
		return
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadID,
	})
}

// authorizeMultipartUpload loads the tracked multipart upload, verifies it
// belongs to the caller (or the caller is an admin) and matches the addressed
// bucket/key, and enforces the given policy action. It returns the authoritative
// upload record and bucket so callers operate on the recorded bucket/key rather
// than trusting request parameters. Without this, any authenticated caller could
// write into, complete, abort, or list another user's in-flight upload.
func (h *S3APIHandler) authorizeMultipartUpload(c *gin.Context, bucketName, objectKey, uploadID, action string) (*models.MultipartUpload, *models.Bucket, bool) {
	userID, exists := c.Get("user_id")
	if !exists {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return nil, nil, false
	}
	userUUID, ok := userID.(uuid.UUID)
	if !ok {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return nil, nil, false
	}
	isAdminVal, _ := c.Get("is_admin")
	admin, _ := isAdminVal.(bool)

	if uploadID == "" {
		h.s3Error(c, "InvalidArgument", "uploadId is required", objectKey, http.StatusBadRequest)
		return nil, nil, false
	}

	var mpu models.MultipartUpload
	if err := database.DB.Where("upload_id = ?", uploadID).First(&mpu).Error; err != nil {
		h.s3Error(c, "NoSuchUpload", "The specified upload does not exist", uploadID, http.StatusNotFound)
		return nil, nil, false
	}

	// The request-addressed bucket/key must match what the upload was created
	// for, and the caller must own the upload unless they are an admin.
	if mpu.BucketName != bucketName || mpu.ObjectKey != objectKey {
		h.s3Error(c, "NoSuchUpload", "The specified upload does not exist", uploadID, http.StatusNotFound)
		return nil, nil, false
	}
	if !admin && mpu.UserID != userUUID {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return nil, nil, false
	}

	// Enforce the object-level policy for the action.
	if allowed, err := h.policyService.CheckObjectAccess(userUUID, mpu.BucketName, mpu.ObjectKey, action); err != nil || !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", objectKey, http.StatusForbidden)
		return nil, nil, false
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", mpu.BucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", mpu.BucketName, http.StatusNotFound)
		return nil, nil, false
	}

	return &mpu, &bucket, true
}

// UploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=X
func (h *S3APIHandler) UploadPart(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	uploadID := c.Query("uploadId")
	partNumberStr := c.Query("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.s3Error(c, "InvalidArgument", "Invalid part number", objectKey, http.StatusBadRequest)
		return
	}

	mpu, bucket, ok := h.authorizeMultipartUpload(c, bucketName, objectKey, uploadID, services.ActionPutObject)
	if !ok {
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	// Same body pipeline as PutObject: aws-chunked decoding with chunk
	// signature verification, the decoded (not encoded) length as the part
	// size, and a guarded body that enforces that length and the size cap
	// and withholds the final byte until everything verified.
	upload, berr := prepareUploadBody(c, h.config.Storage.MaxFileSize)
	if berr != nil {
		h.s3Error(c, berr.code, berr.msg, objectKey, berr.status)
		return
	}
	body := newGuardedBody(upload.reader, upload.declared, h.config.Storage.MaxFileSize, nil)

	etag, err := storageBackend.UploadPart(mpu.BucketName, mpu.ObjectKey, uploadID, partNumber, body, upload.declared)
	if err != nil || !body.Complete() {
		if code, msg, status, ok := bodyFailure(body); ok {
			h.s3Error(c, code, msg, objectKey, status)
			return
		}
		h.s3Error(c, "InternalError", "Failed to upload part", objectKey, http.StatusInternalServerError)
		return
	}

	c.Header("ETag", fmt.Sprintf(`"%s"`, etag))
	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusOK)
}

// CompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=X
func (h *S3APIHandler) CompleteMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	uploadID := c.Query("uploadId")
	// Callers only, before reading the body (authorizeMultipartUpload below
	// does the full check).
	if _, authed := h.s3Caller(c); !authed {
		return
	}

	body, err := readBoundedBody(c.Request.Body, 4<<20)
	if err != nil {
		h.s3Error(c, "MalformedXML", "Failed to read request body", "", http.StatusBadRequest)
		return
	}

	var req CompleteMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		h.s3Error(c, "MalformedXML", "Invalid complete multipart XML", "", http.StatusBadRequest)
		return
	}

	mpu, bucket, ok := h.authorizeMultipartUpload(c, bucketName, objectKey, uploadID, services.ActionPutObject)
	if !ok {
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	parts := make([]storage.CompletedPart, len(req.Parts))
	for i, p := range req.Parts {
		parts[i] = storage.CompletedPart{PartNumber: p.PartNumber, ETag: strings.Trim(p.ETag, `"`)}
	}
	if len(parts) == 0 {
		h.s3Error(c, "MalformedXML", "The XML you provided did not include any parts", objectKey, http.StatusBadRequest)
		return
	}

	// Serialize with other writers of the target key.
	unlock, lockErr := lockObjectKeysChecked(c.Request.Context(), bucket, mpu.ObjectKey)
	if lockErr != nil {
		h.s3Error(c, "InternalError", "Failed to lock object", mpu.ObjectKey, http.StatusInternalServerError)
		return
	}
	defer unlock()
	hadPrior, ok := h.checkWritePreconditions(c, bucket, mpu.ObjectKey)
	if !ok {
		return
	}

	// Quota: the assembled object's size is the sum of the listed parts. On
	// S3-backed buckets the part listing is only requested when the bucket
	// has a quota: it requires s3:ListMultipartUploadParts on the upstream,
	// which a minimally-privileged backend credential may lack, and without a
	// quota the upstream CompleteMultipartUpload validates the parts itself.
	// Local listings are free and keep the precise InvalidPart error.
	var assembledSize int64
	if bucket.QuotaBytes > 0 || bucket.StorageBackend != "s3" {
		partSizes, err := storage.PartSizes(storageBackend, mpu.BucketName, mpu.ObjectKey, uploadID)
		if err != nil {
			h.s3Error(c, "InternalError", "Failed to list uploaded parts", uploadID, http.StatusInternalServerError)
			return
		}
		seenParts := make(map[int]bool, len(parts))
		for _, p := range parts {
			sz, ok := partSizes[p.PartNumber]
			if !ok {
				h.s3Error(c, "InvalidPart", fmt.Sprintf("Part %d has not been uploaded", p.PartNumber), objectKey, http.StatusBadRequest)
				return
			}
			if !seenParts[p.PartNumber] {
				seenParts[p.PartNumber] = true
				assembledSize += sz
			}
		}
	}
	reservation, qerr := reserveBucketQuota(bucket, assembledSize)
	if qerr != nil {
		h.s3Error(c, "QuotaExceeded", qerr.Error(), objectKey, http.StatusForbidden)
		return
	}
	defer reservation.release()

	// Versioning: archive the current version before the assembled object
	// replaces it.
	archivedVID, verr := prepareVersionedWrite(storageBackend, bucket, mpu.ObjectKey)
	if verr != nil {
		h.s3Error(c, "InternalError", "Failed to version existing object", objectKey, http.StatusInternalServerError)
		return
	}

	if err := storageBackend.CompleteMultipartUpload(mpu.BucketName, mpu.ObjectKey, uploadID, parts); err != nil {
		rollbackVersionedWrite(storageBackend, bucket, mpu.ObjectKey, archivedVID)
		h.s3Error(c, "InternalError", "Failed to complete multipart upload", objectKey, http.StatusInternalServerError)
		return
	}

	// Update DB metadata. If we can't read back the assembled object, that's a
	// real failure — undo the write rather than leaving bytes without a row.
	objInfo, err := storageBackend.GetObjectInfo(mpu.BucketName, mpu.ObjectKey)
	if err != nil || objInfo == nil {
		discardFailedWrite(storageBackend, bucket, mpu.ObjectKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to finalize multipart upload", objectKey, http.StatusInternalServerError)
		return
	}

	obj := models.Object{
		BucketID:           bucket.ID,
		Key:                mpu.ObjectKey,
		Size:               objInfo.Size,
		ContentType:        mpu.ContentType,
		ContentDisposition: mpu.ContentDisposition,
		CacheControl:       mpu.CacheControl,
		ETag:               objInfo.ETag,
		StoragePath:        mpu.ObjectKey,
		Metadata:           mpu.Metadata,
		VersionID:          newCurrentVersionID(bucket),
	}
	if err := upsertCurrentObject(&obj); err != nil {
		discardFailedWrite(storageBackend, bucket, mpu.ObjectKey, archivedVID, hadPrior)
		h.s3Error(c, "InternalError", "Failed to save object metadata", objectKey, http.StatusInternalServerError)
		return
	}
	if obj.VersionID != "" {
		c.Header("x-amz-version-id", obj.VersionID)
	}

	// Mark multipart upload completed
	database.DB.Model(&models.MultipartUpload{}).Where("upload_id = ?", uploadID).Update("status", "completed")

	notifyObjectEvent(bucket, services.EventObjectCreated, mpu.ObjectKey, objInfo.Size, objInfo.ETag, obj.VersionID)

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, CompleteMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: fmt.Sprintf("/%s/%s", mpu.BucketName, mpu.ObjectKey),
		Bucket:   mpu.BucketName,
		Key:      mpu.ObjectKey,
		ETag:     objInfo.ETag,
	})
}

// AbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=X
func (h *S3APIHandler) AbortMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	uploadID := c.Query("uploadId")

	mpu, bucket, ok := h.authorizeMultipartUpload(c, bucketName, objectKey, uploadID, services.ActionPutObject)
	if !ok {
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	if err := storageBackend.AbortMultipartUpload(mpu.BucketName, mpu.ObjectKey, uploadID); err != nil {
		h.s3Error(c, "InternalError", "Failed to abort multipart upload", objectKey, http.StatusInternalServerError)
		return
	}

	database.DB.Model(&models.MultipartUpload{}).Where("upload_id = ?", uploadID).Update("status", "aborted")

	c.Header("x-amz-request-id", uuid.New().String())
	c.Status(http.StatusNoContent)
}

// ListPartsHandler handles GET /{bucket}/{key}?uploadId=X
func (h *S3APIHandler) ListPartsHandler(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	uploadID := c.Query("uploadId")

	mpu, bucket, ok := h.authorizeMultipartUpload(c, bucketName, objectKey, uploadID, services.ActionPutObject)
	if !ok {
		return
	}

	storageBackend, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	parts, err := storageBackend.ListParts(mpu.BucketName, mpu.ObjectKey, uploadID)
	if err != nil {
		h.s3Error(c, "NoSuchUpload", "The specified upload does not exist", uploadID, http.StatusNotFound)
		return
	}

	entries := make([]ListPartEntry, len(parts))
	for i, p := range parts {
		entries[i] = ListPartEntry{
			PartNumber:   p.PartNumber,
			LastModified: p.LastModified,
			ETag:         fmt.Sprintf(`"%s"`, p.ETag),
			Size:         p.Size,
		}
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, ListPartsResult{
		Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:       bucketName,
		Key:          objectKey,
		UploadId:     uploadID,
		StorageClass: "STANDARD",
		IsTruncated:  false,
		Parts:        entries,
	})
}

// ListMultipartUploadsHandler handles GET /{bucket}?uploads
func (h *S3APIHandler) ListMultipartUploadsHandler(c *gin.Context) {
	bucketName := c.Param("bucket")

	userID, exists := c.Get("user_id")
	if !exists {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return
	}
	userUUID, ok := userID.(uuid.UUID)
	if !ok {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return
	}
	isAdminVal, _ := c.Get("is_admin")
	admin, _ := isAdminVal.(bool)

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "The specified bucket does not exist", bucketName, http.StatusNotFound)
		return
	}

	if allowed, _ := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied", bucketName, http.StatusForbidden)
		return
	}

	query := database.DB.Where("bucket_name = ? AND status = ?", bucketName, "in-progress")
	// Non-admin callers see only their own uploads, mirroring the ownership
	// rule authorizeMultipartUpload enforces on per-upload operations.
	if !admin {
		query = query.Where("user_id = ?", userUUID)
	}
	prefix := c.Query("prefix")
	if prefix != "" {
		query = query.Where("object_key LIKE ?", validation.EscapeLikeWildcards(prefix)+"%")
	}

	var uploads []models.MultipartUpload
	if err := query.Order("created_at ASC").Limit(1000).Find(&uploads).Error; err != nil {
		h.s3Error(c, "InternalError", "Failed to list multipart uploads", bucketName, http.StatusInternalServerError)
		return
	}

	entries := make([]MultipartUploadEntry, len(uploads))
	for i, u := range uploads {
		entries[i] = MultipartUploadEntry{
			Key:          u.ObjectKey,
			UploadId:     u.UploadID,
			Initiated:    u.CreatedAt.UTC(),
			StorageClass: "STANDARD",
		}
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, ListMultipartUploadsResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucketName,
		Prefix:      prefix,
		MaxUploads:  1000,
		IsTruncated: false,
		Uploads:     entries,
	})
}

// parseCopySourceRange parses an x-amz-copy-source-range header of the form
// "bytes=start-end" (both bounds required, inclusive) and validates it against
// the source object size.
func parseCopySourceRange(header string, size int64) (start, end int64, err error) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("copy source range must be of the form bytes=start-end")
	}
	dash := strings.Index(spec, "-")
	if dash <= 0 || dash == len(spec)-1 {
		return 0, 0, fmt.Errorf("copy source range must be of the form bytes=start-end")
	}
	start, serr := strconv.ParseInt(spec[:dash], 10, 64)
	end, eerr := strconv.ParseInt(spec[dash+1:], 10, 64)
	if serr != nil || eerr != nil {
		return 0, 0, fmt.Errorf("copy source range must be of the form bytes=start-end")
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, fmt.Errorf("the requested range is not satisfiable for the source object")
	}
	return start, end, nil
}

// UploadPartCopy handles PUT /{bucket}/{key}?partNumber=N&uploadId=X with an
// x-amz-copy-source header: the part's bytes come from an existing object
// (optionally a byte range of it) instead of the request body.
func (h *S3APIHandler) UploadPartCopy(c *gin.Context) {
	bucketName := c.Param("bucket")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	uploadID := c.Query("uploadId")

	partNumber, err := strconv.Atoi(c.Query("partNumber"))
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.s3Error(c, "InvalidArgument", "Invalid part number", objectKey, http.StatusBadRequest)
		return
	}

	// Authorize the destination upload (also verifies ownership and PutObject
	// policy on the recorded bucket/key).
	mpu, bucket, ok := h.authorizeMultipartUpload(c, bucketName, objectKey, uploadID, services.ActionPutObject)
	if !ok {
		return
	}
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID) // validated by authorizeMultipartUpload

	// Parse the copy source exactly like CopyObject does: it may be
	// URL-encoded, format is /srcBucket/srcKey.
	copySource := c.GetHeader("x-amz-copy-source")
	decoded, derr := url.PathUnescape(copySource)
	if derr != nil {
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

	// Permission: GetObject on source
	if allowed, _ := h.policyService.CheckObjectAccess(userUUID, srcBucket, srcKey, services.ActionGetObject); !allowed {
		h.s3Error(c, "AccessDenied", "Access Denied on source", srcKey, http.StatusForbidden)
		return
	}

	var srcBucketModel models.Bucket
	if err := database.DB.Where("name = ?", srcBucket).First(&srcBucketModel).Error; err != nil {
		h.s3Error(c, "NoSuchBucket", "Source bucket does not exist", srcBucket, http.StatusNotFound)
		return
	}
	var srcObj models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", srcBucketModel.ID, srcKey).First(&srcObj).Error; err != nil {
		h.s3Error(c, "NoSuchKey", "Source key does not exist", srcKey, http.StatusNotFound)
		return
	}

	// Optional x-amz-copy-source-range: bytes=start-end (inclusive).
	offset := int64(0)
	length := srcObj.Size
	if rangeHeader := c.GetHeader("x-amz-copy-source-range"); rangeHeader != "" {
		start, end, rerr := parseCopySourceRange(rangeHeader, srcObj.Size)
		if rerr != nil {
			h.s3Error(c, "InvalidRange", rerr.Error(), srcKey, http.StatusRequestedRangeNotSatisfiable)
			return
		}
		offset = start
		length = end - start + 1
	}

	srcStorage, err := h.bucketHandler.getStorageBackend(&srcBucketModel)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize source storage", srcKey, http.StatusInternalServerError)
		return
	}
	destStorage, err := h.bucketHandler.getStorageBackend(bucket)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to initialize storage", objectKey, http.StatusInternalServerError)
		return
	}

	// Early quota check: the part will count towards the destination bucket
	// once the upload completes (Complete enforces it authoritatively).
	qres, qerr := reserveBucketQuota(bucket, length)
	if qerr != nil {
		h.s3Error(c, "QuotaExceeded", qerr.Error(), objectKey, http.StatusForbidden)
		return
	}
	qres.release()

	// Native ranged read of the source (no streaming-and-discarding up to the
	// range start on the S3 backend).
	partReader, err := getCurrentRange(srcStorage, srcBucket, &srcObj, offset, length)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to read source object", srcKey, http.StatusInternalServerError)
		return
	}
	defer partReader.Close() //nolint:errcheck // best-effort close of read stream

	etag, err := destStorage.UploadPart(mpu.BucketName, mpu.ObjectKey, uploadID, partNumber, partReader, length)
	if err != nil {
		h.s3Error(c, "InternalError", "Failed to upload part", objectKey, http.StatusInternalServerError)
		return
	}

	c.Header("x-amz-request-id", uuid.New().String())
	c.XML(http.StatusOK, CopyPartResult{
		Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
		ETag:         fmt.Sprintf(`"%s"`, etag),
		LastModified: time.Now().UTC(),
	})
}
