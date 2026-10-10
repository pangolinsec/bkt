package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/services"
	"bkt/internal/validation"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ProgressReader wraps an io.ReadSeeker and tracks upload progress in real-time
type ProgressReader struct {
	reader            io.ReadSeeker
	uploadID          uuid.UUID
	totalSize         int64
	bytesRead         int64
	lastUpdate        time.Time
	updateMutex       sync.Mutex
	minUpdateInterval time.Duration
}

// NewProgressReader creates a new progress tracking reader
func NewProgressReader(reader io.ReadSeeker, uploadID uuid.UUID, totalSize int64) *ProgressReader {
	return &ProgressReader{
		reader:            reader,
		uploadID:          uploadID,
		totalSize:         totalSize,
		bytesRead:         0,
		lastUpdate:        time.Now(),
		minUpdateInterval: 500 * time.Millisecond, // Update DB at most every 500ms
	}
}

// Read implements io.Reader and tracks progress
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 {
		pr.updateMutex.Lock()
		pr.bytesRead += int64(n)

		// Update database periodically to avoid too many writes
		now := time.Now()
		if now.Sub(pr.lastUpdate) >= pr.minUpdateInterval {
			pr.lastUpdate = now

			// Update in database (non-blocking) with actual bytes uploaded
			// This gives smooth incremental progress as bytes are transferred
			go func(bytesUploaded int64) {
				database.DB.Model(&models.Upload{}).
					Where("id = ?", pr.uploadID).
					Update("uploaded_size", bytesUploaded)
			}(pr.bytesRead)
		}
		pr.updateMutex.Unlock()
	}
	return n, err
}

// Seek implements io.Seeker to support AWS SDK retries
func (pr *ProgressReader) Seek(offset int64, whence int) (int64, error) {
	pr.updateMutex.Lock()
	defer pr.updateMutex.Unlock()

	// Delegate seek to underlying reader
	pos, err := pr.reader.Seek(offset, whence)
	if err != nil {
		return pos, err
	}

	// Reset bytesRead to match the new position
	// This ensures progress tracking remains accurate after seeks
	pr.bytesRead = pos

	return pos, nil
}

// UploadObjectAsync initiates an asynchronous upload and returns immediately with upload ID
func (h *BucketHandler) UploadObjectAsync(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Get bucket from database
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Bucket not found",
		})
		return
	}

	// Cap the request body before the multipart form is parsed (and spooled
	// to temp disk) — see limitMultipartBody.
	if !h.limitMultipartBody(c) {
		return
	}

	// Get object key from form or query
	objectKey := c.PostForm("key")
	if objectKey == "" {
		objectKey = c.Query("key")
	}
	if objectKey == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Object key is required",
		})
		return
	}

	// Validate object key
	if err := validation.ValidateObjectKey(objectKey); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid object key",
			Message: err.Error(),
		})
		return
	}

	// Check policy permissions
	allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionPutObject)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Policy check failed",
			Message: err.Error(),
		})
		return
	}
	if !allowed {
		c.JSON(http.StatusForbidden, models.ErrorResponse{
			Error:   "Permission denied",
			Message: "You don't have permission to upload objects to this bucket",
		})
		return
	}

	// Get uploaded file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Failed to get file",
			Message: err.Error(),
		})
		return
	}

	// Validate file size
	if fileHeader.Size < 0 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid file size",
			Message: "File size cannot be negative",
		})
		return
	}

	if fileHeader.Size > h.config.Storage.MaxFileSize {
		c.JSON(http.StatusRequestEntityTooLarge, models.ErrorResponse{
			Error:   "File too large",
			Message: fmt.Sprintf("Maximum file size is %d bytes", h.config.Storage.MaxFileSize),
		})
		return
	}

	// Open uploaded file to detect content type
	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to open file",
			Message: err.Error(),
		})
		return
	}

	// Detect content type
	detectedType, _, err := validation.DetectContentType(file)
	_ = file.Close()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to detect content type",
			Message: err.Error(),
		})
		return
	}

	// Validate content type
	if !validation.IsSafeContentType(detectedType) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Forbidden file type",
			Message: fmt.Sprintf("File type '%s' is not allowed", detectedType),
		})
		return
	}

	// Create upload record
	upload := models.Upload{
		UserID:      userUUID,
		BucketName:  bucketName,
		ObjectKey:   objectKey,
		Filename:    fileHeader.Filename,
		ContentType: detectedType,
		TotalSize:   fileHeader.Size,
		Status:      models.UploadStatusPending,
	}

	// Fast-fail when the bucket is already too full for this file; the
	// authoritative quota check runs again when the write is processed.
	qres, qerr := reserveBucketQuota(&bucket, fileHeader.Size)
	if qerr != nil {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "Quota exceeded", Message: qerr.Error()})
		return
	}
	qres.release()

	if err := database.DB.Create(&upload).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to create upload record",
			Message: err.Error(),
		})
		return
	}

	failUpload := func(msg string) {
		upload.Status = models.UploadStatusFailed
		upload.ErrorMessage = msg
		database.DB.Save(&upload)
	}

	// Save file to temporary location for background processing. The file
	// name is fixed (never the client-supplied filename).
	tempDir := filepath.Join(os.TempDir(), "bkt-uploads", upload.ID.String())
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		failUpload("Failed to create temporary directory")
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to create temporary directory",
			Message: err.Error(),
		})
		return
	}

	tempFilePath := filepath.Join(tempDir, "upload.bin")
	if err := c.SaveUploadedFile(fileHeader, tempFilePath); err != nil {
		_ = os.RemoveAll(tempDir)
		failUpload("Failed to save uploaded file")
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to save uploaded file",
			Message: err.Error(),
		})
		return
	}

	// Start background upload processing
	go h.processAsyncUpload(upload.ID, tempFilePath, bucket.ID)

	// Return upload ID immediately
	c.JSON(http.StatusAccepted, gin.H{
		"upload_id": upload.ID,
		"status":    upload.Status,
		"message":   "Upload initiated. Use /api/uploads/" + upload.ID.String() + "/status to check progress.",
	})
}

// processAsyncUpload processes the upload in the background. It uses the
// same commit path as the synchronous upload (storeObject): per-key lock,
// quota reservation, versioned archive, write, and an atomic metadata upsert
// with a fresh version id — rolling back if any step fails.
func (h *BucketHandler) processAsyncUpload(uploadID uuid.UUID, tempFilePath string, bucketID uuid.UUID) {
	// Ensure temp file is cleaned up
	defer func() {
		_ = os.Remove(tempFilePath)
		_ = os.Remove(filepath.Dir(tempFilePath)) // Remove temp directory
	}()

	// Get upload record
	var upload models.Upload
	if err := database.DB.First(&upload, uploadID).Error; err != nil {
		logger.Error("Failed to fetch upload record", map[string]interface{}{
			"upload_id": uploadID,
			"error":     err.Error(),
		})
		return
	}

	fail := func(msg string) {
		upload.Status = models.UploadStatusFailed
		upload.ErrorMessage = msg
		database.DB.Save(&upload)
	}

	// Reload the bucket: its versioning/quota settings may have changed
	// since the request was accepted.
	var bucket models.Bucket
	if err := database.DB.Where("id = ?", bucketID).First(&bucket).Error; err != nil {
		fail("Bucket no longer exists")
		return
	}

	// Update status to processing
	upload.Status = models.UploadStatusProcessing
	upload.UploadedSize = 0 // Start at 0%
	database.DB.Save(&upload)

	// Open temp file
	file, err := os.Open(tempFilePath) //nolint:gosec // server-generated temp path (os.TempDir + upload UUID), not user input
	if err != nil {
		fail(fmt.Sprintf("Failed to open temporary file: %v", err))
		return
	}
	defer file.Close() //nolint:errcheck // best-effort close of read-only temp file

	// Re-detect content type from file
	detectedType, _, err := validation.DetectContentType(file)
	if err != nil {
		fail(fmt.Sprintf("Failed to detect content type: %v", err))
		return
	}

	// SHA256 of the content (computed from the local temp file before the
	// write so the metadata row is complete in one upsert).
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		fail(fmt.Sprintf("Failed to read temporary file: %v", err))
		return
	}
	sha256Hash, err := validation.CalculateSHA256(file)
	if err != nil {
		logger.Warn("Failed to calculate SHA256 hash", map[string]interface{}{
			"upload_id": uploadID,
			"error":     err.Error(),
		})
		sha256Hash = "" // Continue without hash
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		fail(fmt.Sprintf("Failed to read temporary file: %v", err))
		return
	}

	// Get storage backend
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		fail(fmt.Sprintf("Failed to initialize storage backend: %v", err))
		return
	}

	startTime := time.Now()

	// Wrap file with progress tracker for real-time updates. File implements
	// io.ReadSeeker, so ProgressReader stays seekable for AWS SDK retries.
	progressReader := NewProgressReader(file, upload.ID, upload.TotalSize)

	lockCtx, cancelLock := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancelLock()
	object, _, err := h.storeObject(lockCtx, storageBackend, &bucket, upload.ObjectKey, progressReader, upload.TotalSize, detectedType, sha256Hash)
	if err != nil {
		fail(fmt.Sprintf("Failed to store object: %v", err))
		return
	}
	uploadDuration := time.Since(startTime)

	notifyObjectEvent(&bucket, services.EventObjectCreated, object.Key, object.Size, object.ETag, object.VersionID)

	// Update upload status to completed
	now := time.Now()
	upload.Status = models.UploadStatusCompleted
	upload.UploadedSize = upload.TotalSize
	upload.CompletedAt = &now
	upload.ObjectID = &object.ID
	upload.ErrorMessage = ""
	database.DB.Save(&upload)

	logger.Info("Async upload completed", map[string]interface{}{
		"upload_id":     uploadID,
		"object_id":     object.ID,
		"size_bytes":    upload.TotalSize,
		"duration":      uploadDuration.String(),
		"average_speed": fmt.Sprintf("%.2f MB/s", float64(upload.TotalSize)/(1024*1024)/uploadDuration.Seconds()),
	})
}

// GetUploadStatus returns the current status of an upload
func (h *BucketHandler) GetUploadStatus(c *gin.Context) {
	uploadIDStr := c.Param("id")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	uploadID, err := uuid.Parse(uploadIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Invalid upload ID",
		})
		return
	}

	// Get upload record
	var upload models.Upload
	if err := database.DB.Where("id = ? AND user_id = ?", uploadID, userUUID).First(&upload).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Upload not found",
		})
		return
	}

	// Calculate progress percentage
	progressPct := 0.0
	if upload.TotalSize > 0 {
		progressPct = float64(upload.UploadedSize) / float64(upload.TotalSize) * 100
	}

	// Build response
	response := models.UploadStatusResponse{
		ID:           upload.ID,
		Status:       upload.Status,
		Filename:     upload.Filename,
		ObjectKey:    upload.ObjectKey,
		TotalSize:    upload.TotalSize,
		UploadedSize: upload.UploadedSize,
		ProgressPct:  progressPct,
		ErrorMessage: upload.ErrorMessage,
		ObjectID:     upload.ObjectID,
		CreatedAt:    upload.CreatedAt,
		CompletedAt:  upload.CompletedAt,
	}

	c.JSON(http.StatusOK, response)
}

// ListUploads returns all uploads for the current user
func (h *BucketHandler) ListUploads(c *gin.Context) {
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Optional query parameters for filtering
	status := c.Query("status") // e.g., "pending", "processing", "completed", "failed"
	limit := 50                 // Default limit
	if limitStr := c.Query("limit"); limitStr != "" {
		_, _ = fmt.Sscanf(limitStr, "%d", &limit)
		if limit > 100 {
			limit = 100 // Max 100 results
		}
	}

	query := database.DB.Where("user_id = ?", userUUID)
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var uploads []models.Upload
	if err := query.Order("created_at DESC").Limit(limit).Find(&uploads).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to fetch uploads",
			Message: err.Error(),
		})
		return
	}

	// Convert to response format
	responses := make([]models.UploadStatusResponse, len(uploads))
	for i, upload := range uploads {
		progressPct := 0.0
		if upload.TotalSize > 0 {
			progressPct = float64(upload.UploadedSize) / float64(upload.TotalSize) * 100
		}

		responses[i] = models.UploadStatusResponse{
			ID:           upload.ID,
			Status:       upload.Status,
			Filename:     upload.Filename,
			ObjectKey:    upload.ObjectKey,
			TotalSize:    upload.TotalSize,
			UploadedSize: upload.UploadedSize,
			ProgressPct:  progressPct,
			ErrorMessage: upload.ErrorMessage,
			ObjectID:     upload.ObjectID,
			CreatedAt:    upload.CreatedAt,
			CompletedAt:  upload.CompletedAt,
		}
	}

	c.JSON(http.StatusOK, responses)
}
