package api

import (
	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/logger"
	"bkt/internal/models"
	"bkt/internal/security"
	"bkt/internal/services"
	"bkt/internal/storage"
	"bkt/internal/validation"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// s3ConfigCacheEntry represents a cached S3 configuration with expiration
type s3ConfigCacheEntry struct {
	Config    *s3ConfigData
	ExpiresAt time.Time
}

// s3ConfigData holds decrypted S3 configuration data for caching
type s3ConfigData struct {
	Endpoint        string
	Region          string
	AccessKeyID     string // Decrypted
	SecretAccessKey string // Decrypted
	BucketPrefix    string
	UseSSL          bool
	ForcePathStyle  bool
}

// Global S3 config cache with 5 minute TTL (reduces database load)
var (
	s3ConfigCache    = make(map[string]*s3ConfigCacheEntry)
	s3ConfigCacheMu  sync.RWMutex
	s3ConfigCacheTTL = 5 * time.Minute
)

type BucketHandler struct {
	config        *config.Config
	policyService *services.PolicyService
	auditService  *services.AuditService
	// Test observer for the asynchronous S3 listing import. Nil in production.
	reconcileImportDone func()
}

func NewBucketHandler(cfg *config.Config) *BucketHandler {
	return &BucketHandler{
		config:        cfg,
		policyService: services.NewPolicyService(),
		auditService:  services.NewAuditService(),
	}
}

// getS3ConfigFromCache retrieves S3 config from cache if valid
func getS3ConfigFromCache(cacheKey string) (*s3ConfigData, bool) {
	s3ConfigCacheMu.RLock()
	defer s3ConfigCacheMu.RUnlock()

	entry, exists := s3ConfigCache[cacheKey]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry.Config, true
}

// setS3ConfigInCache stores S3 config in cache with TTL
func setS3ConfigInCache(cacheKey string, config *s3ConfigData) {
	s3ConfigCacheMu.Lock()
	defer s3ConfigCacheMu.Unlock()

	s3ConfigCache[cacheKey] = &s3ConfigCacheEntry{
		Config:    config,
		ExpiresAt: time.Now().Add(s3ConfigCacheTTL),
	}
}

// InvalidateS3ConfigCache invalidates cached S3 configurations (called when configs are modified)
func InvalidateS3ConfigCache() {
	s3ConfigCacheMu.Lock()
	defer s3ConfigCacheMu.Unlock()

	// Clear entire cache when any config is modified
	s3ConfigCache = make(map[string]*s3ConfigCacheEntry)
}

// getStorageBackend creates a storage backend instance based on the bucket's
// configuration: local storage, the bucket's pinned S3 configuration
// (s3_config_id), or — for an S3 bucket without one — the .env S3 settings.
func (h *BucketHandler) getStorageBackend(bucket *models.Bucket) (storage.StorageBackend, error) {
	backend := bucket.StorageBackend
	if backend == "" {
		backend = "local" // Default to local
	}

	// If not S3, return local storage
	if backend != "s3" {
		return storage.NewLocalStorage(h.config.Storage.RootPath), nil
	}

	// S3 backend. Routing is fixed per bucket: a bucket with an S3ConfigID is
	// always served by that configuration, and a bucket WITHOUT one is always
	// served by the .env S3 settings. The DB default configuration is applied
	// only when a bucket is created (its id is pinned then — see
	// CreateBucket); resolving it dynamically here would silently re-route
	// existing buckets whenever the default changed, after which the listing
	// reconcile would prune their metadata and import foreign objects.
	var endpoint, region, accessKeyID, secretAccessKey, bucketPrefix string
	var useSSL, forcePathStyle bool

	var configData *s3ConfigData
	if bucket.S3ConfigID != nil {
		cacheKey := bucket.S3ConfigID.String()
		var cacheHit bool
		configData, cacheHit = getS3ConfigFromCache(cacheKey)
		if !cacheHit {
			var s3Config models.S3Configuration
			if err := database.DB.Where("id = ?", bucket.S3ConfigID).First(&s3Config).Error; err != nil {
				// Never fall back to another backend: serving the bucket from
				// a different location would read, write and prune the wrong data.
				return nil, fmt.Errorf("S3 configuration %s of bucket %q is unavailable: %w", cacheKey, bucket.Name, err)
			}
			// Decrypt S3 credentials (they're stored encrypted for security)
			decryptedAccessKeyID, err := security.DecryptSecretKey(s3Config.AccessKeyID)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt access key ID: %w", err)
			}
			decryptedSecretAccessKey, err := security.DecryptSecretKey(s3Config.SecretAccessKey)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt secret access key: %w", err)
			}
			configData = &s3ConfigData{
				Endpoint:        s3Config.Endpoint,
				Region:          s3Config.Region,
				AccessKeyID:     decryptedAccessKeyID,
				SecretAccessKey: decryptedSecretAccessKey,
				BucketPrefix:    s3Config.BucketPrefix,
				UseSSL:          s3Config.UseSSL,
				ForcePathStyle:  s3Config.ForcePathStyle,
			}
			setS3ConfigInCache(cacheKey, configData)
		}
	} else {
		configData = &s3ConfigData{
			Endpoint:        h.config.Storage.S3.Endpoint,
			Region:          h.config.Storage.S3.Region,
			AccessKeyID:     h.config.Storage.S3.AccessKeyID,
			SecretAccessKey: h.config.Storage.S3.SecretAccessKey,
			BucketPrefix:    h.config.Storage.S3.BucketPrefix,
			UseSSL:          h.config.Storage.S3.UseSSL,
			ForcePathStyle:  h.config.Storage.S3.ForcePathStyle,
		}
	}

	// Extract values from config data
	endpoint = configData.Endpoint
	region = configData.Region
	accessKeyID = configData.AccessKeyID
	secretAccessKey = configData.SecretAccessKey
	bucketPrefix = configData.BucketPrefix
	useSSL = configData.UseSSL
	forcePathStyle = configData.ForcePathStyle

	storageBackend, err := storage.NewStorageBackend(
		backend,
		h.config.Storage.RootPath,
		endpoint,
		region,
		accessKeyID,
		secretAccessKey,
		bucketPrefix,
		useSSL,
		forcePathStyle,
		h.config.Storage.S3SSE,
	)
	if err != nil {
		// Log configuration error - don't silently fallback as this can hide issues
		logger.Warn("Failed to initialize storage backend", map[string]interface{}{
			"backend": backend,
			"bucket":  bucket.Name,
			"error":   err.Error(),
		})

		// Return error if S3 was explicitly configured for this bucket
		// Silent fallback can lead to data being written to wrong storage
		if backend == "s3" {
			return nil, fmt.Errorf("S3 storage backend configuration error: %w", err)
		}

		// Only fallback to local if backend was "local" or unspecified
		logger.Info("Falling back to local storage", map[string]interface{}{
			"bucket": bucket.Name,
		})
		return storage.NewLocalStorage(h.config.Storage.RootPath), nil
	}

	return storageBackend, nil
}

// CreateBucket creates a new bucket
// @Summary Create a bucket
// @Description Admin-only. Creates a new storage bucket. If the bucket already exists in the storage backend, it is linked rather than re-created.
// @Tags buckets
// @Accept json
// @Produce json
// @Param request body models.CreateBucketRequest true "Bucket configuration"
// @Success 201 {object} models.Bucket
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets [post]
func (h *BucketHandler) CreateBucket(c *gin.Context) {
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	var req models.CreateBucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request",
			Message: err.Error(),
		})
		return
	}

	// Validate bucket name according to S3 naming rules
	if err := validation.ValidateBucketName(req.Name); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid bucket name",
			Message: err.Error(),
		})
		return
	}

	// Validate region format
	if err := validation.ValidateRegion(req.Region); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid region",
			Message: err.Error(),
		})
		return
	}

	// Check policy permissions
	allowed, err := h.policyService.CheckBucketAccess(userUUID, req.Name, services.ActionCreateBucket)
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
			Message: "You don't have permission to create this bucket",
		})
		return
	}
	unlockName, lockErr := lockBucketName(c.Request.Context(), req.Name)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock bucket name", Message: lockErr.Error()})
		return
	}
	defer unlockName()

	// Check if bucket already exists in our database
	var existing models.Bucket
	if err := database.DB.Where("name = ?", req.Name).First(&existing).Error; err == nil {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error: "Bucket already exists in this system",
		})
		return
	}
	if bucketCleanupPending(req.Name) {
		c.JSON(http.StatusConflict, models.ErrorResponse{Error: "Bucket storage cleanup is pending"})
		return
	}

	// Create bucket struct (for storage backend check)
	bucket := models.Bucket{
		Name:           req.Name,
		OwnerID:        userUUID,
		IsPublic:       req.IsPublic,
		Region:         req.Region,
		StorageBackend: req.StorageBackend,
	}

	if bucket.Region == "" {
		bucket.Region = "us-east-1"
	}

	// Default to local storage if not specified or invalid
	if bucket.StorageBackend != "local" && bucket.StorageBackend != "s3" {
		bucket.StorageBackend = "local"
	}

	// Pin the S3 configuration now: the requested one, else the current DB
	// default, else none (= the .env S3 settings). The routing never changes
	// afterwards (see getStorageBackend).
	if bucket.StorageBackend == "s3" {
		configID, status, err := resolveNewBucketS3Config(req.S3ConfigID)
		if err != nil {
			c.JSON(status, models.ErrorResponse{
				Error:   "Invalid S3 configuration",
				Message: err.Error(),
			})
			return
		}
		bucket.S3ConfigID = configID
	}

	// Check if bucket already exists in storage backend (S3 or local)
	// If it exists and we can access it, we'll "link" to it instead of creating a new one
	var linkedToExisting bool
	storageBackend, err := h.getStorageBackend(&bucket)
	if bucket.StorageBackend == "s3" && err != nil {
		c.JSON(http.StatusBadGateway, models.ErrorResponse{Error: "Failed to resolve S3 backend", Message: err.Error()})
		return
	}
	if err == nil {
		if s3Backend, ok := storageBackend.(*storage.S3Storage); ok {
			bucket.S3RouteIdentity = s3Backend.RouteIdentity()
		}
		exists, checkErr := storageBackend.BucketExists(bucket.Name)
		if checkErr != nil {
			// Permission issue - bucket might exist but we can't access it
			c.JSON(http.StatusForbidden, models.ErrorResponse{
				Error:   "Cannot access bucket in storage backend",
				Message: checkErr.Error(),
			})
			return
		}
		linkedToExisting = exists
	}

	// Create bucket record in database
	if err := database.DB.Create(&bucket).Error; err != nil {
		// Get user info for audit log
		username, _ := c.Get("username")

		// Log failure
		_ = h.auditService.LogFailure(
			c,
			userUUID,
			username.(string),
			"CreateBucket",
			"Bucket",
			"",
			req.Name,
			err.Error(),
			map[string]interface{}{
				"bucket_name":     req.Name,
				"region":          req.Region,
				"storage_backend": req.StorageBackend,
				"is_public":       req.IsPublic,
			},
		)

		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to create bucket",
			Message: "An internal error occurred. Please try again.",
		})
		return
	}

	// If bucket doesn't exist in storage backend, create it
	if !linkedToExisting && storageBackend != nil {
		if err := storageBackend.CreateBucket(bucket.Name, bucket.Region); err != nil {
			logger.Warn("Failed to create bucket in storage backend", map[string]interface{}{
				"bucket_name":     bucket.Name,
				"storage_backend": bucket.StorageBackend,
				"error":           err.Error(),
			})
			// Roll back the record: a bucket the backend refused to create
			// (e.g. IAM without s3:CreateBucket) would fail every later write.
			if derr := database.DB.Unscoped().Delete(&bucket).Error; derr != nil {
				logger.Error("Failed to roll back bucket record", map[string]interface{}{
					"bucket_name": bucket.Name,
					"error":       derr.Error(),
				})
			}
			c.JSON(http.StatusBadGateway, models.ErrorResponse{
				Error:   "Storage backend could not create the bucket",
				Message: err.Error(),
			})
			return
		} else {
			logger.Info("Bucket created in storage backend", map[string]interface{}{
				"bucket_name":     bucket.Name,
				"storage_backend": bucket.StorageBackend,
				"region":          bucket.Region,
			})
		}
	} else if linkedToExisting {
		logger.Info("Bucket linked to existing storage backend bucket", map[string]interface{}{
			"bucket_name":     bucket.Name,
			"storage_backend": bucket.StorageBackend,
		})
	}

	// Get user info for audit log
	username, _ := c.Get("username")

	// Log success
	_ = h.auditService.LogSuccess(
		c,
		userUUID,
		username.(string),
		"CreateBucket",
		"Bucket",
		bucket.ID.String(),
		bucket.Name,
		map[string]interface{}{
			"bucket_name":        bucket.Name,
			"region":             bucket.Region,
			"storage_backend":    bucket.StorageBackend,
			"is_public":          bucket.IsPublic,
			"linked_to_existing": linkedToExisting,
		},
	)

	// Return response with indication of whether bucket was linked or created
	response := gin.H{
		"id":              bucket.ID,
		"name":            bucket.Name,
		"owner_id":        bucket.OwnerID,
		"is_public":       bucket.IsPublic,
		"region":          bucket.Region,
		"storage_backend": bucket.StorageBackend,
		"s3_config_id":    bucket.S3ConfigID,
		"created_at":      bucket.CreatedAt,
		"updated_at":      bucket.UpdatedAt,
	}

	if linkedToExisting {
		response["message"] = "Bucket linked to existing storage. Any existing contents will be accessible."
		response["linked"] = true
	}

	c.JSON(http.StatusCreated, response)
}

// resolveNewBucketS3Config picks the S3 configuration a new S3 bucket is
// pinned to: the requested id (which must exist), else the current DB default
// configuration, else nil (the .env S3 settings). On error it returns the
// HTTP status to answer with.
func resolveNewBucketS3Config(requested *string) (*uuid.UUID, int, error) {
	if requested != nil && *requested != "" {
		id, err := uuid.Parse(*requested)
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("s3_config_id is not a valid id")
		}
		var n int64
		if err := database.DB.Model(&models.S3Configuration{}).Where("id = ?", id).Count(&n).Error; err != nil {
			return nil, http.StatusInternalServerError, fmt.Errorf("failed to look up S3 configuration: %w", err)
		}
		if n == 0 {
			return nil, http.StatusBadRequest, fmt.Errorf("S3 configuration %s does not exist", id)
		}
		return &id, 0, nil
	}
	var def models.S3Configuration
	err := database.DB.Where("is_default = ?", true).Order("created_at ASC").First(&def).Error
	switch {
	case err == nil:
		return &def.ID, 0, nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		return nil, 0, nil
	default:
		// Don't guess: a bucket silently created on the .env backend instead
		// of the default configuration would stay there for good.
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to look up the default S3 configuration: %w", err)
	}
}

// ListBuckets lists accessible buckets for the authenticated user
// @Summary List buckets
// @Description Returns all buckets the authenticated user has access to. Admins see all buckets; regular users see only buckets permitted by their attached policies.
// @Tags buckets
// @Accept json
// @Produce json
// @Success 200 {array} models.Bucket
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets [get]
func (h *BucketHandler) ListBuckets(c *gin.Context) {
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)
	isAdmin, _ := c.Get("is_admin")

	var allBuckets []models.Bucket
	query := database.DB.Preload("Owner")

	// Fetch all buckets for filtering
	if err := query.Find(&allBuckets).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to fetch buckets",
			Message: err.Error(),
		})
		return
	}

	// Admin bypass - return all buckets
	if isAdmin.(bool) {
		c.JSON(http.StatusOK, allBuckets)
		return
	}

	// Use batch permission check to avoid N+1 queries (fixes CRITICAL performance issue)
	// Check if user has ANY of these common actions on each bucket
	actions := []string{
		services.ActionListBucket,
		services.ActionGetObject,
		services.ActionPutObject,
		services.ActionDeleteObject,
	}

	// Track buckets with access (use map to avoid duplicates)
	accessibleBucketMap := make(map[uuid.UUID]models.Bucket)

	// For each action, perform batch check and collect accessible buckets
	for _, action := range actions {
		bucketsWithAccess, err := h.policyService.FilterAccessibleBuckets(userUUID, allBuckets, action)
		if err != nil {
			// Log error but continue with other actions
			continue
		}
		// Add accessible buckets to map (deduplicates automatically)
		for _, bucket := range bucketsWithAccess {
			accessibleBucketMap[bucket.ID] = bucket
		}
	}

	// Convert map back to slice. Non-admins get the reduced bucket view (no
	// owner user record, storage config reference, webhook URL or replication
	// target — GET /api/buckets/:name shows the latter two to callers allowed
	// to change them).
	accessibleBuckets := make([]bucketView, 0, len(accessibleBucketMap))
	for _, bucket := range accessibleBucketMap {
		b := bucket
		accessibleBuckets = append(accessibleBuckets, newBucketView(&b, false, false))
	}
	sort.Slice(accessibleBuckets, func(i, j int) bool { return accessibleBuckets[i].Name < accessibleBuckets[j].Name })

	c.JSON(http.StatusOK, accessibleBuckets)
}

// GetBucket returns details of a specific bucket
// @Summary Get a bucket
// @Description Returns the details of a specific bucket by name. Requires admin or any of s3:ListBucket, s3:GetBucketLocation, s3:GetBucketPolicy on the bucket. Non-admins get a reduced view (no s3_config_id or owner record; webhook URL/events and replication target only with the permission to change them).
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.Bucket
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name} [get]
func (h *BucketHandler) GetBucket(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	var bucket models.Bucket
	if err := database.DB.Preload("Owner").Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Bucket not found",
		})
		return
	}

	// One batched evaluation covers both the access decision and the
	// permissions reported to the console.
	perms, err := h.policyService.CheckBucketActions(userUUID, bucketName, bucketPermissionActions)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Policy check failed",
			Message: err.Error(),
		})
		return
	}
	// Any bucket-level read grant opens the bucket details (the console's
	// Bucket settings): s3:ListBucket (e.g. the read-only template — such
	// callers already see this same reduced view in GET /api/buckets),
	// s3:GetBucketLocation, or s3:GetBucketPolicy. Admins pass every check.
	allowed := false
	for _, action := range bucketDetailsActions {
		allowed = allowed || perms[action]
	}
	if !allowed {
		c.JSON(http.StatusForbidden, models.ErrorResponse{
			Error:   "Permission denied",
			Message: "You don't have permission to access this bucket",
		})
		return
	}

	// Public-read buckets advertise the base of their unsigned object URLs
	// (<S3 endpoint>/<bucket>); the console appends the encoded key.
	publicURLBase := ""
	if bucket.IsPublic {
		publicURLBase = h.s3PublicEndpoint(c) + "/" + bucket.Name
	}

	isAdmin := c.GetBool("is_admin")
	pv := newBucketPermissionsView(perms, isAdmin)
	if isAdmin {
		c.JSON(http.StatusOK, adminBucketView{Bucket: bucket, PublicURLBase: publicURLBase, Permissions: pv})
		return
	}
	// Non-admins get the reduced view; the webhook URL (a bearer secret for
	// many receivers) and replication target are shown only to callers who
	// may change them.
	v := newBucketView(&bucket, perms[services.ActionPutBucketNotification], perms[services.ActionPutReplicationConfiguration])
	v.PublicURLBase = publicURLBase
	v.Permissions = pv
	c.JSON(http.StatusOK, v)
}

// bucketDetailsActions are the bucket-level actions any one of which lets a
// caller read GET /api/buckets/:name.
var bucketDetailsActions = []string{
	services.ActionListBucket,
	services.ActionGetBucketLocation,
	services.ActionGetBucketPolicy,
}

// bucketPermissionActions are evaluated (in one batch) by GetBucket.
var bucketPermissionActions = []string{
	services.ActionListBucket,
	services.ActionGetBucketLocation,
	services.ActionGetBucketPolicy,
	services.ActionPutBucketVersioning,
	services.ActionPutLifecycleConfiguration,
	services.ActionPutBucketQuota,
	services.ActionPutBucketObjectLockConfiguration,
	services.ActionPutBucketNotification,
	services.ActionPutReplicationConfiguration,
}

// bucketPermissionsView tells the console which bucket settings the caller
// may change, so it can render the others read-only. It is advisory: every
// write endpoint still authorizes on its own.
type bucketPermissionsView struct {
	GetPolicy       bool `json:"get_policy"`
	PutPolicy       bool `json:"put_policy"`        // admin only
	PutPublicAccess bool `json:"put_public_access"` // admin only
	PutVersioning   bool `json:"put_versioning"`
	PutLifecycle    bool `json:"put_lifecycle"`
	PutQuota        bool `json:"put_quota"`
	PutRetention    bool `json:"put_retention"`
	PutNotification bool `json:"put_notification"`
	PutReplication  bool `json:"put_replication"`
}

func newBucketPermissionsView(perms map[string]bool, isAdmin bool) *bucketPermissionsView {
	return &bucketPermissionsView{
		GetPolicy:       isAdmin || perms[services.ActionGetBucketPolicy],
		PutPolicy:       isAdmin,
		PutPublicAccess: isAdmin,
		PutVersioning:   isAdmin || perms[services.ActionPutBucketVersioning],
		PutLifecycle:    isAdmin || perms[services.ActionPutLifecycleConfiguration],
		PutQuota:        isAdmin || perms[services.ActionPutBucketQuota],
		PutRetention:    isAdmin || perms[services.ActionPutBucketObjectLockConfiguration],
		PutNotification: isAdmin || perms[services.ActionPutBucketNotification],
		PutReplication:  isAdmin || perms[services.ActionPutReplicationConfiguration],
	}
}

// adminBucketView is the full bucket record returned to admins by GetBucket,
// plus public_url_base for public-read buckets.
type adminBucketView struct {
	models.Bucket
	PublicURLBase string                 `json:"public_url_base,omitempty"`
	Permissions   *bucketPermissionsView `json:"permissions,omitempty"`
}

// DeleteBucket deletes a bucket. S3-backed buckets must have no live objects.
// @Summary Delete a bucket
// @Description Admin-only. Local buckets are deleted with their contents. S3-backed buckets require no live objects, versions, or uploads. Only BKT hidden orphan blobs are reclaimed; public upstream keys and native versioning require manual cleanup.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.SuccessResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name} [delete]
func (h *BucketHandler) DeleteBucket(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)
	username, _ := c.Get("username")

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Bucket not found",
		})
		return
	}

	// Check policy permissions
	allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionDeleteBucket)
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
			Message: "You don't have permission to delete this bucket",
		})
		return
	}

	// Object mutations hold a shared advisory lock on the bucket for their
	// full write/commit sequence. Deletion takes the exclusive side before it
	// enumerates or removes bytes, so no replica can write through this sweep.
	unlockBucket, lockErr := lockBucketExclusive(c.Request.Context(), &bucket)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock bucket", Message: lockErr.Error()})
		return
	}
	defer unlockBucket()
	if err := refreshLockedBucket(c.Request.Context(), &bucket); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to reload bucket", Message: err.Error()})
		return
	}

	// Get storage backend for this bucket
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to get storage backend",
			Message: err.Error(),
		})
		return
	}

	// Get all objects in the bucket
	// WORM: a bucket with retention still covering any object or version
	// cannot be deleted (deleting the bucket would destroy retained data).
	if bucket.RetentionDays > 0 {
		cutoff := time.Now().AddDate(0, 0, -bucket.RetentionDays)
		var retained int64
		database.DB.Model(&models.Object{}).Where("bucket_id = ? AND updated_at > ?", bucket.ID, cutoff).Count(&retained)
		var retainedVers int64
		database.DB.Model(&models.ObjectVersion{}).Where("bucket_id = ? AND is_delete_marker = false AND content_modified_at > ?", bucket.ID, cutoff).Count(&retainedVers)
		if retained+retainedVers > 0 {
			c.JSON(http.StatusConflict, models.ErrorResponse{
				Error:   "Bucket under retention",
				Message: fmt.Sprintf("%d object(s)/version(s) are still within the %d-day retention period", retained+retainedVers, bucket.RetentionDays),
			})
			return
		}
	}

	var objects []models.Object
	if err := database.DB.Where("bucket_id = ?", bucket.ID).Find(&objects).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to list bucket objects",
			Message: err.Error(),
		})
		return
	}

	// S3's own DeleteBucket checks whether the bucket is physically empty.
	// Never sweep it while metadata still references bytes: a later refusal
	// would otherwise leave live rows pointing at deleted generations. When
	// only unreferenced blobs remain, they can be removed safely and the
	// upstream delete retried under this bucket's exclusive lock.
	// Local storage is removed after the DB commit; a durable cleanup record
	// blocks re-linking leftover bytes if removal fails or the process stops.
	if bucket.StorageBackend == "s3" {
		s3Backend, ok := storageBackend.(*storage.S3Storage)
		if !ok || bucket.S3RouteIdentity == "" || bucket.S3RouteIdentity != s3Backend.RouteIdentity() {
			c.JSON(http.StatusConflict, models.ErrorResponse{Error: "S3 bucket route is unverified", Message: "The upstream route changed or predates route pinning; verify the original location before deleting this bucket"})
			return
		}
		var contentVersions, activeUploads int64
		if err := database.DB.Model(&models.ObjectVersion{}).
			Where("bucket_id = ? AND is_delete_marker = false", bucket.ID).Count(&contentVersions).Error; err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to check bucket versions", Message: err.Error()})
			return
		}
		if err := database.DB.Model(&models.MultipartUpload{}).
			Where("bucket_name = ? AND status = ?", bucket.Name, "in-progress").Count(&activeUploads).Error; err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to check bucket uploads", Message: err.Error()})
			return
		}
		if len(objects) != 0 || contentVersions != 0 || activeUploads != 0 {
			c.JSON(http.StatusConflict, models.ErrorResponse{Error: "Bucket is not empty", Message: "Delete objects, versions, and in-progress uploads before deleting this S3 bucket"})
			return
		}
		if err := storageBackend.DeleteBucket(bucketName); err != nil {
			if !storage.IsS3BucketNotEmpty(err) {
				c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to delete bucket from storage", Message: err.Error()})
				return
			}
			s3Backend, ok := storageBackend.(*storage.S3Storage)
			if !ok {
				c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to empty S3 bucket"})
				return
			}
			if err := s3Backend.EmptyBucket(bucketName); err != nil {
				status := http.StatusInternalServerError
				if errors.Is(err, storage.ErrS3BucketContainsPublicObjects) || errors.Is(err, storage.ErrS3BucketUsesNativeVersioning) {
					status = http.StatusConflict
				}
				c.JSON(status, models.ErrorResponse{Error: "Failed to empty S3 bucket", Message: err.Error()})
				return
			}
			if err := storageBackend.DeleteBucket(bucketName); err != nil {
				status := http.StatusInternalServerError
				if storage.IsS3BucketNotEmpty(err) {
					status = http.StatusConflict
				}
				c.JSON(status, models.ErrorResponse{Error: "Failed to delete bucket from storage", Message: err.Error()})
				return
			}
		}
	}

	var localCleanup *models.LocalBucketCleanup
	if bucket.StorageBackend != "s3" {
		root, err := filepath.Abs(h.config.Storage.RootPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to resolve local storage root", Message: err.Error()})
			return
		}
		rootID, err := ensureLocalRootIdentity(root)
		if err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to identify local storage root", Message: err.Error()})
			return
		}
		localCleanup = &models.LocalBucketCleanup{ID: bucket.ID, Name: bucket.Name, RootPath: root, RootIdentity: rootID}
	}
	// Use transaction to delete all objects and the bucket from database
	err = database.DB.Transaction(func(tx *gorm.DB) error {
		if localCleanup != nil {
			if err := tx.Create(localCleanup).Error; err != nil {
				return fmt.Errorf("failed to record local bucket cleanup: %w", err)
			}
		}
		// Delete all objects and version history from database (history
		// can exist even when no current objects remain).
		if err := tx.Where("bucket_id = ?", bucket.ID).Delete(&models.ObjectVersion{}).Error; err != nil {
			return err
		}
		if err := tx.Where("bucket_id = ?", bucket.ID).Delete(&models.Object{}).Error; err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
		if err := tx.Where("bucket_name = ?", bucket.Name).Delete(&models.MultipartUpload{}).Error; err != nil {
			return fmt.Errorf("failed to delete multipart upload records: %w", err)
		}

		// Delete any bucket policies
		if err := tx.Where("bucket_id = ?", bucket.ID).Delete(&models.BucketPolicy{}).Error; err != nil {
			return fmt.Errorf("failed to delete bucket policies: %w", err)
		}

		// Disable replication INTO this bucket. A dangling replicate_to would
		// otherwise target a later bucket re-created under the same name
		// (possibly by another owner), overwriting and mirror-deleting its
		// objects.
		if err := tx.Model(&models.Bucket{}).
			Where("id <> ? AND (replicate_to = ? OR replicate_to_id = ?)", bucket.ID, bucket.Name, bucket.ID).
			Updates(map[string]interface{}{
				"replicate_to":              "",
				"replicate_to_id":           nil,
				"replication_configured_by": nil,
				"replication_configured_at": nil,
			}).Error; err != nil {
			return fmt.Errorf("failed to clear replication targeting the bucket: %w", err)
		}

		// Delete the bucket
		if err := tx.Delete(&bucket).Error; err != nil {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		return nil
	})

	if err != nil {
		// Log failure
		_ = h.auditService.LogFailure(
			c,
			userUUID,
			username.(string),
			"DeleteBucket",
			"Bucket",
			bucket.ID.String(),
			bucket.Name,
			err.Error(),
			map[string]interface{}{
				"bucket_name":   bucket.Name,
				"owner_id":      bucket.OwnerID.String(),
				"objects_count": len(objects),
			},
		)

		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to delete bucket",
			Message: err.Error(),
		})
		return
	}
	if localCleanup != nil {
		if err := cleanupLocalBucketLocked(localCleanup); err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to delete bucket from storage", Message: err.Error()})
			return
		}
	}

	// Log success
	_ = h.auditService.LogSuccess(
		c,
		userUUID,
		username.(string),
		"DeleteBucket",
		"Bucket",
		bucket.ID.String(),
		bucket.Name,
		map[string]interface{}{
			"bucket_name":     bucket.Name,
			"owner_id":        bucket.OwnerID.String(),
			"objects_deleted": len(objects),
		},
	)

	c.JSON(http.StatusOK, models.SuccessResponse{
		Message: fmt.Sprintf("Bucket deleted successfully (%d objects removed)", len(objects)),
	})
}

func bucketCleanupPending(name string) bool {
	var count int64
	// A lookup failure must not let CreateBucket link unknown local bytes.
	return database.DB.Model(&models.LocalBucketCleanup{}).Where("name = ?", name).Count(&count).Error != nil || count != 0
}

const localRootIdentityFile = ".bkt-root-id"

func readLocalRootIdentity(root string) (string, error) {
	contents, err := os.ReadFile(filepath.Join(root, localRootIdentityFile))
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(contents))
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("invalid local root identity: %w", err)
	}
	return id, nil
}

func ensureLocalRootIdentity(root string) (string, error) {
	if id, err := readLocalRootIdentity(root); err == nil {
		return id, syncLocalRootDir(root)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	id := uuid.NewString()
	f, err := os.CreateTemp(root, ".bkt-root-id-tmp-")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(id + "\n"); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Link(f.Name(), filepath.Join(root, localRootIdentityFile)); err != nil {
		if os.IsExist(err) {
			id, err := readLocalRootIdentity(root)
			if err != nil {
				return "", err
			}
			return id, syncLocalRootDir(root)
		}
		return "", err
	}
	if err := syncLocalRootDir(root); err != nil {
		return "", err
	}
	return id, nil
}

func syncLocalRootDir(root string) error {
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

var removeLocalBucket = func(rootPath, name string) error {
	return storage.NewLocalStorage(rootPath).DeleteBucket(name)
}

// cleanupLocalBucketLocked must be called while holding the bucket-name
// advisory lock. A worker that took an old snapshot must not remove bytes
// after another worker cleared its tombstone and CreateBucket reused the name.
func cleanupLocalBucketLocked(snapshot *models.LocalBucketCleanup) error {
	var entry models.LocalBucketCleanup
	err := database.DB.Where("id = ? AND name = ?", snapshot.ID, snapshot.Name).First(&entry).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if entry.RootPath != snapshot.RootPath || entry.RootIdentity != snapshot.RootIdentity {
		return fmt.Errorf("local bucket %q cleanup record changed", snapshot.Name)
	}
	var live int64
	if err := database.DB.Model(&models.Bucket{}).Where("name = ?", entry.Name).Count(&live).Error; err != nil {
		return err
	}
	if live != 0 {
		return fmt.Errorf("local bucket %q was recreated before cleanup", entry.Name)
	}
	rootID, err := readLocalRootIdentity(entry.RootPath)
	if err != nil {
		return fmt.Errorf("failed to verify local storage root: %w", err)
	}
	if rootID != entry.RootIdentity {
		return fmt.Errorf("local storage root identity changed for bucket %q", entry.Name)
	}
	if err := removeLocalBucket(entry.RootPath, entry.Name); err != nil {
		return err
	}
	return database.DB.Where("id = ? AND name = ?", entry.ID, entry.Name).Delete(&models.LocalBucketCleanup{}).Error
}

// ReconcileLocalBucketCleanups retries durable cleanup after a crash or
// filesystem failure. Only the configured root path is touched; a changed
// root identity fails closed and leaves its cleanup record for recovery.
func ReconcileLocalBucketCleanups(rootPath string) error {
	root, err := filepath.Abs(rootPath)
	if err != nil {
		return err
	}
	var entries []models.LocalBucketCleanup
	if err := database.DB.Find(&entries).Error; err != nil {
		return err
	}
	for i := range entries {
		if entries[i].RootPath != root {
			continue
		}
		unlock, err := lockBucketName(context.Background(), entries[i].Name)
		if err != nil {
			return err
		}
		err = cleanupLocalBucketLocked(&entries[i])
		unlock()
		if err != nil {
			return fmt.Errorf("local bucket %q cleanup: %w", entries[i].Name, err)
		}
	}
	return nil
}

// SetBucketPolicy sets an access policy on a bucket
// @Summary Set bucket policy
// @Description Admin-only. Sets an S3-style access policy document on the specified bucket (strictly validated; the same JSON is returned by GET). Audit-logged as bucket.policy.set.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Param request body object true "Bucket policy document" SchemaExample({"policy":"{\"Version\":\"2012-10-17\",...}"})
// @Success 200 {object} models.SuccessResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/policy [put]
func (h *BucketHandler) SetBucketPolicy(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Check policy permissions - must have PutBucketPolicy permission
	allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionPutBucketPolicy)
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
			Message: "You don't have permission to set bucket policy",
		})
		return
	}

	var req struct {
		Policy string `json:"policy" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request",
			Message: err.Error(),
		})
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}

	// Set bucket policy using the service (strict validation)
	previous, _ := h.policyService.GetBucketPolicy(bucketName)
	if err := h.policyService.SetBucketPolicy(bucketName, req.Policy); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Failed to set bucket policy",
			Message: err.Error(),
		})
		return
	}
	auditBucketPolicyChange(c, userUUID, c.GetString("username"), &bucket, "console", previous, &req.Policy)

	c.JSON(http.StatusOK, models.SuccessResponse{
		Message: "Bucket policy set successfully",
	})
}

// DeleteBucketPolicy removes a bucket's access policy
// @Summary Delete bucket policy
// @Description Admin-only. Removes the bucket policy (idempotent: succeeds when the bucket has none). Audit-logged as bucket.policy.delete.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} models.SuccessResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/policy [delete]
func (h *BucketHandler) DeleteBucketPolicy(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Admin only (AdminMiddleware on the route); re-checked against the
	// database so a just-demoted admin is refused.
	allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionPutBucketPolicy)
	if err != nil || !allowed || !c.GetBool("is_admin") {
		c.JSON(http.StatusForbidden, models.ErrorResponse{
			Error:   "Permission denied",
			Message: "Only an admin can delete a bucket policy",
		})
		return
	}

	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "Bucket not found"})
		return
	}
	previous, _ := h.policyService.GetBucketPolicy(bucketName)
	if err := h.policyService.DeleteBucketPolicy(bucketName); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to delete bucket policy",
			Message: err.Error(),
		})
		return
	}
	if previous != nil {
		auditBucketPolicyChange(c, userUUID, c.GetString("username"), &bucket, "console", previous, nil)
	}
	c.JSON(http.StatusOK, models.SuccessResponse{Message: "Bucket policy deleted"})
}

// GetBucketPolicy retrieves the access policy for a bucket
// @Summary Get bucket policy
// @Description Returns the S3-style access policy document for the specified bucket. Requires admin or s3:GetBucketPolicy on the bucket. 404 when the bucket has no policy.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Success 200 {object} object "Bucket policy document"
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/policy [get]
func (h *BucketHandler) GetBucketPolicy(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Check policy permissions - must have GetBucketPolicy permission
	allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionGetBucketPolicy)
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
			Message: "You don't have permission to get bucket policy",
		})
		return
	}

	// Get bucket policy using the service
	bucketPolicy, err := h.policyService.GetBucketPolicy(bucketName)
	if err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error:   "Bucket policy not found",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"policy": bucketPolicy.PolicyDocument,
	})
}

// ListObjects lists objects in a bucket
// @Summary List objects in a bucket
// @Description Returns objects stored in the specified bucket. Supports prefix filtering and pagination. For S3-backed buckets, automatically syncs new and removed objects from the storage backend.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Param prefix query string false "Filter objects by key prefix"
// @Param max-keys query string false "Maximum number of objects to return (default 1000, max 1000)"
// @Success 200 {object} object "List of objects with count"
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects [get]
func (h *BucketHandler) ListObjects(c *gin.Context) {
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

	// Check policy permissions
	allowed, err := h.policyService.CheckBucketAccess(userUUID, bucketName, services.ActionListBucket)
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
			Message: "You don't have permission to list objects in this bucket",
		})
		return
	}

	// Query parameters for pagination and filtering
	prefix := c.DefaultQuery("prefix", "")
	maxKeys := 1000
	if mk := c.Query("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil && parsed > 0 && parsed <= 1000 {
			maxKeys = parsed
		}
	}
	// Keyset pagination: the continuation token is the last key of the previous
	// page; the next page is everything strictly after it in key order.
	continuationToken := c.Query("continuation-token")

	// Get objects from database
	query := database.DB.Where("bucket_id = ?", bucket.ID)
	if prefix != "" {
		// Escape LIKE wildcards to prevent SQL injection via prefix parameter
		escapedPrefix := validation.EscapeLikeWildcards(prefix)
		query = query.Where("key LIKE ?", escapedPrefix+"%")
	}
	if continuationToken != "" {
		query = query.Where("key > ?", continuationToken)
	}

	// Fetch one extra row to learn whether more pages exist.
	var objects []models.Object
	if err := query.Limit(maxKeys + 1).Order("key ASC").Find(&objects).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to list objects",
			Message: err.Error(),
		})
		return
	}
	dbTruncated := len(objects) > maxKeys
	if dbTruncated {
		objects = objects[:maxKeys]
	}
	// Continuation basis if reconciliation below prunes the whole page.
	lastExaminedKey := ""
	if len(objects) > 0 {
		lastExaminedKey = objects[len(objects)-1].Key
	}

	// Sync with actual storage backend (S3 or local)
	// This handles both:
	// 1. Removing stale DB entries (objects deleted directly from S3)
	// 2. Adding new entries (objects in S3 that aren't in DB, e.g., from linked buckets)
	if bucket.StorageBackend == "s3" {
		storageBackend, err := h.getStorageBackend(&bucket)
		if err == nil {
			// Get actual objects from S3
			s3Objects, err := storageBackend.ListObjects(bucketName, prefix)
			if err == nil {
				// Build maps for comparison
				s3KeysMap := make(map[string]storage.ObjectInfo)
				for _, obj := range s3Objects {
					s3KeysMap[obj.Key] = obj
				}

				dbKeysMap := make(map[string]bool)
				for _, obj := range objects {
					dbKeysMap[obj.Key] = true
				}

				// Decide which of this page's rows are stale (absent from
				// the backend listing) BEFORE importing anything: when most
				// rows look deleted, the far likelier explanation is that the
				// bucket is being served from the wrong backend location, and
				// neither pruning nor importing may happen.
				// CRITICAL: only treat "absent from the S3 listing" as "deleted
				// from S3" when the listing is known COMPLETE. The backend caps
				// its listing at 10,000 objects; if we hit that cap the listing
				// is truncated and an absent key may simply be beyond the window.
				listingComplete := len(s3Objects) < storage.MaxListObjects
				plan := planReconcilePrune(objects, s3KeysMap, listingComplete, time.Now())
				if plan.suspicious {
					logger.Warn("Listing reconcile skipped: most metadata rows are missing from the storage backend (possible misrouted backend); not pruning or importing", map[string]interface{}{
						"bucket":       bucketName,
						"prefix":       prefix,
						"rows":         len(objects),
						"missing_rows": plan.missing,
					})
				}

				// Find objects in S3 but not in database (need to add)
				// Limit sync to prevent overwhelming DB on huge buckets
				// Users can paginate/navigate to sync more incrementally
				const maxSyncPerRequest = 1000
				pageRows := len(objects)
				newObjects := make([]models.Object, 0)
				for key, s3Obj := range s3KeysMap {
					if plan.suspicious {
						break
					}
					if !dbKeysMap[key] {
						// Parse LastModified time
						lastModified := time.Now()
						if s3Obj.LastModified != "" {
							if parsed, err := time.Parse(time.RFC3339, s3Obj.LastModified); err == nil {
								lastModified = parsed
							}
						}

						newObjects = append(newObjects, models.Object{
							BucketID:    bucket.ID,
							Key:         key,
							Size:        s3Obj.Size,
							ContentType: s3Obj.ContentType,
							ETag:        s3Obj.ETag,
							StoragePath: key,
							CreatedAt:   lastModified,
							UpdatedAt:   lastModified,
						})

						// Cap sync to avoid memory/DB overload on massive buckets
						if len(newObjects) >= maxSyncPerRequest {
							break
						}
					}
				}

				// Recheck each candidate under the same key lock as deletes.
				// The listing may have seen a legacy public key just before its
				// metadata was removed; an unlocked background insert could then
				// resurrect it after the physical delete.
				importDone := h.reconcileImportDone
				if len(newObjects) > 0 {
					go func(objs []models.Object, listedBucket models.Bucket) {
						if importDone != nil {
							defer importDone()
						}
						for _, obj := range objs {
							ctx, cancel := context.WithTimeout(context.Background(), reconcilePruneLockWait)
							unlock, err := lockObjectKeysChecked(ctx, &listedBucket, obj.Key)
							cancel()
							if err != nil {
								continue // a later listing can retry the import
							}
							if _, err := storageBackend.GetObjectInfo(listedBucket.Name, obj.Key); err == nil {
								database.DB.Exec(`
								INSERT INTO objects (id, bucket_id, key, size, content_type, e_tag, storage_path, sha256, created_at, updated_at)
								VALUES (gen_random_uuid(), ?, ?, ?, ?, ?, ?, '', ?, ?)
								ON CONFLICT (bucket_id, key) DO NOTHING`,
									obj.BucketID, obj.Key, obj.Size, obj.ContentType, obj.ETag, obj.StoragePath, obj.CreatedAt, obj.UpdatedAt)
							}
							unlock()
						}
					}(newObjects, bucket)

					// Add to response immediately (don't wait for DB), but only
					// keys inside this page's range — keys at or before the
					// continuation token belong to earlier pages and would
					// duplicate entries the client already has.
					for _, obj := range newObjects {
						if continuationToken == "" || obj.Key > continuationToken {
							objects = append(objects, obj)
						}
					}
				} else if importDone != nil {
					importDone()
				}

				// Prune stale rows in the background, each under its key's
				// write lock and only if the row is unchanged since this
				// listing — so a row being overwritten right now (bytes briefly
				// absent between archive and write) is never pruned.
				if len(plan.stale) > 0 {
					go pruneStaleObjectRows(&bucket, plan.stale)
				}

				// Keep the page's non-stale rows plus the imported ones.
				objects = append(plan.keep, objects[pageRows:]...)
			}
		}
	}

	// Re-establish page invariants after the reconcile above may have added
	// (S3 sync) or removed (stale prune) entries: sorted by key, at most
	// maxKeys rows, with a token pointing just past the last row we cover.
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	isTruncated := dbTruncated
	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
		isTruncated = true
	}
	nextToken := ""
	if isTruncated {
		if len(objects) > 0 {
			nextToken = objects[len(objects)-1].Key
		} else {
			// The whole page was pruned as stale but more DB rows exist beyond
			// it — continue from the last key this page examined.
			nextToken = lastExaminedKey
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"bucket":                  bucketName,
		"objects":                 objects,
		"count":                   len(objects),
		"is_truncated":            isTruncated,
		"next_continuation_token": nextToken,
	})
}

// Reconcile pruning bounds (see planReconcilePrune).
const (
	// reconcilePruneMinAge: rows written this recently are never pruned — the
	// backend listing may predate the write, or the key may be mid-overwrite.
	reconcilePruneMinAge = 5 * time.Minute
	// Safety valve: when more than reconcileSuspiciousMin rows AND more than
	// half of the page's rows are missing from the backend, the bucket is far
	// more likely served from the wrong location than emptied out of band.
	reconcileSuspiciousMin = 10
	// reconcilePruneLockWait bounds the wait for a key's write lock.
	reconcilePruneLockWait = 2 * time.Second
)

// reconcilePrunePlan is the outcome of planReconcilePrune.
type reconcilePrunePlan struct {
	keep       []models.Object // rows to keep in the listing
	stale      []models.Object // rows to prune
	missing    int             // rows absent from the backend listing
	suspicious bool            // safety valve tripped: prune (and import) nothing
}

// planReconcilePrune partitions a page of persisted metadata rows against the
// backend listing. A row is stale only when the listing is complete, the key
// is absent from it, and the row was not updated within reconcilePruneMinAge.
// If too many rows are missing (see reconcileSuspiciousMin) nothing is stale.
func planReconcilePrune(rows []models.Object, listed map[string]storage.ObjectInfo, listingComplete bool, now time.Time) reconcilePrunePlan {
	plan := reconcilePrunePlan{keep: make([]models.Object, 0, len(rows))}
	persisted := 0
	for _, obj := range rows {
		if obj.ID == uuid.Nil {
			plan.keep = append(plan.keep, obj)
			continue
		}
		// Committed generations live in the private physical keyspace, which
		// ListObjects intentionally hides. Absence of the public key cannot
		// imply this metadata row is stale.
		if obj.GenerationID != "" {
			plan.keep = append(plan.keep, obj)
			continue
		}
		persisted++
		if _, ok := listed[obj.Key]; ok {
			plan.keep = append(plan.keep, obj)
			continue
		}
		plan.missing++
		if !listingComplete || now.Sub(obj.UpdatedAt) < reconcilePruneMinAge {
			plan.keep = append(plan.keep, obj)
			continue
		}
		plan.stale = append(plan.stale, obj)
	}
	if plan.missing > reconcileSuspiciousMin && plan.missing*2 > persisted {
		plan.suspicious = true
		plan.keep = append(plan.keep, plan.stale...)
		plan.stale = nil
		sort.Slice(plan.keep, func(i, j int) bool { return plan.keep[i].Key < plan.keep[j].Key })
	}
	return plan
}

// pruneStaleObjectRows deletes metadata rows whose objects vanished from the
// backend. Each delete runs under the key's write lock (skipped if the lock is
// busy) and only matches the row as it was listed (same id and updated_at),
// so a concurrent overwrite, delete or move of the key always wins.
func pruneStaleObjectRows(bucket *models.Bucket, stale []models.Object) {
	for _, obj := range stale {
		unlock, ok := tryLockObjectKeysChecked(bucket, reconcilePruneLockWait, obj.Key)
		if !ok {
			continue
		}
		res := database.DB.Where("id = ? AND updated_at = ? AND generation_id = ''", obj.ID, obj.UpdatedAt).Delete(&models.Object{})
		unlock()
		if res.Error != nil {
			logger.Warn("Failed to prune stale object row", map[string]interface{}{
				"bucket": bucket.Name, "key": obj.Key, "error": res.Error.Error(),
			})
		}
	}
}

// UploadObject uploads an object to a bucket
// @Summary Upload an object
// @Description Uploads a file to the specified bucket. The content type is detected from file magic bytes. Requires PutObject permission.
// @Tags buckets
// @Accept multipart/form-data
// @Produce json
// @Param name path string true "Bucket name"
// @Param key formData string true "Object key (path within bucket)"
// @Param file formData file true "File to upload"
// @Success 200 {object} object "Upload result with ETag and content type"
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 413 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects [post]
func (h *BucketHandler) UploadObject(c *gin.Context) {
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

	// Cap the request body BEFORE anything parses the multipart form:
	// c.PostForm / c.FormFile spool the entire upload to memory/temp disk, so
	// the MaxFileSize check on the parsed file header came too late.
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

	// S3 key rules (length, UTF-8, NUL) and the reserved version keyspace.
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

	// Validate file size (prevent edge cases and resource abuse)
	if fileHeader.Size < 0 {
		// Negative size is invalid (should never happen, but check for safety)
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

	// Warn about suspiciously large files even if under limit (potential resource abuse)
	// 1GB threshold for warning (could indicate accidental large file upload)
	if fileHeader.Size > 1*1024*1024*1024 {
		// Log warning but allow upload (admin may want to review)
		logger.Warn("Large file upload detected", map[string]interface{}{
			"object_key": objectKey,
			"size_bytes": fileHeader.Size,
			"size_mb":    fileHeader.Size / (1024 * 1024),
		})
	}

	// Open uploaded file
	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to open file",
			Message: err.Error(),
		})
		return
	}
	defer file.Close() //nolint:errcheck // best-effort close of read-only file

	// Detect actual content type from file magic numbers (don't trust client)
	detectedType, firstBytes, err := validation.DetectContentType(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to detect content type",
			Message: err.Error(),
		})
		return
	}

	// Validate content type is safe
	if !validation.IsSafeContentType(detectedType) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Forbidden file type",
			Message: fmt.Sprintf("File type '%s' is not allowed", detectedType),
		})
		return
	}

	// Use detected content type (from magic numbers, not from client header)
	contentType := detectedType

	// The form file is already spooled server-side (memory or temp file) and
	// seekable: rewind it rather than re-joining the sniffed bytes, so the
	// body stays seekable (the S3 SDK can rewind it on retries) and its size
	// is exact, as for async uploads.
	var body io.Reader
	if _, err := file.Seek(0, io.SeekStart); err == nil {
		body = file
	} else {
		body = io.MultiReader(bytes.NewReader(firstBytes), file)
	}

	// Get storage backend for this bucket
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to initialize storage backend",
			Message: err.Error(),
		})
		return
	}

	// Store with a timeout (prevents indefinite blocking on large uploads).
	// The whole lock → quota → archive → write → metadata sequence runs in
	// the goroutine, so a timed-out request never leaves half-committed
	// state: the write either completes fully in the background or (when the
	// deferred file.Close interrupts its read) rolls back.
	uploadTimeout := 10 * time.Minute
	ctx, cancel := context.WithTimeout(c.Request.Context(), uploadTimeout)
	defer cancel()

	type uploadResult struct {
		obj    *models.Object
		status int
		err    error
	}
	resultChan := make(chan uploadResult, 1)
	go func() {
		obj, status, err := h.storeObject(ctx, storageBackend, &bucket, objectKey, body, fileHeader.Size, contentType, "")
		resultChan <- uploadResult{obj: obj, status: status, err: err}
	}()

	var object *models.Object
	select {
	case result := <-resultChan:
		if result.err != nil {
			title := "Failed to save object"
			if result.status == http.StatusForbidden {
				title = "Quota exceeded"
			}
			c.JSON(result.status, models.ErrorResponse{
				Error:   title,
				Message: result.err.Error(),
			})
			return
		}
		object = result.obj
	case <-ctx.Done():
		c.JSON(http.StatusRequestTimeout, models.ErrorResponse{
			Error:   "Upload timeout",
			Message: fmt.Sprintf("Upload exceeded timeout of %v", uploadTimeout),
		})
		return
	}

	notifyObjectEvent(&bucket, services.EventObjectCreated, objectKey, object.Size, object.ETag, object.VersionID)

	c.JSON(http.StatusOK, gin.H{
		"message":      "Object uploaded successfully",
		"bucket":       bucketName,
		"key":          objectKey,
		"size":         object.Size,
		"etag":         object.ETag,
		"content_type": object.ContentType,
	})
}

// multipartBodyOverhead is the allowance on top of MaxFileSize for multipart
// framing and the small form fields that accompany a console upload.
const multipartBodyOverhead = 1 << 20

// limitMultipartBody bounds a console (multipart/form-data) upload request to
// MaxFileSize plus framing overhead and parses the form under that bound. It
// writes the error response and returns false when the body is too large or
// not a valid multipart form.
//
// The whole body is received (spooled to memory/temp disk) here, BEFORE the
// handler takes the object's write lock, so a slow client never holds the
// lock; the upload inactivity bound (applyUploadIdleTimeout) keeps a stalled
// client from tying up the request and its spool file indefinitely.
func (h *BucketHandler) limitMultipartBody(c *gin.Context) bool {
	applyUploadIdleTimeout(c)
	if max := h.config.Storage.MaxFileSize; max > 0 {
		limit := max + multipartBodyOverhead
		if c.Request.ContentLength > limit {
			c.JSON(http.StatusRequestEntityTooLarge, models.ErrorResponse{
				Error:   "File too large",
				Message: fmt.Sprintf("Maximum file size is %d bytes", max),
			})
			return false
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	}
	if _, err := c.MultipartForm(); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) || strings.Contains(err.Error(), "request body too large") {
			c.JSON(http.StatusRequestEntityTooLarge, models.ErrorResponse{
				Error:   "File too large",
				Message: fmt.Sprintf("Maximum file size is %d bytes", h.config.Storage.MaxFileSize),
			})
			return false
		}
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid upload",
			Message: "Request must be a multipart/form-data upload with a 'file' field",
		})
		return false
	}
	return true
}

// storeObject writes a console upload (sync or async) into a private immutable
// generation, then publishes the current pointer and prior-version row in one
// database transaction. An interrupted write leaves only an orphan stage;
// the previously committed bytes and metadata stay readable.
func (h *BucketHandler) storeObject(ctx context.Context, backend storage.StorageBackend, bucket *models.Bucket, key string, data io.Reader, size int64, contentType, sha256Hex string) (*models.Object, int, error) {
	generationBackend, ok := backend.(storage.GenerationStorage)
	if !ok {
		return nil, http.StatusInternalServerError, fmt.Errorf("storage backend does not support immutable generations")
	}
	unlock, lockErr := lockObjectKeysChecked(ctx, bucket, key)
	if lockErr != nil {
		return nil, http.StatusInternalServerError, lockErr
	}
	defer unlock()

	reservation, qerr := reserveBucketQuota(bucket, size)
	if qerr != nil {
		return nil, http.StatusForbidden, qerr
	}
	defer reservation.release()

	// Stream sources are verified (exact length, size cap, quota) before the
	// backend sees their last byte. A seekable source is a server-side temp
	// file of known size (async uploads); it is passed through as-is so the
	// S3 SDK can rewind it on retries.
	var body *guardedBody
	src := data
	if _, seekable := data.(io.ReadSeeker); !seekable {
		body = newGuardedBody(data, size, h.config.Storage.MaxFileSize, reservation)
		src = body
	}

	generationID := uuid.NewString()
	info, putErr := generationBackend.PutGeneration(bucket.Name, key, generationID, src, size, contentType, nil)
	if putErr != nil || (body != nil && !body.Complete()) {
		if putErr == nil {
			// The backend reported success without consuming the verified
			// body: treat the bytes as uncommitted.
			_ = generationBackend.DeleteGeneration(bucket.Name, key, generationID)
			return nil, http.StatusInternalServerError, fmt.Errorf("storage backend did not consume the full upload")
		}
		if body != nil {
			if berr := body.Err(); berr != nil {
				var qe *quotaExceededError
				switch {
				case errors.As(berr, &qe):
					return nil, http.StatusForbidden, qe
				case errors.Is(berr, errBodyTooLarge):
					return nil, http.StatusRequestEntityTooLarge, berr
				default:
					return nil, http.StatusBadRequest, berr
				}
			}
		}
		return nil, http.StatusInternalServerError, putErr
	}

	obj := &models.Object{
		BucketID:     bucket.ID,
		Key:          key,
		Size:         info.Size,
		ContentType:  contentType,
		ETag:         info.ETag,
		SHA256:       sha256Hex,
		StoragePath:  key,
		GenerationID: generationID,
		VersionID:    newCurrentVersionID(bucket),
	}
	generationCommitBoundary("console")
	if err := commitStagedGeneration(generationBackend, bucket, obj); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to save object metadata: %w", err)
	}
	return obj, http.StatusOK, nil
}

// DownloadObject downloads an object from a bucket
// @Summary Download an object
// @Description Streams the content of an object from the specified bucket. Add ?download=true to force a download attachment. Requires GetObject permission.
// @Tags buckets
// @Produce application/octet-stream
// @Param name path string true "Bucket name"
// @Param key path string true "Object key"
// @Param download query string false "Set to 'true' to force download attachment"
// @Success 200 {file} binary "Object content"
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/{key} [get]
func (h *BucketHandler) DownloadObject(c *gin.Context) {
	bucketName := c.Param("name")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	if validation.IsReservedObjectKey(objectKey) {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Object not found",
		})
		return
	}
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

	// Check policy permissions
	allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionGetObject)
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
			Message: "You don't have permission to download this object",
		})
		return
	}

	// Get object metadata from database
	var object models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Object not found",
		})
		return
	}

	// Get storage backend for this bucket
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to initialize storage backend",
			Message: err.Error(),
		})
		return
	}

	// Set response headers
	c.Header("Content-Type", object.ContentType)
	c.Header("ETag", fmt.Sprintf("\"%s\"", object.ETag))
	c.Header("Last-Modified", object.UpdatedAt.UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")

	// Set content disposition based on query parameter
	if c.Query("download") == "true" {
		c.Header("Content-Disposition", attachmentDisposition(objectKey))
	} else {
		c.Header("Content-Disposition", "inline")
	}

	// Honor Range requests so browsers can seek media and resume downloads.
	// We advertise Accept-Ranges, so a 200-with-full-body response to a range
	// request corrupts the client's reassembled file.
	if start, length, ok, satisfiable := parseRange(c.GetHeader("Range"), object.Size); ok {
		if !satisfiable {
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", object.Size))
			c.Status(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		// Native ranged read (S3 Range GET / file seek) — no streaming and
		// discarding of the bytes before the range start.
		rangeReader, err := getCurrentRange(storageBackend, bucketName, &object, start, length)
		if err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to read object range", Message: err.Error()})
			return
		}
		defer rangeReader.Close() //nolint:errcheck // best-effort close of read stream
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, object.Size))
		c.DataFromReader(http.StatusPartialContent, length, object.ContentType, rangeReader, nil)
		return
	}

	// Get object from storage backend
	file, err := getCurrentBytes(storageBackend, bucketName, &object)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to retrieve object",
			Message: err.Error(),
		})
		return
	}
	defer file.Close() //nolint:errcheck // best-effort close of read-only file

	// Stream file to response — cap at object.Size so the body never exceeds the
	// declared Content-Length (multipart-assembled files may have trailing bytes).
	c.Header("Content-Length", strconv.FormatInt(object.Size, 10))
	c.DataFromReader(http.StatusOK, object.Size, object.ContentType, io.LimitReader(file, object.Size), nil)
}

// DeleteObject deletes an object from a bucket
// @Summary Delete an object
// @Description Deletes an object and its metadata from the specified bucket. Requires DeleteObject permission.
// @Tags buckets
// @Produce json
// @Param name path string true "Bucket name"
// @Param key path string true "Object key"
// @Success 200 {object} models.SuccessResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/{key} [delete]
func (h *BucketHandler) DeleteObject(c *gin.Context) {
	bucketName := c.Param("name")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	if validation.IsReservedObjectKey(objectKey) {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Object not found",
		})
		return
	}
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

	// Check policy permissions
	allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionDeleteObject)
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
			Message: "You don't have permission to delete this object",
		})
		return
	}

	// Serialize with other writers of this key (archive/delete must not
	// interleave with an overwrite).
	unlock, lockErr := lockObjectKeysChecked(c.Request.Context(), &bucket, objectKey)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock object"})
		return
	}
	defer unlock()

	// Get object metadata from database
	var object models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Object not found",
		})
		return
	}

	// Get storage backend for this bucket
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to initialize storage backend",
			Message: err.Error(),
		})
		return
	}

	// Versioned delete: archive bytes + record a delete marker.
	if _, handled, derr := versionedDeleteCurrent(storageBackend, &bucket, &object); handled {
		if derr != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "Failed to delete object",
				Message: derr.Error(),
			})
			return
		}
		notifyObjectEvent(&bucket, services.EventObjectRemoved, objectKey, 0, "", "")
		c.JSON(http.StatusOK, models.SuccessResponse{
			Message: "Object deleted successfully",
		})
		return
	}

	// WORM: an unversioned delete is permanent, so retention forbids it.
	if retentionBlocks(&bucket, object.UpdatedAt) {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error:   "Object under retention",
			Message: fmt.Sprintf("Object is under retention for %d days and cannot be deleted yet", bucket.RetentionDays),
		})
		return
	}

	// The metadata row is the visibility boundary. A physical generation (or
	// pre-migration public-key blob) remains until a reader-safe collector can
	// prove it is unreferenced; deleting it first would strand a live row if
	// the process stopped before the database commit.
	if _, generationAware := storageBackend.(storage.GenerationStorage); generationAware {
		generationCommitBoundary("console-delete")
	} else {
		if err := storageBackend.DeleteObject(bucketName, objectKey); err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "Failed to delete object from storage",
				Message: err.Error(),
			})
			return
		}
	}

	// Delete object metadata from database
	if err := database.DB.Delete(&object).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to delete object metadata",
			Message: err.Error(),
		})
		return
	}
	if err := deleteLegacyCurrentAfterCommit(storageBackend, &bucket, &object); err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to delete legacy object from storage", Message: err.Error()})
		return
	}

	notifyObjectEvent(&bucket, services.EventObjectRemoved, objectKey, 0, "", "")
	c.JSON(http.StatusOK, models.SuccessResponse{
		Message: "Object deleted successfully",
	})
}

// HeadObject retrieves metadata headers for an object without downloading its content
// @Summary Get object metadata (HEAD)
// @Description Returns HTTP headers with metadata (Content-Type, Content-Length, ETag, Last-Modified) for the specified object without returning the body. Requires HeadObject permission.
// @Tags buckets
// @Param name path string true "Bucket name"
// @Param key path string true "Object key"
// @Success 200 "Object metadata headers"
// @Failure 403 "Permission denied"
// @Failure 404 "Object or bucket not found"
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/{key} [head]
func (h *BucketHandler) HeadObject(c *gin.Context) {
	bucketName := c.Param("name")
	objectKey := strings.TrimPrefix(c.Param("key"), "/")
	if validation.IsReservedObjectKey(objectKey) {
		c.Status(http.StatusNotFound)
		return
	}
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	// Get bucket from database
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Check policy permissions
	allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, objectKey, services.ActionHeadObject)
	if err != nil || !allowed {
		c.Status(http.StatusForbidden)
		return
	}

	// Get object metadata from database
	var object models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, objectKey).First(&object).Error; err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Set response headers (no body for HEAD request)
	c.Header("Content-Type", object.ContentType)
	c.Header("Content-Length", strconv.FormatInt(object.Size, 10))
	c.Header("ETag", fmt.Sprintf("\"%s\"", object.ETag))
	c.Header("Last-Modified", object.UpdatedAt.UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	writeStoredContentHeaders(c, object.ContentDisposition, object.CacheControl)

	c.Status(http.StatusOK)
}

// MoveObjectRequest represents the request body for moving an object
type MoveObjectRequest struct {
	SourceKey      string `json:"source_key" binding:"required"`
	DestinationKey string `json:"destination_key" binding:"required"`
}

// RenameObjectRequest represents the request body for renaming an object
type RenameObjectRequest struct {
	SourceKey string `json:"source_key" binding:"required"`
	NewName   string `json:"new_name" binding:"required"`
}

// MoveObject moves an object to a new key within the same bucket
// @Summary Move an object
// @Description Moves an object from one key to another within the same bucket. Requires GetObject, PutObject, and DeleteObject permissions on the respective keys.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Param request body MoveObjectRequest true "Source and destination keys"
// @Success 200 {object} object "Move result with updated object"
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/move [post]
func (h *BucketHandler) MoveObject(c *gin.Context) {
	var req MoveObjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request",
			Message: err.Error(),
		})
		return
	}

	// Validate keys
	if req.SourceKey == req.DestinationKey {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Source and destination keys cannot be the same",
		})
		return
	}

	h.moveSingleObject(c, req.SourceKey, req.DestinationKey, "Object moved successfully", "Destination object already exists")
}

// moveSingleObject is the shared implementation of MoveObject and
// RenameObject: validate the destination key, require GetObject + DeleteObject
// on the source and PutObject on the destination, refuse to overwrite an
// existing destination or to permanently remove retained data, then move
// through the versioning-aware helper under per-key locks.
func (h *BucketHandler) moveSingleObject(c *gin.Context, srcKey, dstKey, successMsg, conflictMsg string) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	if err := validation.ValidateObjectKey(dstKey); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid destination key",
			Message: err.Error(),
		})
		return
	}
	if validation.IsReservedObjectKey(srcKey) {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Source object not found",
		})
		return
	}

	// Get bucket from database
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Bucket not found",
		})
		return
	}

	checks := []struct {
		key, action, denied string
	}{
		{srcKey, services.ActionGetObject, "You don't have permission to read the source object"},
		{dstKey, services.ActionPutObject, "You don't have permission to write to the destination"},
		{srcKey, services.ActionDeleteObject, "You don't have permission to delete the source object"},
	}
	for _, chk := range checks {
		allowed, err := h.policyService.CheckObjectAccess(userUUID, bucketName, chk.key, chk.action)
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
				Message: chk.denied,
			})
			return
		}
	}

	// Get storage backend
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to initialize storage backend",
			Message: err.Error(),
		})
		return
	}

	unlock, lockErr := lockObjectKeysChecked(c.Request.Context(), &bucket, srcKey, dstKey)
	if lockErr != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to lock objects"})
		return
	}
	defer unlock()

	// Get source object from database
	var sourceObject models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, srcKey).First(&sourceObject).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Source object not found",
		})
		return
	}

	// Never overwrite: the caller has not been authorized to replace (or
	// version) whatever lives at the destination.
	if currentObjectExists(bucket.ID, dstKey) {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error: conflictMsg,
		})
		return
	}

	// WORM: outside versioning the source bytes are removed permanently.
	if bucket.Versioning != models.VersioningEnabled && retentionBlocks(&bucket, sourceObject.UpdatedAt) {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error:   "Object under retention",
			Message: fmt.Sprintf("Object is under retention for %d days and cannot be moved yet", bucket.RetentionDays),
		})
		return
	}

	moved, err := moveObjectWithinBucket(storageBackend, &bucket, &sourceObject, dstKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to move object",
			Message: err.Error(),
		})
		return
	}

	notifyObjectEvent(&bucket, services.EventObjectCreated, moved.Key, moved.Size, moved.ETag, moved.VersionID)
	notifyObjectEvent(&bucket, services.EventObjectRemoved, srcKey, 0, "", "")

	c.JSON(http.StatusOK, gin.H{
		"message": successMsg,
		"object":  moved,
	})
}

// RenameObject renames an object within its current folder
// @Summary Rename an object
// @Description Renames an object by changing its filename while keeping it in the same folder. The new name cannot contain slashes. Requires GetObject, PutObject, and DeleteObject permissions.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Param request body RenameObjectRequest true "Source key and new filename"
// @Success 200 {object} object "Rename result with updated object"
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/objects/rename [post]
func (h *BucketHandler) RenameObject(c *gin.Context) {
	var req RenameObjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request",
			Message: err.Error(),
		})
		return
	}

	// Validate new name (no slashes allowed - it's just a filename)
	if strings.Contains(req.NewName, "/") {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "New name cannot contain slashes. Use move operation to change folders.",
		})
		return
	}

	destinationKey := renameDestinationKey(req.SourceKey, req.NewName)

	if req.SourceKey == destinationKey {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "New name is the same as the current name",
		})
		return
	}

	h.moveSingleObject(c, req.SourceKey, destinationKey, "Object renamed successfully", "An object with that name already exists")
}

// renameDestinationKey builds the key of srcKey renamed to newName in the
// same folder. A folder-marker object ("a/dir/") is renamed as a folder
// marker ("a/<newName>/"); this renames only the marker object — the folder
// move moves a folder's contents.
func renameDestinationKey(srcKey, newName string) string {
	name, suffix := strings.TrimSuffix(srcKey, "/"), ""
	if name != srcKey {
		suffix = "/"
	}
	if lastSlash := strings.LastIndex(name, "/"); lastSlash >= 0 {
		return name[:lastSlash+1] + newName + suffix
	}
	return newName + suffix
}

// MoveFolderRequest represents the request body for moving a folder
type MoveFolderRequest struct {
	SourcePrefix      string `json:"source_prefix" binding:"required"`
	DestinationPrefix string `json:"destination_prefix" binding:"required"`
}

// maxFolderMoveObjects bounds a single folder move (every object is copied,
// permission-checked and locked individually).
const maxFolderMoveObjects = storage.MaxListObjects

// folderMoveLockTimeout bounds how long a folder move waits for its per-key
// locks before answering 409 (retry).
const folderMoveLockTimeout = 10 * time.Second

// maxFolderMoveEvents caps the webhook events one folder move emits (two per
// moved object: created + removed). Beyond it events are skipped and the
// skip is logged, so a large move cannot flood the webhook queue.
const maxFolderMoveEvents = 1000

// MoveFolder moves all objects under a folder prefix to a new prefix
// @Summary Move a folder
// @Description Moves all objects under the specified source prefix to a new destination prefix within the same bucket. Both prefixes must end with '/'. Requires GetObject and DeleteObject on every source object and PutObject on every destination key; refuses to overwrite existing destination objects. Cannot move a folder into itself.
// @Tags buckets
// @Accept json
// @Produce json
// @Param name path string true "Bucket name"
// @Param request body MoveFolderRequest true "Source and destination folder prefixes"
// @Success 200 {object} object "Move result with count of moved objects"
// @Failure 400 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security BearerAuth
// @Router /api/buckets/{name}/folders/move [post]
func (h *BucketHandler) MoveFolder(c *gin.Context) {
	bucketName := c.Param("name")
	userID, _ := c.Get("user_id")
	userUUID := userID.(uuid.UUID)

	var req MoveFolderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Invalid request",
			Message: err.Error(),
		})
		return
	}
	srcPrefix, dstPrefix := req.SourcePrefix, req.DestinationPrefix

	// Folder prefixes must be canonical "a/b/" forms: without the trailing
	// slash "foo" would also match "foobar/...".
	for _, p := range []string{srcPrefix, dstPrefix} {
		if !strings.HasSuffix(p, "/") {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error: "Folder prefixes must end with '/'",
			})
			return
		}
		if err := validation.ValidateObjectKey(p); err != nil {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "Invalid folder prefix",
				Message: err.Error(),
			})
			return
		}
	}

	// Validate prefixes
	if srcPrefix == dstPrefix {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Source and destination prefixes cannot be the same",
		})
		return
	}

	// Don't allow moving a folder into itself
	if strings.HasPrefix(dstPrefix, srcPrefix) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error: "Cannot move a folder into itself",
		})
		return
	}

	// Get bucket from database
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "Bucket not found",
		})
		return
	}

	// Get all objects with the source prefix (LIKE wildcards escaped: a
	// prefix containing '%' or '_' must not match unrelated keys).
	var candidates []models.Object
	if err := database.DB.Where("bucket_id = ? AND key LIKE ?", bucket.ID, validation.EscapeLikeWildcards(srcPrefix)+"%").
		Order("key ASC").Limit(maxFolderMoveObjects + 1).Find(&candidates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to list source objects",
			Message: err.Error(),
		})
		return
	}
	sourceObjects := make([]models.Object, 0, len(candidates))
	for _, obj := range candidates {
		if strings.HasPrefix(obj.Key, srcPrefix) {
			sourceObjects = append(sourceObjects, obj)
		}
	}

	if len(sourceObjects) == 0 {
		c.JSON(http.StatusNotFound, models.ErrorResponse{
			Error: "No objects found in source folder",
		})
		return
	}
	if len(sourceObjects) > maxFolderMoveObjects {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "Folder too large",
			Message: fmt.Sprintf("A folder move is limited to %d objects", maxFolderMoveObjects),
		})
		return
	}

	// Pre-validate every object before mutating any: destination key
	// validity, per-object permissions (ownership of the bucket does not
	// bypass policy), and retention. Permissions come from one evaluator
	// (user, group and bucket policies loaded and parsed once) instead of
	// three database-backed policy checks per object.
	evaluator, err := h.policyService.NewAccessEvaluator(userUUID, bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Policy check failed",
			Message: err.Error(),
		})
		return
	}
	newKeys := make([]string, len(sourceObjects))
	lockKeys := make([]string, 0, 2*len(sourceObjects))
	for i, obj := range sourceObjects {
		newKey := dstPrefix + strings.TrimPrefix(obj.Key, srcPrefix)
		if err := validation.ValidateObjectKey(newKey); err != nil {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "Invalid destination key",
				Message: fmt.Sprintf("%s: %v", newKey, err),
			})
			return
		}
		checks := []struct{ key, action string }{
			{obj.Key, services.ActionGetObject},
			{obj.Key, services.ActionDeleteObject},
			{newKey, services.ActionPutObject},
		}
		for _, chk := range checks {
			if !evaluator.Allowed(chk.action, chk.key) {
				c.JSON(http.StatusForbidden, models.ErrorResponse{
					Error:   "Permission denied",
					Message: fmt.Sprintf("Missing %s permission on %s", chk.action, chk.key),
				})
				return
			}
		}
		newKeys[i] = newKey
		lockKeys = append(lockKeys, obj.Key, newKey)
	}

	// Get storage backend
	storageBackend, err := h.getStorageBackend(&bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "Failed to initialize storage backend",
			Message: err.Error(),
		})
		return
	}

	// Bounded wait for the per-key locks: a folder move holds thousands of
	// them, so rather than queueing behind a slow upload of one key (while
	// blocking every other writer of the keys already acquired) it gives up
	// and asks the client to retry.
	unlock, locked := tryLockObjectKeysChecked(&bucket, folderMoveLockTimeout, lockKeys...)
	if !locked {
		c.JSON(http.StatusConflict, models.ErrorResponse{
			Error:   "Objects busy",
			Message: "Some objects in the folder are being modified; please retry",
		})
		return
	}
	defer unlock()

	// Re-read the source rows under the lock and re-check the destinations
	// and retention, so nothing changed between validation and mutation.
	ids := make([]uuid.UUID, len(sourceObjects))
	for i, obj := range sourceObjects {
		ids[i] = obj.ID
	}
	current := make(map[uuid.UUID]models.Object, len(ids))
	for i := 0; i < len(ids); i += 500 {
		end := min(i+500, len(ids))
		var rows []models.Object
		if err := database.DB.Where("id IN ?", ids[i:end]).Find(&rows).Error; err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to re-read source objects", Message: err.Error()})
			return
		}
		for _, r := range rows {
			current[r.ID] = r
		}
	}
	for i := 0; i < len(newKeys); i += 500 {
		end := min(i+500, len(newKeys))
		var existing []string
		if err := database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key IN ?", bucket.ID, newKeys[i:end]).
			Limit(1).Pluck("key", &existing).Error; err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "Failed to check destination", Message: err.Error()})
			return
		}
		if len(existing) > 0 {
			c.JSON(http.StatusConflict, models.ErrorResponse{
				Error:   "Destination object already exists",
				Message: existing[0],
			})
			return
		}
	}
	for i, obj := range sourceObjects {
		cur, ok := current[obj.ID]
		if !ok || cur.Key != obj.Key {
			c.JSON(http.StatusConflict, models.ErrorResponse{
				Error:   "Source folder changed during the move; please retry",
				Message: obj.Key,
			})
			return
		}
		if bucket.Versioning != models.VersioningEnabled && retentionBlocks(&bucket, cur.UpdatedAt) {
			c.JSON(http.StatusConflict, models.ErrorResponse{
				Error:   "Object under retention",
				Message: fmt.Sprintf("%s is under retention for %d days and cannot be moved yet", obj.Key, bucket.RetentionDays),
			})
			return
		}
		sourceObjects[i] = cur
	}

	// Move each object through the versioning-aware helper. Webhook events
	// are bounded per request (see maxFolderMoveEvents).
	movedCount := 0
	eventsSent := 0
	defer func() {
		if skipped := 2*movedCount - eventsSent; skipped > 0 {
			logger.Warn("Folder move: webhook events capped", map[string]interface{}{
				"bucket": bucketName, "source_prefix": srcPrefix, "destination_prefix": dstPrefix,
				"moved": movedCount, "events_sent": eventsSent, "events_skipped": skipped,
			})
		}
	}()
	for i := range sourceObjects {
		obj := sourceObjects[i]
		moved, err := moveObjectWithinBucket(storageBackend, &bucket, &obj, newKeys[i])
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":       "Failed to move folder",
				"message":     err.Error(),
				"moved_count": movedCount,
			})
			return
		}
		if eventsSent+2 <= maxFolderMoveEvents {
			notifyObjectEvent(&bucket, services.EventObjectCreated, moved.Key, moved.Size, moved.ETag, moved.VersionID)
			notifyObjectEvent(&bucket, services.EventObjectRemoved, obj.Key, 0, "", "")
			eventsSent += 2
		}
		movedCount++
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "Folder moved successfully",
		"moved_count": movedCount,
	})
}
