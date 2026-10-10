package database

import (
	"errors"
	"log"

	"bkt/internal/config"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// InitializeStartupBuckets provisions the S3-backed buckets declared in
// config (S3_BUCKETS). For each name it ensures the real S3 bucket exists
// (linking to it if it already does, creating it otherwise) and registers a
// matching bkt bucket record owned by the default admin, so the bucket shows
// up in the UI immediately and uploads land in the configured S3 backend.
//
// It is a no-op when no buckets are configured. Failures for an individual
// bucket are logged and skipped rather than aborting startup.
func InitializeStartupBuckets(cfg *config.Config) error {
	names := cfg.Storage.S3.Buckets
	if len(names) == 0 {
		return nil
	}

	if !cfg.Storage.S3.Enabled {
		log.Println("⚠️  S3_BUCKETS is set but S3_ENABLED is not true — skipping S3 bucket provisioning")
		return nil
	}

	// Buckets are owned by the default admin (created earlier in startup).
	var admin models.User
	if err := DB.Where("username = ?", cfg.Auth.AdminUsername).First(&admin).Error; err != nil {
		log.Printf("⚠️  Cannot provision S3 buckets: admin user %q not found: %v", cfg.Auth.AdminUsername, err)
		return nil
	}

	// Build an S3 backend from the .env S3 configuration. Buckets registered
	// here get no S3ConfigID, which always means "the .env S3 backend" (see
	// api getStorageBackend) — they are never re-routed to a DB default config.
	s3Backend, err := storage.NewStorageBackend(
		"s3",
		cfg.Storage.RootPath,
		cfg.Storage.S3.Endpoint,
		cfg.Storage.S3.Region,
		cfg.Storage.S3.AccessKeyID,
		cfg.Storage.S3.SecretAccessKey,
		cfg.Storage.S3.BucketPrefix,
		cfg.Storage.S3.UseSSL,
		cfg.Storage.S3.ForcePathStyle,
		cfg.Storage.S3SSE,
	)
	if err != nil {
		log.Printf("⚠️  Cannot provision S3 buckets: failed to init S3 backend: %v", err)
		return nil
	}

	for _, name := range names {
		provisionOneBucket(s3Backend, admin.ID, name, cfg.Storage.S3.Region)
	}
	return nil
}

func provisionOneBucket(s3Backend storage.StorageBackend, ownerID uuid.UUID, name, region string) {
	// Already registered as a bkt bucket? Nothing to do.
	var existing models.Bucket
	err := DB.Where("name = ?", name).First(&existing).Error
	if err == nil {
		log.Printf("✓ S3 bucket %q already registered — skipping", name)
		return
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("⚠️  S3 bucket %q: database lookup failed: %v", name, err)
		return
	}

	// Ensure the real S3 bucket exists: link if present, create otherwise.
	exists, checkErr := s3Backend.BucketExists(name)
	if checkErr != nil {
		log.Printf("⚠️  S3 bucket %q: cannot access in S3 (check credentials/permissions): %v", name, checkErr)
		return
	}
	if !exists {
		if createErr := s3Backend.CreateBucket(name, region); createErr != nil {
			log.Printf("⚠️  S3 bucket %q: failed to create in S3: %v", name, createErr)
			return
		}
		log.Printf("✅ Created S3 bucket %q in storage backend", name)
	} else {
		log.Printf("✅ Linked existing S3 bucket %q", name)
	}

	// Register the bkt bucket record so it appears in the UI. S3ConfigID
	// stays nil: the bucket lives on the .env S3 backend.
	bucket := models.Bucket{
		Name:           name,
		OwnerID:        ownerID,
		StorageBackend: "s3",
		Region:         region,
	}
	if concrete, ok := s3Backend.(*storage.S3Storage); ok {
		bucket.S3RouteIdentity = concrete.RouteIdentity()
	}
	if err := DB.Create(&bucket).Error; err != nil {
		log.Printf("⚠️  S3 bucket %q: failed to register in database: %v", name, err)
		return
	}
	log.Printf("✅ Registered S3 bucket %q in bkt (owner=admin)", name)
}
