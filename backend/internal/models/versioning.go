package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Bucket versioning states.
const (
	VersioningDisabled  = ""
	VersioningEnabled   = "enabled"
	VersioningSuspended = "suspended"
)

// ObjectVersion is a NON-current version of an object: either an archived
// prior version (bytes moved to version storage) or a delete marker. The
// `objects` table remains the single source of truth for the CURRENT state of
// every key — existing read paths stay untouched by versioning.
type ObjectVersion struct {
	ID        uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	BucketID  uuid.UUID `gorm:"type:uuid;not null;index:idx_ver_bucket_key" json:"bucket_id"`
	Key       string    `gorm:"not null;index:idx_ver_bucket_key" json:"key"`
	VersionID string    `gorm:"uniqueIndex;not null" json:"version_id"`
	// IsDeleteMarker: this version records a delete, not content. Marker rows
	// have no bytes in version storage.
	IsDeleteMarker     bool    `gorm:"default:false" json:"is_delete_marker"`
	Size               int64   `json:"size"`
	ContentType        string  `json:"content_type"`
	ContentDisposition string  `json:"content_disposition,omitempty"`
	CacheControl       string  `json:"cache_control,omitempty"`
	ETag               string  `json:"etag"`
	GenerationID       string  `gorm:"not null;default:''" json:"-"` // Empty for legacy version storage
	Metadata           *string `gorm:"type:jsonb" json:"metadata,omitempty"`
	Tags               *string `gorm:"type:jsonb" json:"tags,omitempty"`
	// VersionedAt is when this version STOPPED being current (archive time for
	// content versions, delete time for markers). ContentModifiedAt preserves
	// the original write time for display/LastModified.
	VersionedAt       time.Time `gorm:"index;not null" json:"versioned_at"`
	ContentModifiedAt time.Time `json:"content_modified_at"`
}

func (v *ObjectVersion) BeforeCreate(tx *gorm.DB) error {
	if v.ID == uuid.Nil {
		v.ID = uuid.New()
	}
	return nil
}

// LifecycleConfig is the JSON stored in Bucket.Lifecycle — a deliberately
// small, honest subset of S3 lifecycle: age-based expiry of current objects
// (optionally under a prefix) and age-based permanent expiry of noncurrent
// versions.
type LifecycleConfig struct {
	// ExpireDays: delete current objects this many days after their last
	// modification (through the versioned delete path, so versioned buckets
	// get delete markers). 0 = disabled.
	ExpireDays int `json:"expire_days"`
	// Prefix limits expiry to keys with this prefix ("" = whole bucket).
	Prefix string `json:"prefix,omitempty"`
	// NoncurrentExpireDays: permanently remove noncurrent versions (and
	// delete markers) this many days after they stopped being current.
	// 0 = keep forever.
	NoncurrentExpireDays int `json:"noncurrent_expire_days,omitempty"`
}
