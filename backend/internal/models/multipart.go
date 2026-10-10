package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// MultipartUpload tracks an in-progress S3 multipart upload
type MultipartUpload struct {
	ID                 uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	UploadID           string    `gorm:"uniqueIndex;not null" json:"upload_id"` // UUID for local; AWS uploadId for S3 backend
	UserID             uuid.UUID `gorm:"type:uuid;index;not null" json:"user_id"`
	BucketName         string    `gorm:"index;not null" json:"bucket_name"`
	ObjectKey          string    `gorm:"not null" json:"object_key"`
	GenerationID       string    `gorm:"not null;default:''" json:"-"` // hidden physical target for generation-aware uploads
	ContentType        string    `json:"content_type"`
	ContentDisposition string    `json:"content_disposition,omitempty"`
	CacheControl       string    `json:"cache_control,omitempty"`
	Metadata           *string   `gorm:"type:jsonb" json:"metadata,omitempty"`      // x-amz-meta-* captured at initiate, applied at complete
	Status             string    `gorm:"default:'in-progress';index" json:"status"` // in-progress, completed, aborted
	CreatedAt          time.Time `gorm:"index" json:"created_at"`
	ExpiresAt          time.Time `gorm:"index;not null" json:"expires_at"` // 7 days — for abandoned upload cleanup
}

func (m *MultipartUpload) BeforeCreate(tx *gorm.DB) error {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	return nil
}
