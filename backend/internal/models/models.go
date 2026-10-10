package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// User represents a user in the system
type User struct {
	ID       uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	Username string    `gorm:"uniqueIndex;not null" json:"username"`
	Email    string    `gorm:"uniqueIndex;not null" json:"email"`
	Password string    `gorm:"" json:"-"` // Nullable for SSO users, never serialize
	IsAdmin  bool      `gorm:"default:false" json:"is_admin"`
	IsLocked bool      `gorm:"default:false" json:"is_locked"` // Account lock status
	// TokenVersion is bumped to invalidate all outstanding JWTs for this user
	// (lock, delete, password change, admin demotion, "sign out everywhere").
	TokenVersion int       `gorm:"default:0;not null" json:"-"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`

	// SSO fields
	SSOProvider string `gorm:"index" json:"sso_provider,omitempty"` // "google", "vault", or empty for local
	SSOID       string `gorm:"index" json:"sso_id,omitempty"`       // Unique ID from SSO provider
	SSOEmail    string `gorm:"" json:"sso_email,omitempty"`         // Email from SSO (may differ from Email)

	// Computed for the admin user list (not stored): policies attached directly
	// (manual assignments and SSO claim sync alike) and distinct policies
	// inherited through group membership.
	PolicyCount      *int        `gorm:"-" json:"policy_count,omitempty"`
	GroupPolicyCount *int        `gorm:"-" json:"group_policy_count,omitempty"`
	PolicyIDs        []uuid.UUID `gorm:"-" json:"policy_ids,omitempty"` // directly attached policy IDs

	// Relationships
	Buckets    []Bucket    `gorm:"foreignKey:OwnerID" json:"buckets,omitempty"`
	AccessKeys []AccessKey `gorm:"foreignKey:UserID" json:"access_keys,omitempty"`
	Policies   []Policy    `gorm:"many2many:user_policies;" json:"policies,omitempty"`
}

// BeforeCreate hook to generate UUID
func (u *User) BeforeCreate(tx *gorm.DB) error {
	if u.ID == uuid.Nil {
		u.ID = uuid.New()
	}
	return nil
}

// AccessKey represents API access credentials
type AccessKey struct {
	ID                 uuid.UUID  `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	UserID             uuid.UUID  `gorm:"type:uuid;not null;index" json:"user_id"`
	AccessKey          string     `gorm:"uniqueIndex;not null" json:"access_key"`
	SecretKeyHash      string     `gorm:"not null" json:"-"`                              // Never serialize secret (bcrypt hash for API auth)
	SecretKeyEncrypted string     `gorm:"not null" json:"-"`                              // Never serialize secret (AES-encrypted for S3 auth)
	Name               string     `gorm:"" json:"name,omitempty"`                         // Human label to tell keys apart
	Temporary          bool       `gorm:"default:false;index" json:"temporary,omitempty"` // bkt-STS short-lived credential: excluded from the key limit, hard-deleted after expiry
	IssuerTokenVersion *int       `json:"-"`                                              // STS only: user's TokenVersion at issuance; a later bump (password change, lock, reuse detection) revokes it
	IsActive           bool       `gorm:"default:true" json:"is_active"`
	ReadOnly           bool       `gorm:"default:false" json:"read_only"`    // Deny mutating S3 operations
	ExpiresAt          *time.Time `gorm:"index" json:"expires_at,omitempty"` // nil = never expires
	LastUsedAt         *time.Time `json:"last_used_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`

	// Status is computed for API responses (not stored): "active", "expired"
	// or "revoked" (deactivated, or an STS credential whose session was
	// revoked). is_active alone doesn't say whether a key still works.
	Status string `gorm:"-" json:"status,omitempty"`

	// Relationships
	User User `gorm:"foreignKey:UserID" json:"user,omitempty"`
}

func (a *AccessKey) BeforeCreate(tx *gorm.DB) error {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	return nil
}

// S3Configuration represents an S3 storage configuration
type S3Configuration struct {
	ID              uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	Name            string    `gorm:"uniqueIndex;not null" json:"name"`
	Endpoint        string    `gorm:"not null" json:"endpoint"`
	Region          string    `gorm:"not null" json:"region"`
	AccessKeyID     string    `gorm:"not null" json:"access_key_id"`
	SecretAccessKey string    `gorm:"not null" json:"-"` // Encrypted, never serialize
	BucketPrefix    string    `json:"bucket_prefix,omitempty"`
	UseSSL          bool      `gorm:"default:true" json:"use_ssl"`
	ForcePathStyle  bool      `gorm:"default:false" json:"force_path_style"`
	IsDefault       bool      `gorm:"default:false" json:"is_default"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	// Relationships
	Buckets []Bucket `gorm:"foreignKey:S3ConfigID" json:"buckets,omitempty"`
}

func (s *S3Configuration) BeforeCreate(tx *gorm.DB) error {
	if s.ID == uuid.Nil {
		s.ID = uuid.New()
	}
	return nil
}

// Bucket represents a storage bucket
type Bucket struct {
	ID              uuid.UUID  `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	Name            string     `gorm:"uniqueIndex;not null" json:"name"`
	OwnerID         uuid.UUID  `gorm:"type:uuid;not null;index" json:"owner_id"`
	IsPublic        bool       `gorm:"default:false" json:"is_public"`
	Region          string     `gorm:"default:'us-east-1'" json:"region"`
	StorageBackend  string     `gorm:"default:'local'" json:"storage_backend"`  // "local" or "s3"
	S3ConfigID      *uuid.UUID `gorm:"type:uuid" json:"s3_config_id,omitempty"` // Optional: specific S3 config to use
	S3RouteIdentity string     `gorm:"not null;default:''" json:"-"`            // Fingerprint of the upstream route fixed when the bucket is linked.
	// Versioning state: "" (never enabled), "enabled", or "suspended".
	// Suspended keeps existing versions browsable but stops creating new ones
	// (a documented simplification of AWS's null-version semantics).
	Versioning string `gorm:"default:''" json:"versioning"`
	// Lifecycle holds the bucket's lifecycle configuration as JSON (see
	// LifecycleConfig); nil = no lifecycle rules.
	Lifecycle *string `gorm:"type:jsonb" json:"lifecycle,omitempty"`
	// Lifecycle provenance, recorded whenever lifecycle is set (cleared with
	// it). The sweep expires keys only with the configuring user's CURRENT
	// s3:DeleteObject permission (per key). Nil on legacy configs.
	LifecycleConfiguredBy *uuid.UUID `gorm:"type:uuid" json:"-"`
	LifecycleConfiguredAt *time.Time `json:"-"`
	// QuotaBytes caps the total size of CURRENT objects in the bucket
	// (version storage is not counted). 0 = unlimited.
	QuotaBytes int64 `gorm:"default:0" json:"quota_bytes"`
	// RetentionDays (WORM-lite): while set, no version of any object can be
	// permanently removed until it is older than this many days. Requires
	// versioning to be enabled. Plain deletes still create markers (data is
	// preserved); version-addressed deletes, lifecycle purges, and bucket
	// deletion are refused for retained data. 0 = off.
	RetentionDays int `gorm:"default:0" json:"retention_days"`
	// Webhook event notifications: POSTed as JSON to WebhookURL for the event
	// types in WebhookEvents (csv of "created","removed"). Empty URL = off.
	WebhookURL    string `gorm:"default:''" json:"webhook_url,omitempty"`
	WebhookSecret string `gorm:"default:''" json:"-"`
	WebhookEvents string `gorm:"default:''" json:"webhook_events,omitempty"`
	// ReplicateTo mirrors this bucket's current objects into another bkt
	// bucket (periodic sync; the target is fully managed by replication).
	ReplicateTo string `gorm:"default:''" json:"replicate_to,omitempty"`
	// Replication provenance, recorded when replicate_to is set. The sweep
	// runs with the configuring user's CURRENT permissions (per key) and
	// refuses to write into a target whose ID no longer matches (a bucket
	// deleted and re-created under the same name). Nil on legacy configs.
	ReplicationConfiguredBy *uuid.UUID `gorm:"type:uuid" json:"-"`
	ReplicationConfiguredAt *time.Time `json:"-"`
	ReplicateToID           *uuid.UUID `gorm:"type:uuid" json:"-"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Relationships
	Owner    User             `gorm:"foreignKey:OwnerID" json:"owner,omitempty"`
	Objects  []Object         `gorm:"foreignKey:BucketID" json:"objects,omitempty"`
	S3Config *S3Configuration `gorm:"foreignKey:S3ConfigID" json:"s3_config,omitempty"`
}

// LocalBucketCleanup keeps local bytes discoverable after bucket metadata is
// removed. A failed filesystem cleanup can be retried without linking those
// bytes to a newly created bucket with the same name.
type LocalBucketCleanup struct {
	ID           uuid.UUID `gorm:"type:uuid;primaryKey"`
	Name         string    `gorm:"uniqueIndex;not null"`
	RootPath     string    `gorm:"not null"`
	RootIdentity string    `gorm:"not null"`
}

func (b *Bucket) BeforeCreate(tx *gorm.DB) error {
	if b.ID == uuid.Nil {
		b.ID = uuid.New()
	}
	return nil
}

// Object represents a stored object
type Object struct {
	ID                 uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	BucketID           uuid.UUID `gorm:"type:uuid;not null;index;uniqueIndex:idx_bucket_key_unique" json:"bucket_id"`
	Key                string    `gorm:"not null;uniqueIndex:idx_bucket_key_unique" json:"key"` // Object name/path
	Size               int64     `gorm:"not null" json:"size"`
	ContentType        string    `json:"content_type"`
	ContentDisposition string    `json:"content_disposition,omitempty"`
	CacheControl       string    `json:"cache_control,omitempty"`
	ETag               string    `json:"etag"`
	SHA256             string    `json:"sha256,omitempty"`                     // SHA256 hash of content
	StoragePath        string    `gorm:"not null" json:"-"`                    // Internal file system path
	GenerationID       string    `gorm:"not null;default:''" json:"-"`         // Empty for legacy mutable current bytes
	Metadata           *string   `gorm:"type:jsonb" json:"metadata,omitempty"` // x-amz-meta-* user metadata (JSON map, nullable)
	Tags               *string   `gorm:"type:jsonb" json:"tags,omitempty"`     // S3 object tags (JSON map, nullable)
	// VersionID of the CURRENT version ("" = written while unversioned, the
	// S3 "null" version). Non-current versions live in object_versions.
	VersionID string    `gorm:"default:''" json:"version_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Relationships
	Bucket Bucket `gorm:"foreignKey:BucketID" json:"bucket,omitempty"`
}

func (o *Object) BeforeCreate(tx *gorm.DB) error {
	if o.ID == uuid.Nil {
		o.ID = uuid.New()
	}
	return nil
}

// Policy represents an IAM-style access policy
type Policy struct {
	ID          uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	Name        string    `gorm:"uniqueIndex;not null" json:"name"`
	Description string    `json:"description,omitempty"`
	Document    string    `gorm:"type:jsonb;not null" json:"document"` // IAM policy JSON
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Relationships
	Users []User `gorm:"many2many:user_policies;" json:"users,omitempty"`
}

func (p *Policy) BeforeCreate(tx *gorm.DB) error {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	return nil
}

// BucketPolicy represents a bucket-specific policy
type BucketPolicy struct {
	BucketID       uuid.UUID `gorm:"type:uuid;primary_key" json:"bucket_id"`
	PolicyDocument string    `gorm:"type:jsonb;not null" json:"policy_document"`
	UpdatedAt      time.Time `json:"updated_at"`

	// Relationships
	Bucket Bucket `gorm:"foreignKey:BucketID" json:"bucket,omitempty"`
}

// Request DTOs

type RegisterRequest struct {
	Username string `json:"username" binding:"required,min=3,max=50"`
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
}

type LoginRequest struct {
	Username string `json:"username" binding:"required,max=255"`
	Password string `json:"password" binding:"required,max=1024"`
}

type CreateBucketRequest struct {
	Name           string  `json:"name" binding:"required,min=3,max=63"`
	IsPublic       bool    `json:"is_public"`
	Region         string  `json:"region"`
	StorageBackend string  `json:"storage_backend"`        // "local" or "s3"
	S3ConfigID     *string `json:"s3_config_id,omitempty"` // Optional: specific S3 config to use
}

type CreatePolicyRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Document    string `json:"document" binding:"required"`
}

type UpdatePolicyRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Document    string `json:"document"`
}

type CreateS3ConfigRequest struct {
	Name            string `json:"name" binding:"required,min=3,max=100"`
	Endpoint        string `json:"endpoint" binding:"required"`
	Region          string `json:"region" binding:"required"`
	AccessKeyID     string `json:"access_key_id" binding:"required"`
	SecretAccessKey string `json:"secret_access_key" binding:"required"`
	BucketPrefix    string `json:"bucket_prefix"`
	UseSSL          *bool  `json:"use_ssl"`
	ForcePathStyle  *bool  `json:"force_path_style"`
	IsDefault       bool   `json:"is_default"`
}

type UpdateS3ConfigRequest struct {
	Name            string `json:"name"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"` // Only update if provided
	// BucketPrefix: omitted/null = unchanged, "" = clear the prefix.
	BucketPrefix   *string `json:"bucket_prefix"`
	UseSSL         *bool   `json:"use_ssl"`
	ForcePathStyle *bool   `json:"force_path_style"`
	IsDefault      *bool   `json:"is_default"`
}

// Response DTOs

type AuthResponse struct {
	Token        string `json:"token"`
	RefreshToken string `json:"refresh_token,omitempty"`
	User         User   `json:"user"`
}

// AuditLog represents a logged administrative action for compliance and security
type AuditLog struct {
	ID           uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	UserID       uuid.UUID `gorm:"type:uuid;index;not null" json:"user_id"`
	Username     string    `gorm:"index;not null" json:"username"`       // Denormalized for faster queries
	Action       string    `gorm:"index;not null" json:"action"`         // e.g., "CreateUser", "DeleteBucket", "UpdatePolicy"
	ResourceType string    `gorm:"index" json:"resource_type"`           // e.g., "User", "Bucket", "Policy"
	ResourceID   string    `gorm:"index" json:"resource_id,omitempty"`   // ID of affected resource
	ResourceName string    `gorm:"" json:"resource_name,omitempty"`      // Name of affected resource (for readability)
	IPAddress    string    `gorm:"index" json:"ip_address"`              // Client IP for forensics
	UserAgent    string    `gorm:"" json:"user_agent,omitempty"`         // Client User-Agent
	RequestID    string    `gorm:"index" json:"request_id,omitempty"`    // Correlation ID for tracing
	Status       string    `gorm:"index;not null" json:"status"`         // "success", "failure", "denied"
	ErrorMessage string    `gorm:"" json:"error_message,omitempty"`      // Error details if failed
	Metadata     string    `gorm:"type:jsonb" json:"metadata,omitempty"` // Additional context (JSON)
	CreatedAt    time.Time `gorm:"index" json:"created_at"`

	// NOTE: deliberately no User relation/foreign key — the audit trail must
	// outlive the users it records (an FK would block user deletion, and
	// Username above is already denormalized for display).
}

// BeforeCreate hook to generate UUID
func (a *AuditLog) BeforeCreate(tx *gorm.DB) error {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	return nil
}

// RevokedToken stores JTIs of logged-out tokens until they naturally expire
// RevokedTokenReason* distinguish WHY a token was revoked, so refresh-token
// reuse detection can tell an attack indicator (replay of a rotated token)
// from a benign stale retry (replay of a logout-revoked token).
const (
	RevokedReasonLogout  = "logout"
	RevokedReasonRotated = "rotated"
)

type RevokedToken struct {
	ID        uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	JTI       string    `gorm:"uniqueIndex;not null" json:"jti"`
	UserID    uuid.UUID `gorm:"type:uuid;index;not null" json:"user_id"`
	Reason    string    `gorm:"default:''" json:"reason"`
	ExpiresAt time.Time `gorm:"index;not null" json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

func (r *RevokedToken) BeforeCreate(tx *gorm.DB) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	return nil
}

// IdempotencyKey represents a stored idempotency key for preventing duplicate requests
type IdempotencyKey struct {
	ID           uuid.UUID `gorm:"type:uuid;primary_key;default:gen_random_uuid()" json:"id"`
	Key          string    `gorm:"uniqueIndex;not null" json:"key"`         // Client-provided idempotency key
	UserID       uuid.UUID `gorm:"type:uuid;index;not null" json:"user_id"` // User who made the request
	Method       string    `gorm:"not null" json:"method"`                  // HTTP method (POST, PUT, etc.)
	Path         string    `gorm:"not null" json:"path"`                    // Request path
	StatusCode   int       `gorm:"not null" json:"status_code"`             // Response status code
	ResponseBody string    `gorm:"type:text" json:"response_body"`          // Cached response body
	RequestHash  string    `gorm:"not null" json:"request_hash"`            // SHA256 hash of request body
	CreatedAt    time.Time `gorm:"index" json:"created_at"`
	ExpiresAt    time.Time `gorm:"index;not null" json:"expires_at"` // TTL expiration

	// Relationships
	User User `gorm:"foreignKey:UserID" json:"user,omitempty"`
}

func (i *IdempotencyKey) BeforeCreate(tx *gorm.DB) error {
	if i.ID == uuid.Nil {
		i.ID = uuid.New()
	}
	return nil
}

type AccessKeyResponse struct {
	AccessKey string     `json:"access_key"`
	SecretKey string     `json:"secret_key"` // Only shown once during creation
	Name      string     `json:"name,omitempty"`
	ReadOnly  bool       `json:"read_only"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

type SuccessResponse struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
