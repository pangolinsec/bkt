package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bkt/internal/config"
	"bkt/internal/logger"
	"bkt/internal/models"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL sessions for object advisory locks
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var DB *gorm.DB

// AdvisoryDB has its own connection pool because object writers hold a
// session advisory lock while streaming bytes. Sharing DB's pool would let
// concurrent uploads occupy every connection and deadlock their metadata
// queries, which also need a connection before the locks can be released.
var AdvisoryDB *sql.DB

// migrationAdvisoryLockKey is an arbitrary but fixed key so that, in a
// multi-replica deployment, only one process runs AutoMigrate at a time.
// Concurrent DDL against the same database can deadlock or partially apply.
const migrationAdvisoryLockKey = 4927261

// gormLogLevel keeps SQL statement logging (which includes bound parameters —
// usernames, emails, object keys) out of production logs.
func gormLogLevel() gormlogger.LogLevel {
	env := strings.ToLower(os.Getenv("GO_ENV"))
	if env == "" {
		env = strings.ToLower(os.Getenv("APP_ENV"))
	}
	if env == "production" || env == "prod" {
		return gormlogger.Warn
	}
	return gormlogger.Info
}

// Initialize connects to the database and runs migrations
func Initialize(cfg *config.Config) error {
	dsn := cfg.GetDSN()
	lockDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("failed to open advisory lock database: %w", err)
	}
	lockDB.SetMaxOpenConns(25)
	lockDB.SetMaxIdleConns(10)
	lockDB.SetConnMaxLifetime(time.Hour)
	if err := lockDB.Ping(); err != nil {
		_ = lockDB.Close()
		return fmt.Errorf("failed to connect advisory lock database: %w", err)
	}
	lockDBInstalled := false
	defer func() {
		if !lockDBInstalled {
			_ = lockDB.Close()
		}
	}()

	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormLogLevel()),
	})

	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool to prevent resource exhaustion
	sqlDB, err := DB.DB()
	if err != nil {
		return fmt.Errorf("failed to get database instance: %w", err)
	}

	// Set maximum number of open connections (prevents exhausting database resources)
	sqlDB.SetMaxOpenConns(25)

	// Set maximum number of idle connections (reduces overhead)
	sqlDB.SetMaxIdleConns(10)

	// Set maximum lifetime of a connection (prevents stale connections)
	// 1 hour - forces connection refresh to pick up DNS/network changes
	sqlDB.SetConnMaxLifetime(time.Hour)

	logger.Info("Database connection established", map[string]interface{}{
		"host": cfg.Database.Host,
		"port": cfg.Database.Port,
		"db":   cfg.Database.DBName,
	})

	// Serialize migrations across replicas with a Postgres advisory lock, so
	// concurrent pod startups don't race DDL against the same database.
	// pg_advisory_lock is session-scoped: the lock, the migrations, and the
	// unlock must all run on the SAME pooled connection. DB.Connection pins one
	// connection for the closure; issuing these through the pool instead would
	// let the unlock land on a different session, where it returns false (not
	// an error) and the lock stays held until the original connection dies,
	// blocking other replicas' startup for up to ConnMaxLifetime.
	if err := DB.Connection(func(conn *gorm.DB) error {
		if lerr := conn.Exec("SELECT pg_advisory_lock(?)", migrationAdvisoryLockKey).Error; lerr != nil {
			return fmt.Errorf("failed to acquire migration lock: %w", lerr)
		}
		defer func() {
			var released bool
			uerr := conn.Raw("SELECT pg_advisory_unlock(?)", migrationAdvisoryLockKey).Scan(&released).Error
			if uerr != nil || !released {
				logger.Warn("Failed to release migration advisory lock", map[string]interface{}{
					"error": fmt.Sprintf("err=%v released=%v", uerr, released),
				})
			}
		}()
		if err := runMigrations(conn); err != nil {
			return err
		}
		// One-time data migrations (recorded in bkt_data_migrations). They
		// run before InitializeStartupBuckets, so S3_BUCKETS buckets that
		// already exist are recognized by name.
		return runDataMigrations(conn, cfg.Storage.S3.Buckets)
	}); err != nil {
		return err
	}
	if AdvisoryDB != nil {
		_ = AdvisoryDB.Close()
	}
	AdvisoryDB = lockDB
	lockDBInstalled = true

	return nil
}

func runMigrations(db *gorm.DB) error {
	// Run auto migrations.
	// NOTE: AutoMigrate is additive only — it cannot drop/rename columns, change
	// types lossily, or backfill data. For destructive/transforming schema
	// changes, introduce a versioned migration tool (golang-migrate/goose) with
	// up/down pairs; this advisory-locked AutoMigrate is the interim safeguard.
	err := db.AutoMigrate(
		&models.User{},
		&models.AccessKey{},
		&models.S3Configuration{},
		&models.Bucket{},
		&models.Object{},
		&models.Policy{},
		&models.BucketPolicy{},
		&models.AuditLog{},
		&models.IdempotencyKey{},
		&models.Upload{},
		&models.RevokedToken{},
		&models.MultipartUpload{},
		&models.ObjectVersion{},
		&models.Group{},
		&models.GroupSSOLink{},
	)

	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	// Drop the audit_logs→users FK if an earlier build created it: the audit
	// trail must outlive its users, and the constraint made every user with any
	// audit row (i.e. anyone who ever logged in) undeletable. AutoMigrate never
	// drops constraints, so remove it explicitly.
	if err := db.Exec(`ALTER TABLE audit_logs DROP CONSTRAINT IF EXISTS fk_audit_logs_user`).Error; err != nil {
		logger.Warn("Failed to drop audit_logs user FK", map[string]interface{}{"error": err.Error()})
	}

	// One link per (group, IdP group name) regardless of case: matching is
	// case-insensitive, so "Eng" and "eng" on the same group would be the
	// same link. GORM tags cannot express an expression index.
	if err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_group_sso_links_group_name
		ON group_sso_links (group_id, LOWER(sso_group))`).Error; err != nil {
		return fmt.Errorf("failed to create group_sso_links unique index: %w", err)
	}

	logger.Info("Database migrations completed", nil)

	// Add custom indexes for performance (PostgreSQL-specific optimizations)
	// Create index for efficient LIKE prefix queries on object keys
	// Using text_pattern_ops operator class for better prefix matching performance
	err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_objects_key_pattern
		ON objects (bucket_id, key text_pattern_ops)
	`).Error
	if err != nil {
		// Log warning but don't fail - this is an optimization, not critical
		logger.Warn("Failed to create pattern index", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		logger.Info("Performance indexes created", nil)
	}

	return nil
}

// GetDB returns the database instance
func GetDB() *gorm.DB {
	return DB
}

// CleanupExpiredTokens deletes revoked tokens that have passed their expiry time
func CleanupExpiredTokens() error {
	return DB.Where("expires_at < NOW()").Delete(&models.RevokedToken{}).Error
}

// CleanupExpiredTemporaryKeys hard-deletes bkt-STS temporary access keys once
// they have expired (regular keys are soft-deleted for audit; temp keys are
// churn and would otherwise accumulate forever).
func CleanupExpiredTemporaryKeys() error {
	return DB.Where("temporary = ? AND expires_at IS NOT NULL AND expires_at < NOW()", true).
		Delete(&models.AccessKey{}).Error
}

// CleanupOldAuditLogs deletes audit records older than the retention window so
// the table doesn't grow unbounded. retentionDays <= 0 disables pruning.
func CleanupOldAuditLogs(retentionDays int) error {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	return DB.Where("created_at < ?", cutoff).Delete(&models.AuditLog{}).Error
}

// CleanupAbandonedMultipartUploads removes expired in-progress multipart upload
// rows AND reclaims their on-disk staging directories for the local backend.
// Previously only the DB rows were deleted, leaking the local `.multipart/<id>`
// directories (and, on real S3, in-flight parts — which should additionally be
// handled by an S3 lifecycle rule) forever.
func CleanupAbandonedMultipartUploads(cfg *config.Config) error {
	if err := DB.Where("expires_at < NOW() AND status = 'in-progress'").
		Delete(&models.MultipartUpload{}).Error; err != nil {
		return err
	}
	// Sweep the local multipart staging area: remove any upload directory that
	// no longer corresponds to a still-valid (in-progress, unexpired) upload.
	return sweepLocalMultipart(cfg)
}

func sweepLocalMultipart(cfg *config.Config) error {
	if cfg == nil || cfg.Storage.RootPath == "" {
		return nil
	}
	multipartRoot := filepath.Join(cfg.Storage.RootPath, ".multipart")
	entries, err := os.ReadDir(multipartRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return nil // best-effort; don't fail the cleanup cycle
	}

	// Set of upload IDs still valid (in-progress and not yet expired).
	// A failed query must NOT be treated as "no valid uploads" — that would
	// sweep every in-flight upload's parts on a transient DB error (the same
	// failure shape as the S3 listing mass-delete fixed in storage/s3.go).
	var valid []string
	if err := DB.Model(&models.MultipartUpload{}).
		Where("status = 'in-progress' AND expires_at >= NOW()").
		Pluck("upload_id", &valid).Error; err != nil {
		return fmt.Errorf("skipping multipart sweep, could not list valid uploads: %w", err)
	}
	validSet := make(map[string]struct{}, len(valid))
	for _, id := range valid {
		validSet[id] = struct{}{}
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, ok := validSet[e.Name()]; ok {
			continue
		}
		// Grace period: the staging directory is created moments before its
		// tracking row is inserted, so a dir with no DB row may belong to an
		// upload whose initiation is still in flight. Only sweep dirs that have
		// been untouched long enough that no legitimate initiation is pending.
		if info, ierr := e.Info(); ierr != nil || time.Since(info.ModTime()) < time.Hour {
			continue
		}
		_ = os.RemoveAll(filepath.Join(multipartRoot, e.Name()))
	}
	return nil
}
