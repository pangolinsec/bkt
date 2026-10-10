package main

import (
	"bkt/internal/api"
	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/metrics"
	"bkt/internal/middleware"
	"bkt/internal/security"
	"bkt/internal/services"
	"bkt/internal/storage"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	// Gin runs in debug mode (route dump + "Running in debug mode" warning)
	// unless GIN_MODE is set. Default to release so production deployments are
	// quiet out of the box; set GIN_MODE=debug for local development.
	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	// Load configuration
	cfg := config.Load()
	log.Println("Configuration loaded")

	// Wait for database to be ready
	log.Println("Waiting for database to be ready...")
	time.Sleep(3 * time.Second)

	// Initialize database
	if err := database.Initialize(cfg); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Move credentials still encrypted under a decrypt-only key
	// (ENCRYPTION_KEY_PREVIOUS, the JWT_SECRET fallback,
	// ENCRYPTION_LEGACY_JWT_SECRET) or in the legacy v1 format to the current
	// ENCRYPTION_KEY, so retired keys can be dropped later. Runs in the
	// background: each value costs a PBKDF2 derivation.
	if getEnvBool("ENCRYPTION_REENCRYPT_ON_STARTUP", true) {
		go func() {
			st, err := security.ReencryptStoredSecrets(database.DB)
			if err != nil {
				log.Printf("ERROR: re-encrypting stored credentials stopped: %v (re-encrypted %d so far; will retry on next start)", err, st.Reencrypted)
				return
			}
			if st.Reencrypted > 0 || st.Failed > 0 || st.Skipped > 0 {
				log.Printf("Stored credential re-encryption: %d re-encrypted under the current ENCRYPTION_KEY, %d undecryptable (left unchanged), %d skipped (changed concurrently), %d examined",
					st.Reencrypted, st.Failed, st.Skipped, st.Scanned)
			}
			if st.Failed == 0 && st.Skipped == 0 && (os.Getenv("ENCRYPTION_KEY_PREVIOUS") != "" || os.Getenv("ENCRYPTION_LEGACY_JWT_SECRET") != "") {
				log.Printf("Stored credential re-encryption: no stored credential needs a decrypt-only key any more (%d examined). "+
					"ENCRYPTION_KEY_PREVIOUS / ENCRYPTION_LEGACY_JWT_SECRET can be removed once every replica runs this version.", st.Scanned)
			}
		}()
	}

	// Encrypt bucket webhook secrets still stored in plaintext (written before
	// secrets were sealed at rest). Idempotent; per-row compare-and-swap.
	go func() {
		if _, err := services.SealLegacyWebhookSecrets(database.DB); err != nil {
			log.Printf("ERROR: sealing legacy plaintext webhook secrets stopped: %v (will retry on next start)", err)
		}
	}()

	// Initialize default admin user
	if err := database.InitializeDefaultAdmin(cfg); err != nil {
		log.Fatalf("Failed to initialize default admin: %v", err)
	}

	// Auto-provision any S3 buckets declared in .env (S3_BUCKETS)
	if err := database.InitializeStartupBuckets(cfg); err != nil {
		log.Fatalf("Failed to provision startup S3 buckets: %v", err)
	}

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(cfg.Storage.RootPath, 0750); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}
	// MkdirAll succeeds on an existing root-owned volume (written by a
	// pre-uid-10001 image) — probe real writes/reads so it fails fast instead.
	if err := storage.ProbeWritable(cfg.Storage.RootPath); err != nil {
		log.Fatalf("Storage root check failed: %v", err)
	}
	// One-time conversion of local bucket storage from the legacy key-as-path
	// layout to the blob layout. Must finish before anything serves or
	// touches objects; it is resumable, so a failure stops startup and a
	// restart after fixing the cause continues where it stopped.
	if err := storage.MigrateLocalLayout(cfg.Storage.RootPath); err != nil {
		log.Fatalf("Local storage layout migration failed (resumable: fix the cause and restart): %v", err)
	}
	if err := api.ReconcileLocalBucketCleanups(cfg.Storage.RootPath); err != nil {
		log.Fatalf("Local bucket cleanup failed (resumable: fix the cause and restart): %v", err)
	}

	// Start Prometheus storage metrics collector (runs every 60s)
	metrics.StartStorageMetricsCollector()

	// Periodically clean up expired revoked tokens and abandoned multipart uploads (every 15 minutes)
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := database.CleanupExpiredTokens(); err != nil {
				log.Printf("Failed to clean up expired tokens: %v", err)
			}
			if err := database.CleanupAbandonedMultipartUploads(cfg); err != nil {
				log.Printf("Failed to clean up abandoned multipart uploads: %v", err)
			}
			if err := database.CleanupOldAuditLogs(auditRetentionDays()); err != nil {
				log.Printf("Failed to prune old audit logs: %v", err)
			}
			if err := database.CleanupExpiredTemporaryKeys(); err != nil {
				log.Printf("Failed to prune expired temporary keys: %v", err)
			}
		}
	}()

	// Mirror replicating buckets every 5 minutes.
	go func() {
		time.Sleep(45 * time.Second)
		api.RunReplicationSweep(cfg)
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			api.RunReplicationSweep(cfg)
		}
	}()

	// Apply bucket lifecycle rules hourly (age-based expiry of current
	// objects and noncurrent versions), with one pass shortly after startup
	// so restarts don't defer overdue expirations by up to an hour.
	go func() {
		time.Sleep(30 * time.Second)
		api.RunLifecycleSweep(cfg)
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			api.RunLifecycleSweep(cfg)
		}
	}()

	// Two listeners on the same process:
	//   - console: web UI (embedded) + REST API, browser-facing
	//   - s3:      S3-compatible API at root paths, for aws-cli / s3fs
	// S3 clients address buckets at the host root, so the S3 API cannot share a
	// listener with the UI — hence the split ports.
	consoleRouter := api.SetupConsoleRouter(cfg)
	s3Router := api.SetupS3Router(cfg)

	consoleAddr := cfg.Server.Host + ":" + cfg.Server.ConsolePort
	s3Addr := cfg.Server.Host + ":" + cfg.Server.S3APIPort

	if cfg.TLS.Enabled {
		log.Printf("TLS enabled (cert=%s key=%s)", cfg.TLS.CertFile, cfg.TLS.KeyFile)
	} else {
		// Production config validation rejects TLS_ENABLED=false unless
		// TLS_TERMINATED_UPSTREAM=true, so this only happens in dev or behind a
		// TLS-terminating proxy.
		if cfg.TLS.TerminatedUpstream {
			log.Println("TLS disabled on the listeners — serving plain HTTP behind a TLS-terminating proxy (TLS_TERMINATED_UPSTREAM=true)")
		} else {
			log.Println("TLS disabled — serving plain HTTP")
		}
	}

	consoleServer := startServer("console", consoleAddr, consoleRouter, cfg)
	s3Server := startServer("s3-api", s3Addr, s3Router, cfg)

	// Wait for interrupt signal to gracefully shut down both servers
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down servers...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consoleServer.Shutdown(ctx); err != nil {
		log.Printf("Console server forced to shutdown: %v", err)
	}
	if err := s3Server.Shutdown(ctx); err != nil {
		log.Printf("S3 server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

// getEnvBool parses a boolean env var, returning def when unset/invalid.
func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// auditRetentionDays returns the audit-log retention window in days
// (AUDIT_RETENTION_DAYS, default 90; 0 disables pruning).
func auditRetentionDays() int {
	if v := os.Getenv("AUDIT_RETENTION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 90
}

// startServer launches an http.Server in a goroutine, using TLS when enabled.
func startServer(name, addr string, handler http.Handler, cfg *config.Config) *http.Server {
	srv := &http.Server{
		Addr: addr,
		// Methods outside the fixed set the API serves are refused (501)
		// before routing, logging or metrics see them.
		Handler: middleware.RejectUnknownMethods(handler),
		// Slowloris protection. ReadTimeout/WriteTimeout are intentionally left
		// unset: this server streams arbitrarily large object bodies in both
		// directions, and a fixed whole-request deadline would abort legitimate
		// large transfers. ReadHeaderTimeout + IdleTimeout bound the cheap-to-
		// abuse phases without capping transfer duration.
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
		// Request line + headers. 64 KiB comfortably fits the largest
		// legitimate requests (AWS caps S3 user metadata at 2 KB and the
		// whole header block at 8 KB; presigned URLs with 1 KiB keys and
		// session cookies/JWTs are a few KB) while bounding per-connection
		// memory for unauthenticated clients.
		MaxHeaderBytes: 64 << 10,
	}
	go func() {
		if cfg.TLS.Enabled {
			log.Printf("Starting %s server (HTTPS) on %s", name, addr)
			if err := srv.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Failed to start %s server: %v", name, err)
			}
		} else {
			log.Printf("Starting %s server (HTTP) on %s", name, addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Failed to start %s server: %v", name, err)
			}
		}
	}()
	return srv
}
