package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"
)

// These subprocesses stop after the storage side of a current-object
// mutation and before its metadata commit. A fresh handler must still serve
// the last committed bytes and ETag. The tests intentionally remain red until
// immutable generations and atomic metadata pointers cover every path.
func generationCrashChild(t *testing.T) bool {
	t.Helper()
	mode := os.Getenv("BKT_GENERATION_CRASH_MODE")
	if mode == "" {
		return false
	}
	cfg := replicaConfig(t)
	bucketName := os.Getenv("BKT_REPLICA_BUCKET")
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	var err error
	switch mode {
	case "copy":
		err = backend.CopyObject(bucketName, "source", "dest")
	case "delete":
		err = backend.DeleteObject(bucketName, "dest")
	case "range":
		err = backend.PutObject(bucketName, "dest", strings.NewReader("new-bytes"), 9, "text/plain", nil)
	case "archive", "restore":
		var bucket models.Bucket
		if err = database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
			break
		}
		_, err = prepareVersionedWrite(backend, &bucket, "dest")
		if err == nil && mode == "restore" {
			versionID := os.Getenv("BKT_GENERATION_RESTORE_VERSION")
			var rc io.ReadCloser
			rc, err = backend.GetObjectVersion(bucketName, "dest", versionID)
			if err == nil {
				err = backend.PutObject(bucketName, "dest", rc, -1, "text/plain", nil)
				_ = rc.Close()
			}
		}
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	if err != nil {
		t.Fatalf("crash boundary %s: %v", mode, err)
	}
	os.Exit(79)
	return true
}

func runGenerationCrash(t *testing.T, cfg *config.Config, bucketName, mode, versionID string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestIntegrationGenerationCrash")
	cmd.Env = append(os.Environ(), "BKT_GENERATION_CRASH_MODE="+mode,
		"BKT_REPLICA_DB="+cfg.Database.DBName,
		"BKT_REPLICA_ROOT="+cfg.Storage.RootPath,
		"BKT_REPLICA_BUCKET="+bucketName,
		"BKT_GENERATION_RESTORE_VERSION="+versionID)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Run(); err == nil {
		t.Fatal("crash subprocess unexpectedly completed")
	} else if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 79 {
		t.Fatalf("crash subprocess %s: %v", mode, err)
	}
}

func generationFixture(t *testing.T) (*config.Config, models.User, models.Bucket, http.Handler) {
	t.Helper()
	cfg := itConfig(t)
	owner := mkUser(t, itName("gen"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	return cfg, owner, bucket, itS3Router(cfg, owner)
}

func TestIntegrationGenerationCrashCopy(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	s3PutString(t, router, bucket.Name, "source", "new-bytes")
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "copy", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("copy crash exposed uncommitted destination: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationGenerationCrashDelete(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "delete", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("delete crash lost committed destination: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationGenerationCrashRange(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "range", "")
	req := httptest.NewRequest(http.MethodGet, "/"+bucket.Name+"/dest", nil)
	req.Header.Set("Range", "bytes=0-2")
	after := httptest.NewRecorder()
	router.ServeHTTP(after, req)
	if after.Code != http.StatusPartialContent || after.Body.String() != "old" ||
		after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("range read after crash: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationGenerationCrashArchive(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg := itConfig(t)
	owner := mkUser(t, itName("gen-archive"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "archive", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("archive crash lost current version: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
	var versions int64
	if err := database.DB.Model(&models.ObjectVersion{}).Where("bucket_id = ? AND key = ?", bucket.ID, "dest").Count(&versions).Error; err != nil || versions != 0 {
		t.Errorf("uncommitted archived version visible: count=%d err=%v", versions, err)
	}
}

func TestIntegrationGenerationCrashRestore(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg := itConfig(t)
	owner := mkUser(t, itName("gen-restore"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	s3PutString(t, router, bucket.Name, "dest", "new-bytes")
	var old models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "dest").First(&old).Error; err != nil {
		t.Fatal(err)
	}
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "restore", old.VersionID)
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "new-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("restore crash exposed uncommitted version: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
	var versions int64
	if err := database.DB.Model(&models.ObjectVersion{}).Where("bucket_id = ? AND key = ?", bucket.ID, "dest").Count(&versions).Error; err != nil || versions != 1 {
		t.Errorf("restore crash changed version history: count=%d err=%v", versions, err)
	}
}
