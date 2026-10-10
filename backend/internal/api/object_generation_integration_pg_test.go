package api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/models"
)

// These subprocesses enter real API handlers and exit at their storage/DB
// boundary. A fresh handler must still serve the last committed bytes/ETag.
func generationCrashChild(t *testing.T) bool {
	t.Helper()
	mode := os.Getenv("BKT_GENERATION_CRASH_MODE")
	if mode == "" {
		return false
	}
	cfg := replicaConfig(t)
	bucketName := os.Getenv("BKT_REPLICA_BUCKET")
	var bucket models.Bucket
	if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
		t.Fatal(err)
	}
	owner := models.User{ID: bucket.OwnerID, IsAdmin: true}
	target := mode
	if mode == "range" {
		target = "put"
	}
	generationCommitBoundary = func(boundary string) {
		if boundary == target {
			os.Exit(79)
		}
	}
	s3 := itS3Router(cfg, owner)
	switch mode {
	case "copy":
		req := httptest.NewRequest(http.MethodPut, "/"+bucketName+"/dest", nil)
		req.Header.Set("x-amz-copy-source", "/"+bucketName+"/source")
		s3.ServeHTTP(httptest.NewRecorder(), req)
	case "delete":
		s3.DELETE("/:bucket/*key", NewS3APIHandler(cfg).DeleteObject)
		itDo(s3, http.MethodDelete, "/"+bucketName+"/dest", nil)
	case "range", "archive":
		itDo(s3, http.MethodPut, "/"+bucketName+"/dest", []byte("new-bytes"))
	case "restore":
		console := itRouter(owner)
		console.POST("/:name/objects/restore", NewBucketHandler(cfg).RestoreObjectVersion)
		payload := []byte(`{"key":"dest","version_id":"` + os.Getenv("BKT_GENERATION_RESTORE_VERSION") + `"}`)
		itDo(console, http.MethodPost, "/"+bucketName+"/objects/restore", payload)
	case "multipart":
		start := itDo(s3, http.MethodPost, "/"+bucketName+"/dest?uploads", nil)
		if start.Code != http.StatusOK {
			t.Fatalf("start multipart: %d %s", start.Code, start.Body.String())
		}
		var upload InitiateMultipartUploadResult
		if err := xml.Unmarshal(start.Body.Bytes(), &upload); err != nil || upload.UploadId == "" {
			t.Fatalf("parse multipart start: %v %+v", err, upload)
		}
		part := itDo(s3, http.MethodPut, "/"+bucketName+"/dest?partNumber=1&uploadId="+upload.UploadId, []byte("new-bytes"))
		if part.Code != http.StatusOK {
			t.Fatalf("upload part: %d %s", part.Code, part.Body.String())
		}
		body := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header().Get("ETag")))
		itDo(s3, http.MethodPost, "/"+bucketName+"/dest?uploadId="+upload.UploadId, body)
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	t.Fatalf("production %s crash boundary was not reached", mode)
	return true
}

func TestIntegrationGenerationCrashMultipart(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "multipart", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("multipart crash exposed uncommitted bytes: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
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
