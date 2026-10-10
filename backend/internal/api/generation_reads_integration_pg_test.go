package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
)

func TestIntegrationGenerationPointersResolveCurrentRangeConsoleAndHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-read"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, bucket.Name, "key", "legacy-old")
	if status, body := s3GetString(t, s3, bucket.Name, "key"); status != http.StatusOK || body != "legacy-old" {
		t.Fatalf("legacy row stopped reading: %d %q", status, body)
	}
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	currentID, historyID := uuid.NewString(), uuid.NewString()
	currentInfo, err := backend.PutGeneration(bucket.Name, "key", currentID, strings.NewReader("new-bytes"), 9, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	historyInfo, err := backend.PutGeneration(bucket.Name, "key", historyID, strings.NewReader("history"), 7, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", bucket.ID, "key").Updates(map[string]interface{}{
		"generation_id": currentID, "e_tag": currentInfo.ETag, "size": currentInfo.Size,
	}).Error; err != nil {
		t.Fatal(err)
	}
	versionID := uuid.NewString()
	if err := database.DB.Create(&models.ObjectVersion{
		BucketID: bucket.ID, Key: "key", VersionID: versionID, GenerationID: historyID,
		Size: historyInfo.Size, ETag: historyInfo.ETag, ContentType: "text/plain",
		VersionedAt: time.Now(), ContentModifiedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if status, body := s3GetString(t, s3, bucket.Name, "key"); status != http.StatusOK || body != "new-bytes" {
		t.Errorf("current generation: %d %q", status, body)
	}
	req := httptest.NewRequest(http.MethodGet, "/"+bucket.Name+"/key", nil)
	req.Header.Set("Range", "bytes=0-2")
	rangeResponse := httptest.NewRecorder()
	s3.ServeHTTP(rangeResponse, req)
	if rangeResponse.Code != http.StatusPartialContent || rangeResponse.Body.String() != "new" || rangeResponse.Header().Get("ETag") != `"`+currentInfo.ETag+`"` {
		t.Errorf("current range: %d %q ETag=%q", rangeResponse.Code, rangeResponse.Body.String(), rangeResponse.Header().Get("ETag"))
	}
	console := itRouter(owner)
	console.GET("/:name/objects/*key", NewBucketHandler(cfg).DownloadObject)
	if w := itDo(console, http.MethodGet, "/"+bucket.Name+"/objects/key", nil); w.Code != http.StatusOK || w.Body.String() != "new-bytes" {
		t.Errorf("console current generation: %d %q", w.Code, w.Body.String())
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/key?versionId="+versionID, nil); w.Code != http.StatusOK || w.Body.String() != "history" {
		t.Errorf("historical generation: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationConsoleRestorePublishesNewGenerationAndPreservesHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-restore"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, bucket.Name, "key", "old")
	s3PutString(t, s3, bucket.Name, "key", "new")
	var prior models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&prior).Error; err != nil {
		t.Fatal(err)
	}
	var old models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&old).Error; err != nil {
		t.Fatal(err)
	}
	console := itRouter(owner)
	console.POST("/:name/objects/restore", NewBucketHandler(cfg).RestoreObjectVersion)
	body := []byte(fmt.Sprintf(`{"key":"key","version_id":"%s"}`, old.VersionID))
	if w := itDo(console, http.MethodPost, "/"+bucket.Name+"/objects/restore", body); w.Code != http.StatusOK {
		t.Fatalf("restore: %d %s", w.Code, w.Body.String())
	}
	var restored models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&restored).Error; err != nil {
		t.Fatal(err)
	}
	if restored.GenerationID == "" || restored.GenerationID == prior.GenerationID || restored.GenerationID == old.GenerationID {
		t.Errorf("restore did not publish a new generation: prior=%q old=%q restored=%q", prior.GenerationID, old.GenerationID, restored.GenerationID)
	}
	if status, got := s3GetString(t, s3, bucket.Name, "key"); status != http.StatusOK || got != "old" {
		t.Errorf("restored current bytes: %d %q", status, got)
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/key?versionId="+old.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "old" {
		t.Errorf("requested history changed: %d %q", w.Code, w.Body.String())
	}
	var archivedCurrent models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, "key", prior.VersionID).First(&archivedCurrent).Error; err != nil {
		t.Fatal(err)
	}
	if archivedCurrent.GenerationID != prior.GenerationID {
		t.Errorf("prior current archived pointer = %q, want %q", archivedCurrent.GenerationID, prior.GenerationID)
	}
}

func TestIntegrationS3PutCommitsImmutableCurrentAndHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-write"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "key", "first")
	var first models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if first.GenerationID == "" {
		t.Fatal("first PUT did not commit an immutable generation")
	}
	s3PutString(t, router, bucket.Name, "key", "second")
	var second models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&second).Error; err != nil {
		t.Fatal(err)
	}
	if second.GenerationID == "" || second.GenerationID == first.GenerationID {
		t.Errorf("second PUT did not publish a distinct generation: first=%q second=%q", first.GenerationID, second.GenerationID)
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&archived).Error; err != nil {
		t.Fatal(err)
	}
	if archived.GenerationID != first.GenerationID {
		t.Errorf("archived pointer = %q, want %q", archived.GenerationID, first.GenerationID)
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "second" {
		t.Errorf("current bytes: %d %q", status, body)
	}
	if w := itDo(router, http.MethodGet, "/"+bucket.Name+"/key?versionId="+archived.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "first" {
		t.Errorf("archived bytes: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationS3PutArchivesLegacyCurrentWithoutMovingCommittedBytes(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-legacy"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := backend.PutObject(bucket.Name, "key", strings.NewReader("legacy"), 6, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	info, err := backend.GetObjectInfo(bucket.Name, "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := upsertCurrentObject(&models.Object{BucketID: bucket.ID, Key: "key", Size: 6, ETag: info.ETag, ContentType: "text/plain", StoragePath: "key"}); err != nil {
		t.Fatal(err)
	}
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "key", "replacement")
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&archived).Error; err != nil {
		t.Fatal(err)
	}
	if archived.GenerationID == "" {
		t.Fatal("legacy current was archived without an immutable generation")
	}
	if w := itDo(router, http.MethodGet, "/"+bucket.Name+"/key?versionId="+archived.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "legacy" {
		t.Errorf("archived legacy bytes: %d %q", w.Code, w.Body.String())
	}
}
