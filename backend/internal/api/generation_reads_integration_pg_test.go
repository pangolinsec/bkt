package api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
)

func TestIntegrationS3MultipartPublishesHiddenGeneration(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-s3-mpu"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) {
		b.StorageBackend = "s3"
		b.Versioning = models.VersioningEnabled
		b.QuotaBytes = 1024 // exercise retry after upstream ListParts is consumed
	})
	oldGenerationID, oldVersionID := uuid.NewString(), uuid.NewString()
	if err := database.DB.Create(&models.Object{
		BucketID: bucket.ID, Key: "key", Size: 3, ContentType: "text/plain",
		ETag: "old-etag", StoragePath: "key", GenerationID: oldGenerationID, VersionID: oldVersionID,
	}).Error; err != nil {
		t.Fatal(err)
	}
	var hiddenPath string
	upstreamUploadID := "upstream-" + uuid.NewString()
	t.Cleanup(func() { database.DB.Where("upload_id = ?", upstreamUploadID).Delete(&models.MultipartUpload{}) })
	completed := false
	completeCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/"+bucket.Name+"/.bkt-generations/") {
			t.Errorf("multipart touched public physical key: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/"+oldGenerationID) {
			if r.Method != http.MethodGet {
				t.Errorf("unexpected old generation request: %s", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("ETag", `"old-etag"`)
			_, _ = w.Write([]byte("old"))
			return
		}
		if hiddenPath != "" && hiddenPath != r.URL.Path {
			t.Errorf("generation key changed from %s to %s", hiddenPath, r.URL.Path)
		}
		hiddenPath = r.URL.Path
		q, _ := url.ParseQuery(r.URL.RawQuery)
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>` + upstreamUploadID + `</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && q.Get("partNumber") == "1":
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "new" {
				t.Errorf("part body: %q %v", body, err)
			}
			w.Header().Set("ETag", `"part-etag"`)
		case r.Method == http.MethodPost && q.Get("uploadId") == upstreamUploadID:
			completeCalls++
			if completed {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("multipart completion lacked create-only condition")
			}
			completed = true
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"committed-etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodHead:
			if !completed {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"committed-etag"`)
			w.Header().Set("Content-Length", "3")
		case r.Method == http.MethodGet && q.Get("uploadId") == upstreamUploadID:
			if completed {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>3</Size><ETag>"part-etag"</ETag></Part></ListPartsResult>`))
		case r.Method == http.MethodGet:
			w.Header().Set("ETag", `"committed-etag"`)
			_, _ = w.Write([]byte("new"))
		default:
			t.Errorf("unexpected upstream request: %s %s", r.Method, r.URL.RawQuery)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID = "fixture-access"
	cfg.Storage.S3.SecretAccessKey = "fixture-secret"
	cfg.Storage.S3.UseSSL = false
	cfg.Storage.S3.ForcePathStyle = true
	router := itS3Router(cfg, owner)
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "old" {
		t.Fatalf("prior generation read: %d %q", status, body)
	}
	start := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploads", nil)
	if start.Code != http.StatusOK {
		t.Fatalf("init: %d %s", start.Code, start.Body.String())
	}
	var init InitiateMultipartUploadResult
	if err := xml.Unmarshal(start.Body.Bytes(), &init); err != nil || init.UploadId != upstreamUploadID {
		t.Fatalf("init XML: %+v %v", init, err)
	}
	part := itDo(router, http.MethodPut, "/"+bucket.Name+"/key?partNumber=1&uploadId="+init.UploadId, []byte("new"))
	if part.Code != http.StatusOK {
		t.Fatalf("part: %d %s", part.Code, part.Body.String())
	}
	complete := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header().Get("ETag")))
	priorHook := generationCommitBoundary
	defer func() { generationCommitBoundary = priorHook }()
	generationCommitBoundary = func(boundary string) {
		if boundary == "multipart" {
			panic("simulated API crash after upstream completion")
		}
	}
	crashed := false
	func() {
		defer func() { crashed = recover() != nil }()
		itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+init.UploadId, complete)
	}()
	if !crashed || !completed {
		t.Fatalf("API did not stop after upstream completion: crashed=%v upstream-completed=%v", crashed, completed)
	}
	generationCommitBoundary = priorHook
	var tracking models.MultipartUpload
	if err := database.DB.Where("upload_id = ?", init.UploadId).First(&tracking).Error; err != nil || tracking.Status != "in-progress" {
		t.Fatalf("upstream-completed upload lost retry state: %+v %v", tracking, err)
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "old" {
		t.Fatalf("unpublished upstream generation became visible: %d %q", status, body)
	}
	if w := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+init.UploadId, complete); w.Code != http.StatusOK {
		t.Fatalf("retry after consumed upstream upload: %d %s", w.Code, w.Body.String())
	}
	if completeCalls != 1 {
		t.Errorf("retry reissued a consumed upstream completion: %d calls", completeCalls)
	}
	if err := database.DB.Where("upload_id = ?", init.UploadId).First(&tracking).Error; err != nil || tracking.Status != "completed" {
		t.Fatalf("committed generation left upload active: %+v %v", tracking, err)
	}
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&current).Error; err != nil || current.GenerationID == "" || current.GenerationID == oldGenerationID {
		t.Fatalf("current generation: %+v %v", current, err)
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, "key", oldVersionID).First(&archived).Error; err != nil || archived.GenerationID != oldGenerationID {
		t.Fatalf("archived previous generation: %+v %v", archived, err)
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "new" {
		t.Errorf("committed generation read: %d %q", status, body)
	}
	if w := itDo(router, http.MethodGet, "/"+bucket.Name+"/key?versionId="+oldVersionID, nil); w.Code != http.StatusOK || w.Body.String() != "old" {
		t.Errorf("archived generation read: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationS3PutStreamsIntoHiddenGeneration(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-s3-put"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	var saved []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/"+bucket.Name+"/.bkt-generations/") {
			t.Errorf("PUT touched public physical key: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("generation PUT lacked create-only condition")
			}
			saved, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"generated-etag"`)
		case http.MethodGet:
			if saved == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(saved)
		default:
			t.Errorf("unexpected upstream request: %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID = "fixture-access"
	cfg.Storage.S3.SecretAccessKey = "fixture-secret"
	cfg.Storage.S3.ForcePathStyle = true
	router := itS3Router(cfg, owner)
	put := itDo(router, http.MethodPut, "/"+bucket.Name+"/key", []byte("bytes"))
	if put.Code != http.StatusOK {
		t.Fatalf("S3-backed PUT: %d %s", put.Code, put.Body.String())
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "bytes" {
		t.Errorf("S3-backed GET: %d %q", status, body)
	}
}

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

func TestIntegrationUploadPartCopyReadsCommittedSourceGeneration(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-part-copy"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "source", "source-bytes")
	started := itDo(router, http.MethodPost, "/"+bucket.Name+"/dest?uploads", nil)
	if started.Code != http.StatusOK {
		t.Fatalf("init multipart: %d %s", started.Code, started.Body.String())
	}
	var init InitiateMultipartUploadResult
	if err := xml.Unmarshal(started.Body.Bytes(), &init); err != nil || init.UploadId == "" {
		t.Fatalf("init multipart XML: %v %+v", err, init)
	}
	req := httptest.NewRequest(http.MethodPut, "/"+bucket.Name+"/dest?partNumber=1&uploadId="+init.UploadId, nil)
	req.Header.Set("x-amz-copy-source", "/"+bucket.Name+"/source")
	req.Header.Set("x-amz-copy-source-range", "bytes=0-5")
	part := httptest.NewRecorder()
	router.ServeHTTP(part, req)
	if part.Code != http.StatusOK {
		t.Fatalf("copy part from committed generation: %d %s", part.Code, part.Body.String())
	}
}

func TestIntegrationMultipartCompletePublishesGenerationWithoutReplacingCurrent(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-multipart"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "key", "old")
	var previous models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&previous).Error; err != nil {
		t.Fatal(err)
	}
	started := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploads", nil)
	if started.Code != http.StatusOK {
		t.Fatalf("init multipart: %d %s", started.Code, started.Body.String())
	}
	var init InitiateMultipartUploadResult
	if err := xml.Unmarshal(started.Body.Bytes(), &init); err != nil || init.UploadId == "" {
		t.Fatalf("init multipart XML: %v %+v", err, init)
	}
	path := "/" + bucket.Name + "/key?uploadId=" + init.UploadId
	part := itDo(router, http.MethodPut, "/"+bucket.Name+"/key?partNumber=1&uploadId="+init.UploadId, []byte("new"))
	if part.Code != http.StatusOK {
		t.Fatalf("upload part: %d %s", part.Code, part.Body.String())
	}
	complete := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header().Get("ETag")))
	if w := itDo(router, http.MethodPost, path, complete); w.Code != http.StatusOK {
		t.Fatalf("complete multipart: %d %s", w.Code, w.Body.String())
	}
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.GenerationID == "" || current.GenerationID == previous.GenerationID {
		t.Errorf("multipart did not publish a distinct current generation: before=%q after=%q", previous.GenerationID, current.GenerationID)
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, "key", previous.VersionID).First(&archived).Error; err != nil {
		t.Fatal(err)
	}
	if archived.GenerationID != previous.GenerationID {
		t.Errorf("archived pointer = %q, want %q", archived.GenerationID, previous.GenerationID)
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "new" {
		t.Errorf("multipart current bytes: %d %q", status, body)
	}
	if w := itDo(router, http.MethodGet, "/"+bucket.Name+"/key?versionId="+previous.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "old" {
		t.Errorf("multipart prior bytes: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationMultipartRejectsMismatchedPartETag(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-multipart-etag"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "key", "old")
	start := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploads", nil)
	if start.Code != http.StatusOK {
		t.Fatalf("init: %d %s", start.Code, start.Body.String())
	}
	var upload InitiateMultipartUploadResult
	if err := xml.Unmarshal(start.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	part := itDo(router, http.MethodPut, "/"+bucket.Name+"/key?partNumber=1&uploadId="+upload.UploadId, []byte("new"))
	if part.Code != http.StatusOK {
		t.Fatalf("part: %d %s", part.Code, part.Body.String())
	}
	complete := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>wrong-etag</ETag></Part></CompleteMultipartUpload>`)
	w := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+upload.UploadId, complete)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "InvalidPart") {
		t.Fatalf("mismatched part ETag accepted: %d %s", w.Code, w.Body.String())
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "old" {
		t.Errorf("mismatched part changed current: %d %q", status, body)
	}
}

func TestIntegrationMultipartTrackingFailureRollsBackCurrentAndRetries(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-multipart-tracking"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "key", "old")
	start := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploads", nil)
	if start.Code != http.StatusOK {
		t.Fatalf("init: %d %s", start.Code, start.Body.String())
	}
	var upload InitiateMultipartUploadResult
	if err := xml.Unmarshal(start.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	part := itDo(router, http.MethodPut, "/"+bucket.Name+"/key?partNumber=1&uploadId="+upload.UploadId, []byte("new"))
	if part.Code != http.StatusOK {
		t.Fatalf("part: %d %s", part.Code, part.Body.String())
	}
	complete := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header().Get("ETag")))
	priorHook := generationCommitBoundary
	defer func() { generationCommitBoundary = priorHook }()
	generationCommitBoundary = func(boundary string) {
		if boundary != "multipart" {
			return
		}
		if err := database.DB.Model(&models.MultipartUpload{}).
			Where("upload_id = ?", upload.UploadId).Update("status", "aborted").Error; err != nil {
			t.Fatal(err)
		}
	}
	if w := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+upload.UploadId, complete); w.Code != http.StatusInternalServerError {
		t.Fatalf("tracking failure published object: %d %s", w.Code, w.Body.String())
	}
	generationCommitBoundary = priorHook
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "old" {
		t.Fatalf("failed tracking transaction changed current: %d %q", status, body)
	}
	var versions int64
	if err := database.DB.Model(&models.ObjectVersion{}).Where("bucket_id = ? AND key = ?", bucket.ID, "key").Count(&versions).Error; err != nil || versions != 0 {
		t.Fatalf("failed transaction archived current: count=%d err=%v", versions, err)
	}
	if err := database.DB.Model(&models.MultipartUpload{}).
		Where("upload_id = ?", upload.UploadId).Update("status", "in-progress").Error; err != nil {
		t.Fatal(err)
	}
	if w := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+upload.UploadId, complete); w.Code != http.StatusOK {
		t.Fatalf("retry failed tracking transaction: %d %s", w.Code, w.Body.String())
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "new" {
		t.Errorf("retried transaction bytes: %d %q", status, body)
	}
}

func TestIntegrationMultipartPartReplacementWaitsForCompletion(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-multipart-race"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	start := itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploads", nil)
	if start.Code != http.StatusOK {
		t.Fatalf("init: %d %s", start.Code, start.Body.String())
	}
	var upload InitiateMultipartUploadResult
	if err := xml.Unmarshal(start.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	partPath := "/" + bucket.Name + "/key?partNumber=1&uploadId=" + upload.UploadId
	part := itDo(router, http.MethodPut, partPath, []byte("first"))
	if part.Code != http.StatusOK {
		t.Fatalf("part: %d %s", part.Code, part.Body.String())
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	priorHook := generationCommitBoundary
	generationCommitBoundary = func(boundary string) {
		if boundary == "multipart" {
			close(entered)
			<-release
		}
	}
	defer func() { generationCommitBoundary = priorHook }()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	completeBody := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header().Get("ETag")))
	completed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		completed <- itDo(router, http.MethodPost, "/"+bucket.Name+"/key?uploadId="+upload.UploadId, completeBody)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("multipart completion did not reach commit boundary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPut, partPath, strings.NewReader("later")).WithContext(ctx)
	blocked := httptest.NewRecorder()
	router.ServeHTTP(blocked, req)
	if blocked.Code == http.StatusOK {
		t.Fatal("same-part replacement completed while multipart commit held its lock")
	}
	close(release)
	if w := <-completed; w.Code != http.StatusOK {
		t.Fatalf("complete after part lock wait: %d %s", w.Code, w.Body.String())
	}
	if status, body := s3GetString(t, router, bucket.Name, "key"); status != http.StatusOK || body != "first" {
		t.Errorf("committed bytes changed by racing part replacement: %d %q", status, body)
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
