package api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
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
	case "console":
		consoleUpload(t, itConsoleRouter(cfg, owner), bucketName, "dest", "new-bytes")
	case "console-delete":
		itDo(itConsoleRouter(cfg, owner), http.MethodDelete, objectURL(bucketName, "dest"), nil)
	case "console-move":
		console := itConsoleRouter(cfg, owner)
		console.POST("/api/buckets/:name/objects/move", NewBucketHandler(cfg).MoveObject)
		itDo(console, http.MethodPost, "/api/buckets/"+bucketName+"/objects/move", []byte(`{"source_key":"source","destination_key":"dest"}`))
	case "folder-delete":
		itDo(itConsoleRouter(cfg, owner), http.MethodDelete, folderURL(bucketName, "folder/"), nil)
	case "lifecycle-expire":
		expireCurrentObject(storage.NewLocalStorage(cfg.Storage.RootPath), &bucket, "dest", time.Now().Add(time.Minute))
	case "replication-delete":
		mirrorDelete(storage.NewLocalStorage(cfg.Storage.RootPath), &models.Bucket{ID: uuid.New()}, &bucket, "dest")
	case "replication-copy":
		var source models.Bucket
		if err := database.DB.Where("name = ?", os.Getenv("BKT_REPLICA_SOURCE_BUCKET")).First(&source).Error; err != nil {
			t.Fatal(err)
		}
		backend := storage.NewLocalStorage(cfg.Storage.RootPath)
		replicateObject(backend, backend, &source, &bucket, "dest")
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
	// The crash must not consume the only copy of uploaded parts. The same
	// tracked upload ID must be completable after the failed publication.
	var upload models.MultipartUpload
	if err := database.DB.Where("bucket_name = ? AND object_key = ? AND status = ?", bucket.Name, "dest", "in-progress").First(&upload).Error; err != nil {
		t.Fatal(err)
	}
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	parts, err := backend.ListParts(bucket.Name, "dest", upload.UploadID)
	if err != nil || len(parts) != 1 {
		t.Fatalf("crashed multipart parts unavailable for retry: %+v %v", parts, err)
	}
	complete := []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, parts[0].ETag))
	if w := itDo(router, http.MethodPost, "/"+bucket.Name+"/dest?uploadId="+upload.UploadID, complete); w.Code != http.StatusOK {
		t.Fatalf("retry crashed multipart upload: %d %s", w.Code, w.Body.String())
	}
	if status, body := s3GetString(t, router, bucket.Name, "dest"); status != http.StatusOK || body != "new-bytes" {
		t.Errorf("retried multipart bytes: %d %q", status, body)
	}
}

func TestIntegrationGenerationCrashConsoleUpload(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	s3PutString(t, router, bucket.Name, "dest", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "console", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("console crash exposed uncommitted bytes: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationGenerationCrashConsoleDelete(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, router := generationFixture(t)
	// A pre-migration row still points at mutable public-key bytes. Deleting
	// those bytes before removing the row is the original crash-consistency gap.
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := backend.PutObject(bucket.Name, "dest", bytes.NewReader([]byte("old-bytes")), 9, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	info, err := backend.GetObjectInfo(bucket.Name, "dest")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Create(&models.Object{
		BucketID: bucket.ID, Key: "dest", Size: 9, ContentType: "text/plain", ETag: info.ETag, StoragePath: "dest",
	}).Error; err != nil {
		t.Fatal(err)
	}
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, "console-delete", "")
	after := itDo(router, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("console delete crash lost committed bytes: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationConsoleDeleteRetainsGenerationForReaders(t *testing.T) {
	cfg, owner, bucket, s3 := generationFixture(t)
	s3PutString(t, s3, bucket.Name, "dest", "old-bytes")
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "dest").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if w := itDo(itConsoleRouter(cfg, owner), http.MethodDelete, objectURL(bucket.Name, "dest"), nil); w.Code != http.StatusOK {
		t.Fatalf("console delete: %d %s", w.Code, w.Body.String())
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/dest", nil); w.Code != http.StatusNotFound {
		t.Errorf("deleted current remained visible: %d %s", w.Code, w.Body.String())
	}
	rc, err := backend.GetGeneration(bucket.Name, "dest", current.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if bytes, err := io.ReadAll(rc); err != nil || string(bytes) != "old-bytes" {
		t.Errorf("deleted generation lost before reader-safe reclamation: %q %v", bytes, err)
	}
}

func TestIntegrationDeleteBucketKeepsLegacyBytesWhenMetadataCommitFails(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("bucket-rollback"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, nil)
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	content := []byte("legacy-bytes")
	if err := backend.PutObject(bucket.Name, "legacy", bytes.NewReader(content), int64(len(content)), "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	info, err := backend.GetObjectInfo(bucket.Name, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Create(&models.Object{
		BucketID: bucket.ID, Key: "legacy", Size: info.Size, ContentType: "text/plain",
		ETag: info.ETag, StoragePath: "legacy",
	}).Error; err != nil {
		t.Fatal(err)
	}

	// Force failure at the bucket row commit after DeleteBucket has visited
	// storage. The database transaction will roll back all metadata deletes.
	if err := database.DB.Exec(`CREATE FUNCTION bkt_test_block_bucket_delete() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected bucket delete failure'; END;
		$$ LANGUAGE plpgsql`).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.DB.Exec("DROP FUNCTION IF EXISTS bkt_test_block_bucket_delete()") })
	if err := database.DB.Exec(`CREATE TRIGGER bkt_test_block_bucket_delete
		BEFORE DELETE ON buckets FOR EACH ROW
		EXECUTE FUNCTION bkt_test_block_bucket_delete()`).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.DB.Exec("DROP TRIGGER IF EXISTS bkt_test_block_bucket_delete ON buckets") })

	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	result := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil)
	if result.Code != http.StatusInternalServerError {
		t.Fatalf("injected bucket delete failure: %d %s", result.Code, result.Body.String())
	}
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "legacy").First(&current).Error; err != nil {
		t.Fatalf("metadata should roll back with bucket delete: %v", err)
	}
	var pending int64
	if err := database.DB.Model(&models.LocalBucketCleanup{}).Where("id = ?", bucket.ID).Count(&pending).Error; err != nil || pending != 0 {
		t.Fatalf("cleanup record must roll back with bucket delete: count=%d err=%v", pending, err)
	}
	rc, err := backend.GetObject(bucket.Name, "legacy")
	if err != nil {
		t.Fatalf("live legacy bytes were deleted before metadata commit: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(got, content) {
		t.Errorf("live legacy bytes changed after failed bucket delete: %q %v", got, err)
	}
}

func TestIntegrationGenerationCrashConsoleMove(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, s3 := generationFixture(t)
	s3PutString(t, s3, bucket.Name, "source", "old-bytes")
	runGenerationCrash(t, cfg, bucket.Name, "console-move", "")
	if status, body := s3GetString(t, s3, bucket.Name, "source"); status != http.StatusOK || body != "old-bytes" {
		t.Errorf("console move crash lost source: %d %q", status, body)
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/dest", nil); w.Code != http.StatusNotFound {
		t.Errorf("console move crash exposed destination: %d %s", w.Code, w.Body.String())
	}
}

func TestIntegrationConsoleMovePublishesDestinationAndSourceMarker(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-console-move"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, bucket.Name, "source", "old-bytes")
	var source models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "source").First(&source).Error; err != nil {
		t.Fatal(err)
	}
	console := itConsoleRouter(cfg, owner)
	console.POST("/api/buckets/:name/objects/move", NewBucketHandler(cfg).MoveObject)
	w := itDo(console, http.MethodPost, "/api/buckets/"+bucket.Name+"/objects/move", []byte(`{"source_key":"source","destination_key":"dest"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("console move: %d %s", w.Code, w.Body.String())
	}
	var dest models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "dest").First(&dest).Error; err != nil {
		t.Fatal(err)
	}
	if dest.GenerationID == "" || dest.GenerationID == source.GenerationID {
		t.Errorf("destination did not get its own immutable generation: src=%q dest=%q", source.GenerationID, dest.GenerationID)
	}
	if status, body := s3GetString(t, s3, bucket.Name, "dest"); status != http.StatusOK || body != "old-bytes" {
		t.Errorf("destination bytes: %d %q", status, body)
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/source", nil); w.Code != http.StatusNotFound {
		t.Errorf("moved source stayed current: %d %s", w.Code, w.Body.String())
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, "source", source.VersionID).First(&archived).Error; err != nil || archived.GenerationID != source.GenerationID {
		t.Fatalf("source old version: %+v %v", archived, err)
	}
	var marker models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND is_delete_marker = ?", bucket.ID, "source", true).First(&marker).Error; err != nil || !marker.VersionedAt.After(archived.VersionedAt) {
		t.Fatalf("source delete marker: %+v after %+v err=%v", marker, archived, err)
	}
}

func TestIntegrationGenerationCrashFolderDelete(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, _, bucket, s3 := generationFixture(t)
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := backend.PutObject(bucket.Name, "folder/a", bytes.NewReader([]byte("old-bytes")), 9, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	info, err := backend.GetObjectInfo(bucket.Name, "folder/a")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Create(&models.Object{
		BucketID: bucket.ID, Key: "folder/a", Size: 9, ContentType: "text/plain", ETag: info.ETag, StoragePath: "folder/a",
	}).Error; err != nil {
		t.Fatal(err)
	}
	before := itDo(s3, http.MethodHead, "/"+bucket.Name+"/folder/a", nil)
	runGenerationCrash(t, cfg, bucket.Name, "folder-delete", "")
	after := itDo(s3, http.MethodGet, "/"+bucket.Name+"/folder/a", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("folder delete crash lost committed bytes: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationGenerationCrashLifecycleExpiry(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	testGenerationCrashBackgroundDelete(t, "lifecycle-expire")
}

func TestIntegrationGenerationCrashReplicationDelete(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	testGenerationCrashBackgroundDelete(t, "replication-delete")
}

func testGenerationCrashBackgroundDelete(t *testing.T, mode string) {
	t.Helper()
	cfg, _, bucket, s3 := generationFixture(t)
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := backend.PutObject(bucket.Name, "dest", bytes.NewReader([]byte("old-bytes")), 9, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	info, err := backend.GetObjectInfo(bucket.Name, "dest")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Create(&models.Object{
		BucketID: bucket.ID, Key: "dest", Size: 9, ContentType: "text/plain", ETag: info.ETag, StoragePath: "dest",
	}).Error; err != nil {
		t.Fatal(err)
	}
	before := itDo(s3, http.MethodHead, "/"+bucket.Name+"/dest", nil)
	runGenerationCrash(t, cfg, bucket.Name, mode, "")
	after := itDo(s3, http.MethodGet, "/"+bucket.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("%s crash lost committed bytes: status=%d body=%q ETag=%q committed ETag=%q",
			mode, after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationBackgroundDeletesRetainGenerationForReaders(t *testing.T) {
	for _, mode := range []string{"lifecycle-expire", "replication-delete"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _, bucket, s3 := generationFixture(t)
			s3PutString(t, s3, bucket.Name, "dest", "old-bytes")
			var current models.Object
			if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "dest").First(&current).Error; err != nil {
				t.Fatal(err)
			}
			backend := storage.NewLocalStorage(cfg.Storage.RootPath)
			var removed, busy bool
			switch mode {
			case "lifecycle-expire":
				removed, busy = expireCurrentObject(backend, &bucket, "dest", time.Now().Add(time.Minute))
			case "replication-delete":
				removed, busy = mirrorDelete(backend, &models.Bucket{ID: uuid.New()}, &bucket, "dest")
			}
			if !removed || busy {
				t.Fatalf("%s did not remove current: removed=%v busy=%v", mode, removed, busy)
			}
			if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/dest", nil); w.Code != http.StatusNotFound {
				t.Errorf("%s left current visible: %d %s", mode, w.Code, w.Body.String())
			}
			rc, err := backend.GetGeneration(bucket.Name, "dest", current.GenerationID)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			content, err := io.ReadAll(rc)
			if err != nil || string(content) != "old-bytes" {
				t.Errorf("%s reclaimed a live reader's generation: %q %v", mode, content, err)
			}
		})
	}
}

func TestIntegrationGenerationCrashReplicationCopy(t *testing.T) {
	if generationCrashChild(t) {
		return
	}
	cfg, owner, dest, destS3 := generationFixture(t)
	source := itBucket(t, owner.ID, nil)
	sourceS3 := itS3Router(cfg, owner)
	s3PutString(t, sourceS3, source.Name, "dest", "new-bytes")
	s3PutString(t, destS3, dest.Name, "dest", "old-bytes")
	before := itDo(destS3, http.MethodHead, "/"+dest.Name+"/dest", nil)
	runGenerationCrashWithSource(t, cfg, dest.Name, source.Name, "replication-copy")
	after := itDo(destS3, http.MethodGet, "/"+dest.Name+"/dest", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" || after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("replication copy crash exposed uncommitted bytes: status=%d body=%q ETag=%q committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}

func TestIntegrationReplicationCopyPublishesTargetGenerationAndHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-replication-copy"), "password-123", true, false)
	source := itBucket(t, owner.ID, nil)
	dest := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, source.Name, "dest", "new-bytes")
	s3PutString(t, s3, dest.Name, "dest", "old-bytes")
	var sourceRow, oldDest models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", source.ID, "dest").First(&sourceRow).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.DB.Where("bucket_id = ? AND key = ?", dest.ID, "dest").First(&oldDest).Error; err != nil {
		t.Fatal(err)
	}
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if copied, busy := replicateObject(backend, backend, &source, &dest, "dest"); !copied || busy {
		t.Fatalf("replication copy: copied=%v busy=%v", copied, busy)
	}
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", dest.ID, "dest").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.GenerationID == "" || current.GenerationID == oldDest.GenerationID || current.GenerationID == sourceRow.GenerationID || current.ETag != sourceRow.ETag {
		t.Errorf("target did not get an independent generation of source bytes: source=%+v old=%+v current=%+v", sourceRow, oldDest, current)
	}
	if status, body := s3GetString(t, s3, dest.Name, "dest"); status != http.StatusOK || body != "new-bytes" {
		t.Errorf("replicated current bytes: %d %q", status, body)
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", dest.ID, "dest", oldDest.VersionID).First(&archived).Error; err != nil || archived.GenerationID != oldDest.GenerationID {
		t.Fatalf("replication archive: %+v %v", archived, err)
	}
	if w := itDo(s3, http.MethodGet, "/"+dest.Name+"/dest?versionId="+oldDest.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "old-bytes" {
		t.Errorf("replicated target history: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationConsoleUploadPublishesGenerationAndHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-console-upload"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, bucket.Name, "key", "old-bytes")
	var old models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&old).Error; err != nil {
		t.Fatal(err)
	}
	consoleUpload(t, itConsoleRouter(cfg, owner), bucket.Name, "key", "new-bytes")
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.GenerationID == "" || current.GenerationID == old.GenerationID {
		t.Fatalf("console upload did not publish a new generation: old=%q current=%q", old.GenerationID, current.GenerationID)
	}
	if status, body := s3GetString(t, s3, bucket.Name, "key"); status != http.StatusOK || body != "new-bytes" {
		t.Errorf("console current bytes: %d %q", status, body)
	}
	var archived models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ? AND version_id = ?", bucket.ID, "key", old.VersionID).First(&archived).Error; err != nil || archived.GenerationID != old.GenerationID {
		t.Fatalf("console upload lost old version: %+v %v", archived, err)
	}
	if w := itDo(s3, http.MethodGet, "/"+bucket.Name+"/key?versionId="+old.VersionID, nil); w.Code != http.StatusOK || w.Body.String() != "old-bytes" {
		t.Errorf("console previous version bytes: %d %q", w.Code, w.Body.String())
	}
}

func TestIntegrationAsyncConsoleUploadPublishesGenerationAndHistory(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("generation-console-async"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3 := itS3Router(cfg, owner)
	s3PutString(t, s3, bucket.Name, "key", "old-bytes")
	var old models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&old).Error; err != nil {
		t.Fatal(err)
	}
	console := itConsoleRouter(cfg, owner)
	handler := NewBucketHandler(cfg)
	console.POST("/api/buckets/:name/objects/async", handler.UploadObjectAsync)
	console.GET("/api/uploads/:id/status", handler.GetUploadStatus)
	var form bytes.Buffer
	mw := multipart.NewWriter(&form)
	if err := mw.WriteField("key", "key"); err != nil {
		t.Fatal(err)
	}
	fw, err := mw.CreateFormFile("file", "upload.bin")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte("new-bytes"))
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/buckets/"+bucket.Name+"/objects/async", &form)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	console.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("async init: %d %s", w.Code, w.Body.String())
	}
	var init struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("async init response: %+v %v", init, err)
	}
	t.Cleanup(func() { database.DB.Where("id = ?", init.UploadID).Delete(&models.Upload{}) })
	deadline := time.Now().Add(5 * time.Second)
	for {
		status := itDo(console, http.MethodGet, "/api/uploads/"+init.UploadID+"/status", nil)
		if status.Code != http.StatusOK {
			t.Fatalf("async status: %d %s", status.Code, status.Body.String())
		}
		var state models.UploadStatusResponse
		if err := json.Unmarshal(status.Body.Bytes(), &state); err != nil {
			t.Fatal(err)
		}
		if state.Status == models.UploadStatusCompleted {
			break
		}
		if state.Status == models.UploadStatusFailed || time.Now().After(deadline) {
			t.Fatalf("async generation upload never completed: %+v", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "key").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.GenerationID == "" || current.GenerationID == old.GenerationID || current.SHA256 == "" {
		t.Fatalf("async upload missing new generation or hash: old=%+v current=%+v", old, current)
	}
	if status, body := s3GetString(t, s3, bucket.Name, "key"); status != http.StatusOK || body != "new-bytes" {
		t.Errorf("async current bytes: %d %q", status, body)
	}
	if v := itDo(s3, http.MethodGet, "/"+bucket.Name+"/key?versionId="+old.VersionID, nil); v.Code != http.StatusOK || v.Body.String() != "old-bytes" {
		t.Errorf("async previous version bytes: %d %q", v.Code, v.Body.String())
	}
}

func runGenerationCrash(t *testing.T, cfg *config.Config, bucketName, mode, versionID string) {
	runGenerationCrashWithSourceAndVersion(t, cfg, bucketName, "", mode, versionID)
}

func runGenerationCrashWithSource(t *testing.T, cfg *config.Config, bucketName, sourceName, mode string) {
	runGenerationCrashWithSourceAndVersion(t, cfg, bucketName, sourceName, mode, "")
}

func runGenerationCrashWithSourceAndVersion(t *testing.T, cfg *config.Config, bucketName, sourceName, mode, versionID string) {
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
		"BKT_REPLICA_SOURCE_BUCKET="+sourceName,
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
