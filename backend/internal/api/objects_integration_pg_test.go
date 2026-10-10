package api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
)

// Postgres-gated integration tests for object/bucket metadata paths that
// otherwise only have pure-helper coverage. See integration_pg_test.go.

func TestIntegrationUpsertCurrentObject(t *testing.T) {
	integrationDB(t)
	owner := mkUser(t, itName("own"), "password-123", false, false)
	b := itBucket(t, owner.ID, nil)

	meta := `{"a":"1"}`
	first := models.Object{BucketID: b.ID, Key: "dir/k", Size: 1, ContentType: "text/plain", ETag: "e1"}
	if err := upsertCurrentObject(&first); err != nil {
		t.Fatal(err)
	}
	if first.ID == uuid.Nil || first.CreatedAt.IsZero() || first.StoragePath != "dir/k" {
		t.Fatalf("insert returned %+v", first)
	}
	stored, ok := currentRow(t, b.ID, "dir/k")
	if !ok || stored.ID != first.ID || !stored.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("returned row %v/%v does not match stored %v/%v", first.ID, first.CreatedAt, stored.ID, stored.CreatedAt)
	}

	time.Sleep(5 * time.Millisecond)
	second := models.Object{BucketID: b.ID, Key: "dir/k", Size: 2, ContentType: "application/json", ETag: "e2",
		VersionID: "v2", Metadata: &meta}
	if err := upsertCurrentObject(&second); err != nil {
		t.Fatal(err)
	}
	// The overwrite keeps the row's identity and creation time and returns
	// the reloaded row, not the pre-insert struct (whose BeforeCreate id
	// never reached the table).
	if second.ID != first.ID {
		t.Errorf("overwrite changed id: %v -> %v", first.ID, second.ID)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("overwrite changed created_at: %v -> %v", first.CreatedAt, second.CreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Errorf("updated_at not advanced: %v -> %v", first.UpdatedAt, second.UpdatedAt)
	}
	if second.Size != 2 || second.ETag != "e2" || second.ContentType != "application/json" ||
		second.VersionID != "v2" || second.Metadata == nil {
		t.Errorf("overwrite did not update the row: %+v", second)
	}
	stored, _ = currentRow(t, b.ID, "dir/k")
	if stored.ID != second.ID || stored.Size != 2 || !stored.UpdatedAt.Equal(second.UpdatedAt) {
		t.Errorf("stored row %+v differs from returned %+v", stored, second)
	}

	// Concurrent writers of one key never fail on the unique index and leave
	// exactly one row.
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := models.Object{BucketID: b.ID, Key: "race", Size: int64(i), ETag: fmt.Sprint(i)}
			if err := upsertCurrentObject(&o); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent upsert: %v", err)
	}
	var n int64
	database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", b.ID, "race").Count(&n)
	if n != 1 {
		t.Errorf("%d rows for one key", n)
	}
}

func TestIntegrationS3PutContentHeadersAndConditions(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("s3owner"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	path := "/" + bucket.Name + "/sop.md"
	request := func(method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	initial := map[string]string{
		"Content-Type":        "text/markdown",
		"Content-Disposition": `attachment; filename="original.md"`,
		"Cache-Control":       "private, max-age=60",
		"If-None-Match":       "*",
	}
	first := request(http.MethodPut, path, "original", initial)
	if first.Code != http.StatusOK {
		t.Fatalf("conditional create: %d %s", first.Code, first.Body.String())
	}
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing create ETag")
	}
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		w := request(method, path, "", nil)
		if w.Code != http.StatusOK || w.Header().Get("Content-Disposition") != initial["Content-Disposition"] ||
			w.Header().Get("Cache-Control") != initial["Cache-Control"] {
			t.Errorf("%s persisted content headers: status=%d disposition=%q cache=%q", method,
				w.Code, w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
		}
	}
	if w := request(http.MethodPut, path, "duplicate", map[string]string{"If-None-Match": "*"}); w.Code != http.StatusPreconditionFailed {
		t.Errorf("duplicate create: %d %s, want 412", w.Code, w.Body.String())
	}
	if w := request(http.MethodPut, "/"+bucket.Name+"/missing.md", "bad", map[string]string{"If-Match": etag}); w.Code != http.StatusPreconditionFailed {
		t.Errorf("If-Match on missing key: %d %s, want 412", w.Code, w.Body.String())
	}
	if w := request(http.MethodGet, path, "", nil); w.Body.String() != "original" {
		t.Errorf("duplicate changed original bytes: %q", w.Body.String())
	}
	second := request(http.MethodPut, path, "edited", map[string]string{"If-Match": etag})
	if second.Code != http.StatusOK {
		t.Fatalf("current If-Match: %d %s", second.Code, second.Body.String())
	}
	if w := request(http.MethodPut, path, "stale", map[string]string{"If-Match": etag}); w.Code != http.StatusPreconditionFailed {
		t.Errorf("stale If-Match: %d %s, want 412", w.Code, w.Body.String())
	}
	if w := request(http.MethodGet, path, "", nil); w.Body.String() != "edited" {
		t.Errorf("stale overwrite changed bytes: %q", w.Body.String())
	}
	if w := request(http.MethodHead, path, "", nil); w.Header().Get("Content-Disposition") != "" ||
		w.Header().Get("Cache-Control") != "" {
		t.Errorf("overwrite retained old content headers: disposition=%q cache=%q",
			w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
	}
}

func TestIntegrationS3ContentHeadersAcrossCopyAndVersion(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("versionowner"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	path := "/" + bucket.Name + "/source.txt"
	do := func(method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	old := map[string]string{"Content-Disposition": `attachment; filename="old.txt"`, "Cache-Control": "private, max-age=60"}
	first := do(http.MethodPut, path, "old", old)
	if first.Code != http.StatusOK || first.Header().Get("x-amz-version-id") == "" {
		t.Fatalf("versioned source PUT: %d %s", first.Code, first.Body.String())
	}
	if w := do(http.MethodPut, path, "new", map[string]string{"Content-Disposition": `attachment; filename="new.txt"`, "Cache-Control": "no-cache"}); w.Code != http.StatusOK {
		t.Fatalf("versioned overwrite: %d %s", w.Code, w.Body.String())
	}
	archivedPath := path + "?versionId=" + first.Header().Get("x-amz-version-id")
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		w := do(method, archivedPath, "", nil)
		if w.Code != http.StatusOK || w.Header().Get("Content-Disposition") != old["Content-Disposition"] ||
			w.Header().Get("Cache-Control") != old["Cache-Control"] {
			t.Errorf("archived %s headers: status=%d disposition=%q cache=%q", method,
				w.Code, w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
		}
	}
	copyPath := "/" + bucket.Name + "/copy.txt"
	if w := do(http.MethodPut, copyPath, "", map[string]string{"x-amz-copy-source": path}); w.Code != http.StatusOK {
		t.Fatalf("CopyObject COPY: %d %s", w.Code, w.Body.String())
	}
	if w := do(http.MethodHead, copyPath, "", nil); w.Header().Get("Content-Disposition") != `attachment; filename="new.txt"` ||
		w.Header().Get("Cache-Control") != "no-cache" {
		t.Errorf("copy lost content headers: disposition=%q cache=%q", w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
	}
	replaced := map[string]string{"x-amz-copy-source": path, "x-amz-metadata-directive": "REPLACE",
		"Content-Disposition": `attachment; filename="replaced.txt"`, "Cache-Control": "max-age=0"}
	if w := do(http.MethodPut, copyPath, "", replaced); w.Code != http.StatusOK {
		t.Fatalf("CopyObject REPLACE: %d %s", w.Code, w.Body.String())
	}
	if w := do(http.MethodHead, copyPath, "", nil); w.Header().Get("Content-Disposition") != replaced["Content-Disposition"] ||
		w.Header().Get("Cache-Control") != replaced["Cache-Control"] {
		t.Errorf("copy replacement headers: disposition=%q cache=%q", w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
	}
}

func TestIntegrationS3ConcurrentCreateIfAbsent(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("raceowner"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	path := "/" + bucket.Name + "/same-id.md"
	var wg sync.WaitGroup
	statuses := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(fmt.Sprint(i)))
			req.Header.Set("If-None-Match", "*")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			statuses <- w.Code
		}(i)
	}
	wg.Wait()
	close(statuses)
	created, rejected := 0, 0
	for code := range statuses {
		switch code {
		case http.StatusOK:
			created++
		case http.StatusPreconditionFailed:
			rejected++
		default:
			t.Errorf("concurrent create status %d", code)
		}
	}
	if created != 1 || rejected != 7 {
		t.Errorf("concurrent create: %d accepted, %d rejected; want 1 and 7", created, rejected)
	}
}

func TestIntegrationS3MultipartContentHeaders(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("multiowner"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	path := "/" + bucket.Name + "/multi.txt"
	do := func(method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	headers := map[string]string{"Content-Disposition": `attachment; filename="multi.txt"`, "Cache-Control": "max-age=30"}
	started := do(http.MethodPost, path+"?uploads", "", headers)
	if started.Code != http.StatusOK {
		t.Fatalf("init multipart: %d %s", started.Code, started.Body.String())
	}
	var init InitiateMultipartUploadResult
	if err := xml.Unmarshal(started.Body.Bytes(), &init); err != nil || init.UploadId == "" {
		t.Fatalf("init multipart result: %+v %v", init, err)
	}
	uploadPath := path + "?partNumber=1&uploadId=" + init.UploadId
	part := do(http.MethodPut, uploadPath, "multipart bytes", nil)
	if part.Code != http.StatusOK || part.Header().Get("ETag") == "" {
		t.Fatalf("upload part: %d %s", part.Code, part.Body.String())
	}
	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + part.Header().Get("ETag") +
		`</ETag></Part></CompleteMultipartUpload>`
	completed := do(http.MethodPost, path+"?uploadId="+init.UploadId, completeBody, nil)
	if completed.Code != http.StatusOK {
		t.Fatalf("complete multipart: %d %s", completed.Code, completed.Body.String())
	}
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		w := do(method, path, "", nil)
		if w.Code != http.StatusOK || w.Header().Get("Content-Disposition") != headers["Content-Disposition"] ||
			w.Header().Get("Cache-Control") != headers["Cache-Control"] {
			t.Errorf("multipart %s headers: status=%d disposition=%q cache=%q", method,
				w.Code, w.Header().Get("Content-Disposition"), w.Header().Get("Cache-Control"))
		}
	}
}

func TestIntegrationS3CopySourceConditions(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("copycond"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	source := "/" + bucket.Name + "/source"
	dest := "/" + bucket.Name + "/dest"
	s3PutString(t, router, bucket.Name, "source", "source-bytes")
	s3PutString(t, router, bucket.Name, "dest", "untouched")
	sourceHead := itDo(router, http.MethodHead, source, nil)
	if sourceHead.Code != http.StatusOK || sourceHead.Header().Get("ETag") == "" {
		t.Fatalf("source HEAD: %d", sourceHead.Code)
	}
	etag := sourceHead.Header().Get("ETag")
	copyWith := func(extra map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, dest, nil)
		req.Header.Set("x-amz-copy-source", source)
		for name, value := range extra {
			req.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	for name, headers := range map[string]map[string]string{
		"wrong ETag":        {"x-amz-copy-source-if-match": `"different"`},
		"matching none":     {"x-amz-copy-source-if-none-match": etag},
		"older than cutoff": {"x-amz-copy-source-if-unmodified-since": past},
		"newer than cutoff": {"x-amz-copy-source-if-modified-since": future},
	} {
		w := copyWith(headers)
		if w.Code != http.StatusPreconditionFailed {
			t.Errorf("%s: status %d, want 412", name, w.Code)
		}
		if code, body := s3GetString(t, router, bucket.Name, "dest"); code != http.StatusOK || body != "untouched" {
			t.Errorf("%s changed destination: status=%d body=%q", name, code, body)
		}
	}
	for name, headers := range map[string]map[string]string{
		"matching ETag":        {"x-amz-copy-source-if-match": etag},
		"different none":       {"x-amz-copy-source-if-none-match": `"different"`},
		"unmodified at future": {"x-amz-copy-source-if-unmodified-since": future},
		"modified after past":  {"x-amz-copy-source-if-modified-since": past},
		"match wins over old unmodified": {"x-amz-copy-source-if-match": etag,
			"x-amz-copy-source-if-unmodified-since": past},
	} {
		w := copyWith(headers)
		if w.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200", name, w.Code)
		}
	}
}

func TestIntegrationDeleteBucketClearsReplicationTargets(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	target := itBucket(t, admin.ID, nil)

	// Some content in the target, on disk and in the index.
	ls := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := ls.PutObject(target.Name, "x/y", bytes.NewReader([]byte("data")), 4, "", nil); err != nil {
		t.Fatal(err)
	}
	obj := models.Object{BucketID: target.ID, Key: "x/y", Size: 4, ETag: "e"}
	if err := upsertCurrentObject(&obj); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	byName := itBucket(t, admin.ID, func(b *models.Bucket) {
		b.ReplicateTo, b.ReplicateToID = target.Name, &target.ID
		b.ReplicationConfiguredBy, b.ReplicationConfiguredAt = &admin.ID, &now
	})
	// Pinned by id although the name no longer matches (renamed config).
	byID := itBucket(t, admin.ID, func(b *models.Bucket) {
		b.ReplicateTo, b.ReplicateToID = "some-older-name", &target.ID
		b.ReplicationConfiguredBy, b.ReplicationConfiguredAt = &admin.ID, &now
	})
	otherTarget := uuid.New()
	unrelated := itBucket(t, admin.ID, func(b *models.Bucket) {
		b.ReplicateTo, b.ReplicateToID = "another-bucket", &otherTarget
		b.ReplicationConfiguredBy, b.ReplicationConfiguredAt = &admin.ID, &now
	})

	r := itRouter(admin)
	r.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	if w := itDo(r, http.MethodDelete, "/buckets/"+target.Name, nil); w.Code != http.StatusOK {
		t.Fatalf("DeleteBucket: %d %s", w.Code, w.Body.String())
	}

	var n int64
	database.DB.Model(&models.Bucket{}).Where("id = ?", target.ID).Count(&n)
	if n != 0 {
		t.Error("bucket row survived")
	}
	database.DB.Model(&models.Object{}).Where("bucket_id = ?", target.ID).Count(&n)
	if n != 0 {
		t.Error("object rows survived")
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.RootPath, ".objects", target.Name)); !os.IsNotExist(err) {
		t.Errorf("bucket directory survived: %v", err)
	}
	for _, src := range []models.Bucket{byName, byID} {
		var got models.Bucket
		database.DB.First(&got, "id = ?", src.ID)
		if got.ReplicateTo != "" || got.ReplicateToID != nil || got.ReplicationConfiguredBy != nil || got.ReplicationConfiguredAt != nil {
			t.Errorf("replication into the deleted bucket not cleared on %s: to=%q id=%v by=%v at=%v",
				src.Name, got.ReplicateTo, got.ReplicateToID, got.ReplicationConfiguredBy, got.ReplicationConfiguredAt)
		}
	}
	var got models.Bucket
	database.DB.First(&got, "id = ?", unrelated.ID)
	if got.ReplicateTo != "another-bucket" || got.ReplicateToID == nil || *got.ReplicateToID != otherTarget ||
		got.ReplicationConfiguredBy == nil || got.ReplicationConfiguredAt == nil {
		t.Errorf("unrelated replication config was modified: %+v", got)
	}
}

// deleteObjects posts a DeleteObjects request and decodes the result.
func deleteObjects(t *testing.T, r http.Handler, bucket string, objs ...DeleteObject) DeleteResult {
	t.Helper()
	body, _ := xml.Marshal(DeleteRequest{Objects: objs})
	w := itDo(r, http.MethodPost, "/"+bucket+"?delete", body)
	if w.Code != http.StatusOK {
		t.Fatalf("DeleteObjects: %d %s", w.Code, w.Body.String())
	}
	var res DeleteResult
	if err := xml.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	return res
}

func markerCount(t *testing.T, bucketID uuid.UUID, key string) int {
	t.Helper()
	n := 0
	for _, v := range versionRows(t, bucketID, key) {
		if v.IsDeleteMarker {
			n++
		}
	}
	return n
}

func TestIntegrationDeleteObjectsWithVersionId(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	r := itS3Router(cfg, admin)
	verDir := func(key string) string { return itVersionDir(cfg.Storage.RootPath, b.Name, key) }

	s3PutString(t, r, b.Name, "d/k", "one")
	v1Row, _ := currentRow(t, b.ID, "d/k")
	s3PutString(t, r, b.Name, "d/k", "two")
	cur, _ := currentRow(t, b.ID, "d/k")
	if vs := versionRows(t, b.ID, "d/k"); len(vs) != 1 || vs[0].VersionID != v1Row.VersionID {
		t.Fatalf("setup: versions %+v", vs)
	}

	// 1. Deleting an archived version removes it (row + bytes) and creates
	// no delete marker; the current version is untouched.
	res := deleteObjects(t, r, b.Name, DeleteObject{Key: "d/k", VersionId: v1Row.VersionID})
	if len(res.Errors) != 0 || len(res.Deleted) != 1 || res.Deleted[0].VersionId != v1Row.VersionID || res.Deleted[0].DeleteMarker {
		t.Fatalf("delete archived version: %+v", res)
	}
	if vs := versionRows(t, b.ID, "d/k"); len(vs) != 0 {
		t.Errorf("versions left: %+v", vs)
	}
	if _, err := os.Stat(verDir("d/k")); !os.IsNotExist(err) {
		t.Errorf("version storage for the key not pruned: %v", err)
	}
	if code, body := s3GetString(t, r, b.Name, "d/k"); code != http.StatusOK || body != "two" {
		t.Errorf("current after version delete: %d %q", code, body)
	}

	// 2. A plain delete creates a marker; deleting the marker by id is
	// reported as a marker deletion, creates no new marker and resurrects the
	// previous version.
	res = deleteObjects(t, r, b.Name, DeleteObject{Key: "d/k"})
	if len(res.Deleted) != 1 || !res.Deleted[0].DeleteMarker || res.Deleted[0].DeleteMarkerVersionId == "" {
		t.Fatalf("plain delete: %+v", res)
	}
	marker := res.Deleted[0].DeleteMarkerVersionId
	if _, ok := currentRow(t, b.ID, "d/k"); ok {
		t.Fatal("current row survived a versioned delete")
	}
	res = deleteObjects(t, r, b.Name, DeleteObject{Key: "d/k", VersionId: marker})
	if len(res.Errors) != 0 || len(res.Deleted) != 1 || !res.Deleted[0].DeleteMarker ||
		res.Deleted[0].DeleteMarkerVersionId != marker || res.Deleted[0].VersionId != marker {
		t.Fatalf("delete marker by id: %+v", res)
	}
	if n := markerCount(t, b.ID, "d/k"); n != 0 {
		t.Errorf("%d markers left", n)
	}
	if code, body := s3GetString(t, r, b.Name, "d/k"); code != http.StatusOK || body != "two" {
		t.Errorf("after removing the marker: %d %q, want the resurrected version", code, body)
	}
	if again, ok := currentRow(t, b.ID, "d/k"); !ok || again.VersionID != cur.VersionID {
		t.Errorf("resurrected row %+v, want version %s", again, cur.VersionID)
	}

	// 3. Deleting the current version by id removes the key entirely (no
	// versions left, no marker); an unknown version id is a no-op success.
	res = deleteObjects(t, r, b.Name,
		DeleteObject{Key: "d/k", VersionId: cur.VersionID},
		DeleteObject{Key: "d/k", VersionId: uuid.NewString()})
	if len(res.Errors) != 0 || len(res.Deleted) != 2 || res.Deleted[0].DeleteMarker {
		t.Fatalf("delete current by id: %+v", res)
	}
	if _, ok := currentRow(t, b.ID, "d/k"); ok {
		t.Error("current row survived")
	}
	if vs := versionRows(t, b.ID, "d/k"); len(vs) != 0 {
		t.Errorf("versions/markers left: %+v", vs)
	}
	if _, err := os.Stat(itBlobPath(cfg.Storage.RootPath, b.Name, "d/k")); !os.IsNotExist(err) {
		t.Errorf("object bytes survived: %v", err)
	}

	// 4. Retention: content versions (archived or current) are refused, the
	// refusal is per entry, and markers stay removable.
	database.DB.Model(&models.Bucket{}).Where("id = ?", b.ID).Update("retention_days", 30)
	s3PutString(t, r, b.Name, "w/k", "a")
	old, _ := currentRow(t, b.ID, "w/k")
	s3PutString(t, r, b.Name, "w/k", "b")
	curW, _ := currentRow(t, b.ID, "w/k")
	res = deleteObjects(t, r, b.Name,
		DeleteObject{Key: "w/k", VersionId: old.VersionID},
		DeleteObject{Key: "w/k", VersionId: curW.VersionID})
	if len(res.Deleted) != 0 || len(res.Errors) != 2 || res.Errors[0].Code != "AccessDenied" || res.Errors[1].Code != "AccessDenied" ||
		res.Errors[0].VersionId != old.VersionID {
		t.Fatalf("retention: %+v", res)
	}
	if vs := versionRows(t, b.ID, "w/k"); len(vs) != 1 {
		t.Errorf("retained version removed: %+v", vs)
	}
	if code, body := s3GetString(t, r, b.Name, "w/k"); code != http.StatusOK || body != "b" {
		t.Errorf("retained current: %d %q", code, body)
	}
	res = deleteObjects(t, r, b.Name, DeleteObject{Key: "w/k"})
	if len(res.Deleted) != 1 || !res.Deleted[0].DeleteMarker {
		t.Fatalf("plain delete under retention must create a marker: %+v", res)
	}
	res = deleteObjects(t, r, b.Name, DeleteObject{Key: "w/k", VersionId: res.Deleted[0].DeleteMarkerVersionId})
	if len(res.Errors) != 0 || len(res.Deleted) != 1 || !res.Deleted[0].DeleteMarker {
		t.Fatalf("marker removal under retention: %+v", res)
	}

	// 5. A caller without s3:DeleteObject gets per-entry AccessDenied.
	nobody := mkUser(t, itName("usr"), "password-123", false, false)
	res = deleteObjects(t, itS3Router(cfg, nobody), b.Name, DeleteObject{Key: "w/k", VersionId: old.VersionID})
	if len(res.Deleted) != 0 || len(res.Errors) != 1 || res.Errors[0].Code != "AccessDenied" {
		t.Fatalf("unauthorized: %+v", res)
	}
}

func TestIntegrationS3CreateBucketOnExisting(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	lister := mkUser(t, itName("lst"), "password-123", false, false)
	writer := mkUser(t, itName("wrt"), "password-123", false, false)
	stranger := mkUser(t, itName("str"), "password-123", false, false)
	b := itBucket(t, admin.ID, nil)

	attach := func(u models.User, doc string) {
		p := models.Policy{Name: itName("pol"), Document: doc}
		if err := database.DB.Create(&p).Error; err != nil {
			t.Fatal(err)
		}
		if err := database.DB.Model(&u).Association("Policies").Append(&p); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			database.DB.Exec(`DELETE FROM user_policies WHERE policy_id = ?`, p.ID)
			database.DB.Exec(`DELETE FROM policies WHERE id = ?`, p.ID)
		})
	}
	attach(lister, fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]}]}`, b.Name))
	attach(writer, fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, b.Name))

	cases := []struct {
		who    models.User
		bucket string
		status int
		code   string
	}{
		{admin, b.Name, http.StatusConflict, "BucketAlreadyOwnedByYou"},
		{lister, b.Name, http.StatusConflict, "BucketAlreadyOwnedByYou"},
		{writer, b.Name, http.StatusConflict, "BucketAlreadyOwnedByYou"},
		{stranger, b.Name, http.StatusConflict, "BucketAlreadyExists"},
		{admin, itName("new"), http.StatusForbidden, "AccessDenied"},
		{stranger, itName("new"), http.StatusForbidden, "AccessDenied"},
	}
	for _, c := range cases {
		w := itDo(itS3Router(cfg, c.who), http.MethodPut, "/"+c.bucket, nil)
		if w.Code != c.status || itS3ErrCode(t, w) != c.code {
			t.Errorf("%s PUT /%s: %d %s, want %d %s", c.who.Username, c.bucket, w.Code, w.Body.String(), c.status, c.code)
		}
	}
	var n int64
	database.DB.Model(&models.Bucket{}).Where("owner_id = ?", admin.ID).Count(&n)
	if n != 1 {
		t.Errorf("S3 CreateBucket created buckets: %d", n)
	}
}

// Deleting the current version promotes the next-newest version in the same
// total order the version listings use (versioned_at DESC, id DESC), also
// when several versions were archived in the same instant.
func TestIntegrationPromotionFollowsListingOrder(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	r := itS3Router(cfg, admin)
	same := time.Now().Add(-time.Hour).Truncate(time.Second)

	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("tie/%d", i)
		content := map[string]string{}
		for _, body := range []string{"one", "two", "three"} {
			s3PutString(t, r, b.Name, key, body)
			o, _ := currentRow(t, b.ID, key)
			content[o.VersionID] = body
		}
		database.DB.Model(&models.ObjectVersion{}).Where("bucket_id = ? AND key = ?", b.ID, key).Update("versioned_at", same)
		vs := versionRows(t, b.ID, key) // listing order
		if len(vs) != 2 {
			t.Fatalf("setup: %d versions", len(vs))
		}
		cur, _ := currentRow(t, b.ID, key)
		res := deleteObjects(t, r, b.Name, DeleteObject{Key: key, VersionId: cur.VersionID})
		if len(res.Errors) != 0 {
			t.Fatalf("delete current: %+v", res)
		}
		promoted, ok := currentRow(t, b.ID, key)
		if !ok || promoted.VersionID != vs[0].VersionID {
			t.Errorf("%s: promoted %q, listing order says %q is next", key, promoted.VersionID, vs[0].VersionID)
			continue
		}
		if code, body := s3GetString(t, r, b.Name, key); code != http.StatusOK || body != content[vs[0].VersionID] {
			t.Errorf("%s: promoted content %d %q, want %q", key, code, body, content[vs[0].VersionID])
		}
	}
}
