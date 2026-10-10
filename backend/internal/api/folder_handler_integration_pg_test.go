package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/models"

	"github.com/gin-gonic/gin"
)

// Postgres-gated tests for the console's query-keyed object routes, folder
// delete and deleted-objects listing. See integration_pg_test.go.

// itConsoleRouter mounts the console REST routes these tests use, with the
// same patterns as registerAPIRoutes.
func itConsoleRouter(cfg *config.Config, u models.User) *gin.Engine {
	if cfg.Storage.MaxFileSize == 0 {
		cfg.Storage.MaxFileSize = 1 << 20
	}
	h := NewBucketHandler(cfg)
	r := itRouter(u)
	b := r.Group("/api/buckets")
	b.POST("/:name/objects", h.UploadObject)
	b.GET("/:name/object-versions", h.ListObjectVersionsREST)
	b.DELETE("/:name/object-versions", h.DeleteObjectVersionREST)
	b.POST("/:name/folders/move", h.MoveFolder)
	b.GET("/:name/objects/*key", h.DownloadObject)
	b.DELETE("/:name/objects/*key", h.DeleteObject)
	b.HEAD("/:name/objects/*key", h.HeadObject)
	b.GET("/:name/object", withQueryObjectKey(h.DownloadObject))
	b.DELETE("/:name/object", withQueryObjectKey(h.DeleteObject))
	b.HEAD("/:name/object", withQueryObjectKey(h.HeadObject))
	b.GET("/:name/folders", h.GetFolderSummary)
	b.DELETE("/:name/folders", h.DeleteFolder)
	b.GET("/:name/deleted-objects", h.ListDeletedObjects)
	return r
}

// consoleUpload uploads body as key through the console's multipart upload.
func consoleUpload(t *testing.T, r http.Handler, bucket, key, body string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("key", key)
	fw, err := mw.CreateFormFile("file", "upload.bin")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte(body))
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/buckets/"+bucket+"/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("upload %q: %d %s", key, w.Code, w.Body.String())
	}
}

func objectURL(bucket, key string) string {
	return "/api/buckets/" + bucket + "/object?" + url.Values{"key": {key}}.Encode()
}

func folderURL(bucket, prefix string) string {
	return "/api/buckets/" + bucket + "/folders?" + url.Values{"prefix": {prefix}}.Encode()
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, v interface{}) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
		t.Fatalf("bad JSON (%d): %s", w.Code, w.Body.String())
	}
}

// Keys that a URL path cannot carry byte-for-byte are downloadable and
// deletable through the ?key= routes.
func TestIntegrationQueryKeyObjectRoutes(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, nil)
	r := itConsoleRouter(cfg, admin)

	keys := []string{"../x", "./y", "a/../b", ".%2e/hello.txt", "%2F", "back\\slash", "/leading",
		"a.txt/", "sp ace+plus?q#h&amp", "..", "."}
	for _, k := range keys {
		consoleUpload(t, r, b.Name, k, "data:"+k)
	}
	for _, k := range keys {
		if w := itDo(r, http.MethodHead, objectURL(b.Name, k), nil); w.Code != http.StatusOK {
			t.Errorf("HEAD %q: %d", k, w.Code)
		}
		w := itDo(r, http.MethodGet, objectURL(b.Name, k), nil)
		if w.Code != http.StatusOK || w.Body.String() != "data:"+k {
			t.Errorf("GET %q: %d %q", k, w.Code, w.Body.String())
		}
	}
	for _, k := range keys {
		if w := itDo(r, http.MethodDelete, objectURL(b.Name, k), nil); w.Code != http.StatusOK {
			t.Errorf("DELETE %q: %d %s", k, w.Code, w.Body.String())
		}
		if _, ok := currentRow(t, b.ID, k); ok {
			t.Errorf("DELETE %q left its row", k)
		}
		if w := itDo(r, http.MethodGet, objectURL(b.Name, k), nil); w.Code != http.StatusNotFound {
			t.Errorf("GET %q after delete: %d", k, w.Code)
		}
	}
	if w := itDo(r, http.MethodGet, "/api/buckets/"+b.Name+"/object", nil); w.Code != http.StatusBadRequest {
		t.Errorf("GET without key: %d", w.Code)
	}
	// The path form keeps working for ordinary keys.
	consoleUpload(t, r, b.Name, "dir/plain.txt", "p")
	if w := itDo(r, http.MethodGet, "/api/buckets/"+b.Name+"/objects/dir/plain.txt", nil); w.Code != http.StatusOK || w.Body.String() != "p" {
		t.Errorf("path-form GET: %d %q", w.Code, w.Body.String())
	}
}

type folderDeleteResult struct {
	Deleted          int `json:"deleted"`
	SkippedRetention int `json:"skipped_retention"`
	Denied           int `json:"denied"`
}

func currentKeys(t *testing.T, bucketID interface{}) []string {
	t.Helper()
	var keys []string
	if err := database.DB.Model(&models.Object{}).Where("bucket_id = ?", bucketID).Order("key").Pluck("key", &keys).Error; err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestIntegrationDeleteFolder(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, nil)
	r := itConsoleRouter(cfg, admin)

	for _, k := range []string{"d/", "d/.keep", "d/a", "d/sub/b", "d/sub/", "dx", "d.txt",
		"a_b/c", "aXb/c", "p%/q", "pX/q"} {
		consoleUpload(t, r, b.Name, k, "v")
	}

	var sum struct {
		ObjectCount int64 `json:"object_count"`
	}
	w := itDo(r, http.MethodGet, folderURL(b.Name, "d/"), nil)
	decodeJSON(t, w, &sum)
	if w.Code != http.StatusOK || sum.ObjectCount != 5 {
		t.Fatalf("summary d/: %d %s", w.Code, w.Body.String())
	}

	var res folderDeleteResult
	w = itDo(r, http.MethodDelete, folderURL(b.Name, "d/"), nil)
	decodeJSON(t, w, &res)
	if w.Code != http.StatusOK || res.Deleted != 5 || res.Denied != 0 || res.SkippedRetention != 0 {
		t.Fatalf("delete d/: %d %s", w.Code, w.Body.String())
	}
	// LIKE wildcards in the prefix match literally.
	for _, p := range []string{"a_b/", "p%/"} {
		w = itDo(r, http.MethodDelete, folderURL(b.Name, p), nil)
		decodeJSON(t, w, &res)
		if w.Code != http.StatusOK || res.Deleted != 1 {
			t.Fatalf("delete %s: %d %s", p, w.Code, w.Body.String())
		}
	}
	got := currentKeys(t, b.ID)
	want := []string{"aXb/c", "d.txt", "dx", "pX/q"}
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("remaining keys %v, want %v", got, want)
	}

	// Deleting an already-empty folder is a no-op, not an error.
	w = itDo(r, http.MethodDelete, folderURL(b.Name, "d/"), nil)
	decodeJSON(t, w, &res)
	if w.Code != http.StatusOK || res.Deleted != 0 {
		t.Errorf("repeat delete: %d %s", w.Code, w.Body.String())
	}
	for _, p := range []string{"", "d", "nope"} {
		if w := itDo(r, http.MethodDelete, folderURL(b.Name, p), nil); w.Code != http.StatusBadRequest {
			t.Errorf("prefix %q: %d, want 400", p, w.Code)
		}
	}
}

func TestIntegrationDeleteFolderAuthzAndRetention(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	reader := mkUser(t, itName("rdr"), "password-123", false, false)
	partial := mkUser(t, itName("prt"), "password-123", false, false)
	b := itBucket(t, admin.ID, nil)
	ra := itConsoleRouter(cfg, admin)
	for _, k := range []string{"f/", "f/keep/one", "f/tmp/two", "f/tmp/three"} {
		consoleUpload(t, ra, b.Name, k, "v")
	}

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
	attach(reader, fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket","s3:GetObject"],"Resource":["arn:aws:s3:::%[1]s","arn:aws:s3:::%[1]s/*"]}]}`, b.Name))
	attach(partial, fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::%[1]s"]},{"Effect":"Allow","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::%[1]s/f/tmp/*"]}]}`, b.Name))

	// Read-only: a clear 403 and nothing deleted.
	w := itDo(itConsoleRouter(cfg, reader), http.MethodDelete, folderURL(b.Name, "f/"), nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("reader delete: %d %s", w.Code, w.Body.String())
	}
	if n := len(currentKeys(t, b.ID)); n != 4 {
		t.Fatalf("reader delete removed objects: %d left", n)
	}

	// Partial permission: allowed keys go, the rest are reported as denied.
	var res folderDeleteResult
	w = itDo(itConsoleRouter(cfg, partial), http.MethodDelete, folderURL(b.Name, "f/"), nil)
	decodeJSON(t, w, &res)
	if w.Code != http.StatusOK || res.Deleted != 2 || res.Denied != 2 {
		t.Fatalf("partial delete: %d %s", w.Code, w.Body.String())
	}
	if got := fmt.Sprint(currentKeys(t, b.ID)); got != "[f/ f/keep/one]" {
		t.Errorf("after partial delete: %s", got)
	}

	// Unversioned delete under WORM retention is skipped, not performed.
	database.DB.Model(&models.Bucket{}).Where("id = ?", b.ID).Update("retention_days", 30)
	w = itDo(ra, http.MethodDelete, folderURL(b.Name, "f/"), nil)
	decodeJSON(t, w, &res)
	if w.Code != http.StatusOK || res.Deleted != 0 || res.SkippedRetention != 2 {
		t.Fatalf("retention delete: %d %s", w.Code, w.Body.String())
	}
}

type deletedListing struct {
	Objects []struct {
		Key                   string `json:"key"`
		DeleteMarkerVersionID string `json:"delete_marker_version_id"`
		Recoverable           bool   `json:"recoverable"`
		Size                  int64  `json:"size"`
	} `json:"objects"`
	IsTruncated bool   `json:"is_truncated"`
	NextToken   string `json:"next_continuation_token"`
}

func listDeleted(t *testing.T, r http.Handler, bucket, prefix, token string, maxKeys int) deletedListing {
	t.Helper()
	q := url.Values{"prefix": {prefix}, "max_keys": {fmt.Sprint(maxKeys)}}
	if token != "" {
		q.Set("continuation_token", token)
	}
	w := itDo(r, http.MethodGet, "/api/buckets/"+bucket+"/deleted-objects?"+q.Encode(), nil)
	if w.Code != http.StatusOK {
		t.Fatalf("deleted-objects: %d %s", w.Code, w.Body.String())
	}
	var l deletedListing
	decodeJSON(t, w, &l)
	return l
}

func TestIntegrationDeletedObjectsAndRestore(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	r := itConsoleRouter(cfg, admin)

	for _, k := range []string{"v/", "v/a", "v/b", "v/sub/c", "v/live", "w/other"} {
		consoleUpload(t, r, b.Name, k, "one:"+k)
	}
	consoleUpload(t, r, b.Name, "v/a", "two:v/a") // v/a has two content versions

	var res folderDeleteResult
	w := itDo(r, http.MethodDelete, objectURL(b.Name, "v/live"), nil)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	consoleUpload(t, r, b.Name, "v/live", "back") // deleted, then re-created: not "deleted"
	w = itDo(r, http.MethodDelete, folderURL(b.Name, "v/sub/"), nil)
	decodeJSON(t, w, &res)
	if res.Deleted != 1 {
		t.Fatalf("delete v/sub/: %s", w.Body.String())
	}
	for _, k := range []string{"v/", "v/a", "v/b", "w/other"} {
		if w := itDo(r, http.MethodDelete, objectURL(b.Name, k), nil); w.Code != http.StatusOK {
			t.Fatalf("delete %s: %s", k, w.Body.String())
		}
	}

	all := listDeleted(t, r, b.Name, "v/", "", 1000)
	var keys []string
	for _, o := range all.Objects {
		keys = append(keys, o.Key)
		if !o.Recoverable || o.DeleteMarkerVersionID == "" {
			t.Errorf("%s: %+v", o.Key, o)
		}
	}
	if fmt.Sprint(keys) != "[v/ v/a v/b v/sub/c]" || all.IsTruncated {
		var versions []models.ObjectVersion
		_ = database.DB.Where("bucket_id = ? AND key LIKE ?", b.ID, "v/%").Order("key, versioned_at DESC, id DESC").Find(&versions).Error
		for _, version := range versions {
			t.Logf("version key=%q marker=%v at=%s id=%s", version.Key, version.IsDeleteMarker, version.VersionedAt.Format("2006-01-02T15:04:05.000000000Z07:00"), version.ID)
		}
		t.Fatalf("deleted under v/: %v (truncated=%v)", keys, all.IsTruncated)
	}
	if all.Objects[1].Size != int64(len("two:v/a")) {
		t.Errorf("v/a size %d, want newest content version's", all.Objects[1].Size)
	}

	// Keyset pagination.
	var paged []string
	token := ""
	for i := 0; i < 10; i++ {
		l := listDeleted(t, r, b.Name, "v/", token, 3)
		for _, o := range l.Objects {
			paged = append(paged, o.Key)
		}
		if !l.IsTruncated {
			break
		}
		token = l.NextToken
	}
	if fmt.Sprint(paged) != fmt.Sprint(keys) {
		t.Errorf("paged %v, want %v", paged, keys)
	}

	// Restore = remove the delete marker: the newest version is current again.
	marker := all.Objects[1].DeleteMarkerVersionID
	w = itDo(r, http.MethodDelete, "/api/buckets/"+b.Name+"/object-versions?"+url.Values{"key": {"v/a"}, "version_id": {marker}}.Encode(), nil)
	if w.Code != http.StatusOK {
		t.Fatalf("restore: %d %s", w.Code, w.Body.String())
	}
	if w := itDo(r, http.MethodGet, objectURL(b.Name, "v/a"), nil); w.Code != http.StatusOK || w.Body.String() != "two:v/a" {
		t.Errorf("restored v/a: %d %q", w.Code, w.Body.String())
	}
	l := listDeleted(t, r, b.Name, "", "", 1000)
	keys = keys[:0]
	for _, o := range l.Objects {
		keys = append(keys, o.Key)
	}
	if fmt.Sprint(keys) != "[v/ v/b v/sub/c w/other]" {
		t.Errorf("deleted after restore: %v", keys)
	}

	// Listing needs ListBucket.
	stranger := mkUser(t, itName("str"), "password-123", false, false)
	w = itDo(itConsoleRouter(cfg, stranger), http.MethodGet, "/api/buckets/"+b.Name+"/deleted-objects", nil)
	if w.Code != http.StatusForbidden {
		t.Errorf("stranger listing: %d", w.Code)
	}
}
