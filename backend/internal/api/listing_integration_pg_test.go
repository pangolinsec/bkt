package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"

	"github.com/google/uuid"
)

// Postgres-gated integration tests for the listing paths: S3
// ListObjectVersions pagination and the S3-backed ListObjects reconcile.

type expectedVersion struct {
	key, vid       string
	latest, marker bool
}

// seedVersionedKeys writes a mix of key histories straight into the index
// (listing never touches bytes) and returns the exact S3 listing order for
// keys under prefix: keys ascending, per key the current version first, then
// archived versions newest first (versioned_at DESC, id DESC).
func seedVersionedKeys(t *testing.T, bucketID uuid.UUID, prefix string, nKeys int) []expectedVersion {
	t.Helper()
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	var want []expectedVersion
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("%sk%03d", prefix, i)
		var cur *models.Object
		var vers []models.ObjectVersion
		addVer := func(marker bool, at time.Time) {
			vers = append(vers, models.ObjectVersion{ID: uuid.New(), BucketID: bucketID, Key: key,
				VersionID: uuid.NewString(), IsDeleteMarker: marker, Size: 1, ETag: "e",
				VersionedAt: at, ContentModifiedAt: at})
		}
		at := func(n int) time.Time { return base.Add(time.Duration(i*100+n) * time.Second) }
		switch i % 5 {
		case 0: // written while unversioned: current "null" version only
			cur = &models.Object{BucketID: bucketID, Key: key, Size: 1, ETag: "e"}
		case 1: // current + 3 archived versions
			cur = &models.Object{BucketID: bucketID, Key: key, Size: 1, ETag: "e", VersionID: uuid.NewString()}
			addVer(false, at(1))
			addVer(false, at(2))
			addVer(false, at(3))
		case 2: // deleted: marker newest, 2 content versions
			addVer(false, at(1))
			addVer(false, at(2))
			addVer(true, at(3))
		case 3: // current + 4 archived versions archived in the same instant
			cur = &models.Object{BucketID: bucketID, Key: key, Size: 1, ETag: "e", VersionID: uuid.NewString()}
			for n := 0; n < 4; n++ {
				addVer(false, at(1))
			}
		case 4: // no current row, no marker: newest content version is latest
			addVer(false, at(1))
			addVer(false, at(2))
		}
		if cur != nil {
			if err := database.DB.Create(cur).Error; err != nil {
				t.Fatal(err)
			}
			vid := cur.VersionID
			if vid == "" {
				vid = "null"
			}
			want = append(want, expectedVersion{key: key, vid: vid, latest: true})
		}
		if len(vers) > 0 {
			if err := database.DB.Create(&vers).Error; err != nil {
				t.Fatal(err)
			}
		}
		sort.Slice(vers, func(a, b int) bool {
			if !vers[a].VersionedAt.Equal(vers[b].VersionedAt) {
				return vers[a].VersionedAt.After(vers[b].VersionedAt)
			}
			return vers[a].ID.String() > vers[b].ID.String()
		})
		for n, v := range vers {
			want = append(want, expectedVersion{key: key, vid: v.VersionID, marker: v.IsDeleteMarker, latest: cur == nil && n == 0})
		}
	}
	return want
}

func TestIntegrationListObjectVersionsPagination(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	want := seedVersionedKeys(t, b.ID, "p/", 23)
	other := seedVersionedKeys(t, b.ID, "q/", 4) // outside the prefix
	r := itS3Router(cfg, admin)

	list := func(prefix string, maxKeys int, want []expectedVersion) {
		t.Helper()
		pos := 0
		keyMarker, vidMarker := "", ""
		for page := 0; ; page++ {
			if page > len(want)+2 {
				t.Fatalf("max-keys=%d: pagination does not terminate", maxKeys)
			}
			q := url.Values{"versions": {""}, "max-keys": {fmt.Sprint(maxKeys)}}
			if prefix != "" {
				q.Set("prefix", prefix)
			}
			if keyMarker != "" {
				q.Set("key-marker", keyMarker)
			}
			if vidMarker != "" {
				q.Set("version-id-marker", vidMarker)
			}
			w := itDo(r, http.MethodGet, "/"+b.Name+"?"+q.Encode(), nil)
			if w.Code != http.StatusOK {
				t.Fatalf("max-keys=%d page %d: %d %s", maxKeys, page, w.Code, w.Body.String())
			}
			var res listVersionsResult
			if err := xml.Unmarshal(w.Body.Bytes(), &res); err != nil {
				t.Fatal(err)
			}
			// The page must be exactly the next slice of the expected order
			// (the XML groups Versions and DeleteMarkers, so compare as sets).
			got := map[string]expectedVersion{}
			for _, v := range res.Versions {
				got[v.Key+"\x00"+v.VersionId] = expectedVersion{key: v.Key, vid: v.VersionId, latest: v.IsLatest}
			}
			for _, m := range res.DeleteMarkers {
				got[m.Key+"\x00"+m.VersionId] = expectedVersion{key: m.Key, vid: m.VersionId, latest: m.IsLatest, marker: true}
			}
			n := len(res.Versions) + len(res.DeleteMarkers)
			if len(got) != n {
				t.Fatalf("max-keys=%d page %d: duplicate key/version ids in page", maxKeys, page)
			}
			if pos+n > len(want) {
				t.Fatalf("max-keys=%d page %d: %d entries beyond the %d expected", maxKeys, page, pos+n-len(want), len(want))
			}
			for _, e := range want[pos : pos+n] {
				if g, ok := got[e.key+"\x00"+e.vid]; !ok || g != e {
					t.Fatalf("max-keys=%d page %d (entries %d..%d): want %+v, got %+v (present=%v)", maxKeys, page, pos, pos+n, e, g, ok)
				}
			}
			pos += n
			if !res.IsTruncated {
				break
			}
			if n == 0 {
				t.Fatalf("max-keys=%d page %d: truncated empty page", maxKeys, page)
			}
			if res.NextKeyMarker != want[pos-1].key || res.NextVersionIdMarker != want[pos-1].vid {
				t.Fatalf("max-keys=%d page %d: next marker %s/%s, want %s/%s", maxKeys, page,
					res.NextKeyMarker, res.NextVersionIdMarker, want[pos-1].key, want[pos-1].vid)
			}
			keyMarker, vidMarker = res.NextKeyMarker, res.NextVersionIdMarker
		}
		if pos != len(want) {
			t.Fatalf("max-keys=%d: listed %d of %d versions", maxKeys, pos, len(want))
		}
	}
	for _, mk := range []int{1, 2, 3, 4, 7, 1000} {
		list("p/", mk, want)
	}
	list("", 5, append(append([]expectedVersion{}, want...), other...))

	// A key-marker alone resumes after that key's last version.
	w := itDo(r, http.MethodGet, "/"+b.Name+"?versions&prefix=p/&key-marker="+url.QueryEscape(want[0].key), nil)
	var res listVersionsResult
	if err := xml.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	for _, v := range res.Versions {
		if v.Key <= want[0].key {
			t.Errorf("key-marker %s: listed %s", want[0].key, v.Key)
		}
	}
}

// fakeS3Listing is a minimal S3 endpoint: HEAD bucket and ListObjectsV2.
type fakeS3Listing struct {
	mu   sync.Mutex
	keys []string
}

func (f *fakeS3Listing) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		f.mu.Lock()
		keys := append([]string(nil), f.keys...)
		f.mu.Unlock()
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		n := 0
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			n++
			fmt.Fprintf(&sb, `<Contents><Key>%s</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>`, k)
		}
		fmt.Fprintf(&sb, `<KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`, n)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(sb.String()))
	case r.Method == http.MethodDelete && strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 1:
		// A post-commit legacy cleanup must be able to remove its public key.
		key := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)[1]
		f.mu.Lock()
		for i, candidate := range f.keys {
			if candidate == key {
				f.keys = append(f.keys[:i], f.keys[i+1:]...)
				break
			}
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func seedRows(t *testing.T, bucketID uuid.UUID, updated time.Time, keys ...string) {
	t.Helper()
	for _, k := range keys {
		o := models.Object{BucketID: bucketID, Key: k, Size: 1, ETag: "e", StoragePath: k, CreatedAt: updated, UpdatedAt: updated}
		if err := database.DB.Create(&o).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func rowKeys(t *testing.T, bucketID uuid.UUID) []string {
	t.Helper()
	var keys []string
	database.DB.Model(&models.Object{}).Where("bucket_id = ?", bucketID).Order("key").Pluck("key", &keys)
	return keys
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func listObjectsKeys(t *testing.T, r http.Handler, bucket string) []string {
	t.Helper()
	w := itDo(r, http.MethodGet, "/buckets/"+bucket+"/objects", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects: %d %s", w.Code, w.Body.String())
	}
	var res struct {
		Objects []models.Object `json:"objects"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(res.Objects))
	for _, o := range res.Objects {
		keys = append(keys, o.Key)
	}
	return keys
}

func TestIntegrationListObjectsReconcilePruneSafety(t *testing.T) {
	cfg := itConfig(t)
	fake := &fakeS3Listing{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(srv.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true

	admin := mkUser(t, itName("adm"), "password-123", true, false)
	r := itRouter(admin)
	r.GET("/buckets/:name/objects", NewBucketHandler(cfg).ListObjects)
	old := time.Now().Add(-time.Hour)

	t.Run("prunes stale rows, keeps fresh ones, imports new keys", func(t *testing.T) {
		b := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
		seedRows(t, b.ID, old, "k0", "k1", "k2", "gone1", "gone2")
		seedRows(t, b.ID, time.Now(), "fresh") // written moments ago: listing may predate it
		fake.mu.Lock()
		fake.keys = []string{"k0", "k1", "k2", "imported"}
		fake.mu.Unlock()

		got := listObjectsKeys(t, r, b.Name)
		if want := []string{"fresh", "imported", "k0", "k1", "k2"}; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("listing = %v, want %v", got, want)
		}
		waitFor(t, "stale rows pruned and new key imported", func() bool {
			return strings.Join(rowKeys(t, b.ID), ",") == "fresh,imported,k0,k1,k2"
		})
	})

	t.Run("most rows missing: neither prune nor import", func(t *testing.T) {
		b := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
		var keys []string
		for i := 0; i < 20; i++ {
			keys = append(keys, fmt.Sprintf("r%02d", i))
		}
		seedRows(t, b.ID, old, keys...)
		fake.mu.Lock()
		fake.keys = append(append([]string{}, keys[:5]...), "foreign")
		fake.mu.Unlock()

		got := listObjectsKeys(t, r, b.Name)
		if strings.Join(got, ",") != strings.Join(keys, ",") {
			t.Errorf("listing = %v, want all %d rows and nothing imported", got, len(keys))
		}
		time.Sleep(300 * time.Millisecond) // any (wrongly) started background work
		if rows := rowKeys(t, b.ID); strings.Join(rows, ",") != strings.Join(keys, ",") {
			t.Errorf("rows after a suspicious listing = %v", rows)
		}
	})

	t.Run("hidden generation stays listed without a public physical key", func(t *testing.T) {
		b := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
		seedRows(t, b.ID, old, "generated")
		if err := database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", b.ID, "generated").
			UpdateColumns(map[string]interface{}{"generation_id": uuid.NewString(), "updated_at": old}).Error; err != nil {
			t.Fatal(err)
		}
		fake.mu.Lock()
		fake.keys = nil
		fake.mu.Unlock()
		if got := listObjectsKeys(t, r, b.Name); strings.Join(got, ",") != "generated" {
			t.Fatalf("hidden generation disappeared from console listing: %v", got)
		}
		time.Sleep(100 * time.Millisecond)
		if got := rowKeys(t, b.ID); strings.Join(got, ",") != "generated" {
			t.Errorf("reconciliation pruned committed generation row: %v", got)
		}
	})
}

func TestIntegrationDeletedLegacyS3ObjectStaysDeletedAfterConsoleList(t *testing.T) {
	for _, route := range []string{"console", "s3-single", "s3-batch"} {
		t.Run(route, func(t *testing.T) {
			cfg := itConfig(t)
			fake := &fakeS3Listing{keys: []string{"legacy"}}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			cfg.Storage.S3.Endpoint = strings.TrimPrefix(srv.URL, "http://")
			cfg.Storage.S3.Region = "us-east-1"
			cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
			cfg.Storage.S3.ForcePathStyle = true

			admin := mkUser(t, itName("legacy-delete"), "password-123", true, false)
			bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
			seedRows(t, bucket.ID, time.Now().Add(-time.Hour), "legacy")
			consoleHandler := NewBucketHandler(cfg)
			console := itRouter(admin)
			console.DELETE("/buckets/:name/objects/*key", consoleHandler.DeleteObject)
			console.GET("/buckets/:name/objects", consoleHandler.ListObjects)
			s3 := itS3Router(cfg, admin)
			s3.DELETE("/:bucket/*key", NewS3APIHandler(cfg).DeleteObject)

			switch route {
			case "console":
				deleted := itDo(console, http.MethodDelete, "/buckets/"+bucket.Name+"/objects/legacy", nil)
				if deleted.Code != http.StatusOK {
					t.Fatalf("console delete legacy object: %d %s", deleted.Code, deleted.Body.String())
				}
			case "s3-single":
				deleted := itDo(s3, http.MethodDelete, "/"+bucket.Name+"/legacy", nil)
				if deleted.Code != http.StatusNoContent {
					t.Fatalf("S3 DeleteObject legacy object: %d %s", deleted.Code, deleted.Body.String())
				}
			case "s3-batch":
				deleted := deleteObjects(t, s3, bucket.Name, DeleteObject{Key: "legacy"})
				if len(deleted.Errors) != 0 || len(deleted.Deleted) != 1 || deleted.Deleted[0].Key != "legacy" {
					t.Fatalf("S3 DeleteObjects legacy object: %+v", deleted)
				}
			}
			if got := rowKeys(t, bucket.ID); len(got) != 0 {
				t.Fatalf("deleted object retained current metadata: %v", got)
			}
			listed := listObjectsKeys(t, console, bucket.Name)
			// A listed key is imported asynchronously. Wait for that import to
			// persist before asserting, without a timing-dependent fixed sleep.
			if len(listed) != 0 {
				waitFor(t, "deleted legacy key imported by reconciliation", func() bool {
					return len(rowKeys(t, bucket.ID)) != 0
				})
			}
			if rows := rowKeys(t, bucket.ID); len(listed) != 0 || len(rows) != 0 {
				t.Errorf("deleted legacy object was resurrected by console listing: listed=%v rows=%v", listed, rows)
			}
		})
	}
}

func TestIntegrationDeleteBucketDoesNotCommitAfterS3StorageRefusesDeletion(t *testing.T) {
	cfg := itConfig(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 1:
			w.WriteHeader(http.StatusNoContent) // public key, not hidden generation
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusConflict) // hidden generation keeps bucket nonempty
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true

	admin := mkUser(t, itName("bucket-delete"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	seedRows(t, bucket.ID, time.Now().Add(-time.Hour), "generated")
	if err := database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", bucket.ID, "generated").
		Update("generation_id", uuid.NewString()).Error; err != nil {
		t.Fatal(err)
	}
	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	deleted := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil)
	if deleted.Code < http.StatusBadRequest {
		t.Errorf("bucket deletion reported success despite upstream refusal: %d %s", deleted.Code, deleted.Body.String())
	}
	var remainingBucket, remainingObject int64
	database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&remainingBucket)
	database.DB.Model(&models.Object{}).Where("bucket_id = ?", bucket.ID).Count(&remainingObject)
	if remainingBucket != 1 || remainingObject != 1 {
		t.Errorf("failed bucket deletion lost metadata: buckets=%d objects=%d", remainingBucket, remainingObject)
	}
}
