package api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
)

func pinTestS3Route(t *testing.T, cfg *config.Config, bucket *models.Bucket) {
	t.Helper()
	bucket.S3RouteIdentity = storage.S3RouteIdentity(cfg.Storage.S3.Endpoint, cfg.Storage.S3.Region,
		cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey, cfg.Storage.S3.BucketPrefix,
		cfg.Storage.S3.UseSSL, cfg.Storage.S3.ForcePathStyle)
	if err := database.DB.Model(bucket).Update("s3_route_identity", bucket.S3RouteIdentity).Error; err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationLocalBucketCleanupSurvivesFilesystemFailure(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("local-cleanup"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, nil)
	backend := storage.NewLocalStorage(cfg.Storage.RootPath)
	if err := backend.PutObject(bucket.Name, "orphan", bytes.NewReader([]byte("private-bytes")), 13, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	originalRemove := removeLocalBucket
	removeLocalBucket = func(rootPath, name string) error { return errors.New("injected filesystem failure") }
	t.Cleanup(func() {
		removeLocalBucket = originalRemove
		database.DB.Where("name = ?", bucket.Name).Delete(&models.LocalBucketCleanup{})
	})
	router := itRouter(admin)
	h := NewBucketHandler(cfg)
	router.DELETE("/buckets/:name", h.DeleteBucket)
	router.POST("/buckets", h.CreateBucket)
	if got := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil); got.Code != http.StatusInternalServerError {
		t.Fatalf("filesystem failure: %d %s", got.Code, got.Body.String())
	}
	var bucketRows, cleanupRows int64
	database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&bucketRows)
	database.DB.Model(&models.LocalBucketCleanup{}).Where("name = ?", bucket.Name).Count(&cleanupRows)
	if bucketRows != 0 || cleanupRows != 1 {
		t.Fatalf("durable cleanup state: bucket=%d cleanup=%d", bucketRows, cleanupRows)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.RootPath, ".objects", bucket.Name)); err != nil {
		t.Fatalf("bytes not preserved: %v", err)
	}
	create := []byte(`{"name":"` + bucket.Name + `","storage_backend":"local"}`)
	if got := itDo(router, http.MethodPost, "/buckets", create); got.Code != http.StatusConflict {
		t.Fatalf("create linked orphaned bytes: %d %s", got.Code, got.Body.String())
	}
	removeLocalBucket = originalRemove
	marker := filepath.Join(cfg.Storage.RootPath, localRootIdentityFile)
	originalID, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte(uuid.NewString()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileLocalBucketCleanups(cfg.Storage.RootPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.RootPath, ".objects", bucket.Name)); err != nil {
		t.Fatalf("changed volume identity removed bytes: %v", err)
	}
	if err := os.WriteFile(marker, originalID, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileLocalBucketCleanups(cfg.Storage.RootPath); err != nil {
		t.Fatal(err)
	}
	database.DB.Model(&models.LocalBucketCleanup{}).Where("name = ?", bucket.Name).Count(&cleanupRows)
	if cleanupRows != 0 {
		t.Fatalf("cleanup record not cleared: %d", cleanupRows)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.RootPath, ".objects", bucket.Name)); !os.IsNotExist(err) {
		t.Fatalf("orphaned bytes survived retry: %v", err)
	}
}

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
		parts := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
		if len(parts) == 2 {
			f.mu.Lock()
			found := false
			for _, key := range f.keys {
				if key == parts[1] {
					found = true
					break
				}
			}
			f.mu.Unlock()
			if !found {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
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
			importDone := make(chan struct{})
			consoleHandler.reconcileImportDone = func() { close(importDone) }
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
			// The observer fires only after this request's import job has
			// finished, or synchronously when no job was scheduled. The DB
			// assertion therefore cannot pass ahead of a hidden async import.
			select {
			case <-importDone:
			case <-time.After(5 * time.Second):
				t.Fatal("listing import did not finish")
			}
			if rows := rowKeys(t, bucket.ID); len(listed) != 0 || len(rows) != 0 {
				t.Errorf("deleted legacy object was resurrected by console listing: listed=%v rows=%v", listed, rows)
			}
		})
	}
}

func TestIntegrationLegacyDeleteDoesNotRaceListingImport(t *testing.T) {
	cfg := itConfig(t)
	fake := &fakeS3Listing{keys: []string{"legacy"}}
	deleteEntered := make(chan struct{})
	releaseDelete := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 1 {
			close(deleteEntered)
			<-releaseDelete
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	defer func() {
		select {
		case <-releaseDelete:
		default:
			close(releaseDelete)
		}
	}()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(srv.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true

	admin := mkUser(t, itName("legacy-race"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	seedRows(t, bucket.ID, time.Now().Add(-time.Hour), "legacy")
	consoleHandler := NewBucketHandler(cfg)
	importDone := make(chan struct{})
	consoleHandler.reconcileImportDone = func() { close(importDone) }
	console := itRouter(admin)
	console.GET("/buckets/:name/objects", consoleHandler.ListObjects)
	s3 := itS3Router(cfg, admin)
	s3.DELETE("/:bucket/*key", NewS3APIHandler(cfg).DeleteObject)

	deleted := make(chan int, 1)
	go func() {
		deleted <- itDo(s3, http.MethodDelete, "/"+bucket.Name+"/legacy", nil).Code
	}()
	select {
	case <-deleteEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy cleanup did not reach S3")
	}
	if got := rowKeys(t, bucket.ID); len(got) != 0 {
		t.Fatalf("metadata should be gone before the physical delete: %v", got)
	}
	// This listing observes the still-present public key while deletion holds
	// its key lock. Its background import must recheck existence after the
	// delete completes rather than create a stale row from the old listing.
	if listed := listObjectsKeys(t, console, bucket.Name); len(listed) != 1 || listed[0] != "legacy" {
		t.Fatalf("listing did not capture the in-flight legacy key: %v", listed)
	}
	close(releaseDelete)
	select {
	case code := <-deleted:
		if code != http.StatusNoContent {
			t.Errorf("S3 DeleteObject: %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy delete did not finish")
	}
	select {
	case <-importDone:
	case <-time.After(5 * time.Second):
		t.Fatal("listing import did not finish")
	}
	if rows := rowKeys(t, bucket.ID); len(rows) != 0 {
		t.Errorf("concurrent listing resurrected deleted legacy key: %v", rows)
	}
}

func TestIntegrationDeleteBucketDoesNotCommitAfterS3StorageRefusesDeletion(t *testing.T) {
	cfg := itConfig(t)
	var objectDeletes, bucketDeletes int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && strings.Count(strings.Trim(r.URL.Path, "/"), "/") == 1:
			objectDeletes++
			w.WriteHeader(http.StatusNoContent) // public key, not hidden generation
		case r.Method == http.MethodDelete:
			bucketDeletes++
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
	pinTestS3Route(t, cfg, &bucket)
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
	if objectDeletes != 0 || bucketDeletes != 0 {
		t.Errorf("failed bucket deletion touched object bytes: object DELETEs=%d bucket DELETEs=%d", objectDeletes, bucketDeletes)
	}
}

func TestIntegrationDeleteBucketClearsUnreferencedS3Generations(t *testing.T) {
	cfg := itConfig(t)
	orphan := ".bkt-generations/old/" + uuid.NewString()
	var mu sync.Mutex
	physical := map[string]bool{orphan: true}
	var bucketDeletes, objectDeletes int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		parts := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Has("versioning"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			if physical[orphan] {
				_, _ = fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Contents><Key>%s</Key><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`, orphan)
			} else {
				_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated></ListBucketResult>`))
			}
		case r.Method == http.MethodDelete && len(parts) == 2:
			objectDeletes++
			delete(physical, parts[1])
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete:
			bucketDeletes++
			if len(physical) != 0 {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`<Error><Code>BucketNotEmpty</Code></Error>`))
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true

	admin := mkUser(t, itName("bucket-orphan"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	pinTestS3Route(t, cfg, &bucket)
	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	deleted := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil)
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete bucket with unreferenced generation: %d %s", deleted.Code, deleted.Body.String())
	}
	var remaining int64
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if remaining != 0 || len(physical) != 0 || objectDeletes != 1 || bucketDeletes != 2 {
		t.Errorf("orphan cleanup: bucket rows=%d physical=%v object DELETEs=%d bucket DELETEs=%d", remaining, physical, objectDeletes, bucketDeletes)
	}
}

func TestIntegrationDeleteBucketRefusesPublicKeysAndNativeVersioning(t *testing.T) {
	for _, tc := range []struct {
		name, versioning string
		public           bool
	}{
		{name: "unindexed public object", public: true},
		{name: "native S3 versions", versioning: "Enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := itConfig(t)
			physical := map[string]bool{".bkt-generations/orphan": true}
			if tc.public {
				physical["out-of-band-live"] = true
			}
			// A native-versioned bucket retains an older version when an
			// unversioned DeleteObject only removes its current listing entry.
			nativeVersionRemains := tc.versioning != ""
			var objectDeletes int
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodHead:
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodGet && r.URL.Query().Has("versioning"):
					w.Header().Set("Content-Type", "application/xml")
					_, _ = fmt.Fprintf(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>`, tc.versioning)
				case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
					w.Header().Set("Content-Type", "application/xml")
					keys := make([]string, 0, len(physical))
					for key := range physical {
						keys = append(keys, key)
					}
					sort.Strings(keys)
					_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`))
					for _, key := range keys {
						_, _ = fmt.Fprintf(w, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, key)
					}
					_, _ = w.Write([]byte(`<IsTruncated>false</IsTruncated></ListBucketResult>`))
				case r.Method == http.MethodDelete && strings.Contains(strings.Trim(r.URL.Path, "/"), "/"):
					objectDeletes++
					delete(physical, strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)[1])
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodDelete:
					if len(physical) > 0 || nativeVersionRemains {
						w.Header().Set("Content-Type", "application/xml")
						w.WriteHeader(http.StatusConflict)
						_, _ = w.Write([]byte(`<Error><Code>BucketNotEmpty</Code></Error>`))
					} else {
						w.WriteHeader(http.StatusNoContent)
					}
				default:
					w.WriteHeader(http.StatusNotImplemented)
				}
			}))
			defer upstream.Close()
			cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
			cfg.Storage.S3.Region = "us-east-1"
			cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
			cfg.Storage.S3.ForcePathStyle = true
			admin := mkUser(t, itName("bucket-refuse"), "password-123", true, false)
			bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
			pinTestS3Route(t, cfg, &bucket)
			router := itRouter(admin)
			router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
			deleted := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil)
			if deleted.Code != http.StatusConflict {
				t.Fatalf("unsafe cleanup was not refused: %d %s", deleted.Code, deleted.Body.String())
			}
			var remaining int64
			if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&remaining).Error; err != nil {
				t.Fatal(err)
			}
			if remaining != 1 || objectDeletes != 0 || !physical[".bkt-generations/orphan"] || (tc.public && !physical["out-of-band-live"]) {
				t.Errorf("refusal damaged bucket: row=%d physical=%v object DELETEs=%d", remaining, physical, objectDeletes)
			}
		})
	}
}

func TestIntegrationDeleteBucketKeepsMetadataWhenRetryFindsNoSuchBucket(t *testing.T) {
	cfg := itConfig(t)
	var bucketDeletes int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			bucketDeletes++
			if bucketDeletes == 1 {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`<Error><Code>NoSuchBucket</Code></Error>`))
			}
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true
	admin := mkUser(t, itName("bucket-retry"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	pinTestS3Route(t, cfg, &bucket)
	if err := database.DB.Exec(`CREATE FUNCTION bkt_test_block_bucket_delete() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected bucket delete failure'; END;
		$$ LANGUAGE plpgsql`).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.DB.Exec("DROP FUNCTION IF EXISTS bkt_test_block_bucket_delete()") })
	if err := database.DB.Exec(`CREATE TRIGGER bkt_test_block_bucket_delete BEFORE DELETE ON buckets
		FOR EACH ROW EXECUTE FUNCTION bkt_test_block_bucket_delete()`).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.DB.Exec("DROP TRIGGER IF EXISTS bkt_test_block_bucket_delete ON buckets") })
	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	if failed := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil); failed.Code != http.StatusInternalServerError {
		t.Fatalf("injected DB failure: %d %s", failed.Code, failed.Body.String())
	}
	if err := database.DB.Exec("DROP TRIGGER bkt_test_block_bucket_delete ON buckets").Error; err != nil {
		t.Fatal(err)
	}
	if retried := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil); retried.Code != http.StatusInternalServerError {
		t.Fatalf("NoSuchBucket must keep metadata for routing investigation: %d %s", retried.Code, retried.Body.String())
	}
	var remaining int64
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 1 || bucketDeletes != 2 {
		t.Errorf("NoSuchBucket removed metadata: bucket rows=%d upstream DELETEs=%d", remaining, bucketDeletes)
	}
}

func TestIntegrationDeleteBucketChangedEndpointCannotDeleteWrongEmptyBucket(t *testing.T) {
	cfg := itConfig(t)
	cfg.Storage.S3.Endpoint = "original.invalid"
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID, cfg.Storage.S3.SecretAccessKey = "AK", "SK"
	cfg.Storage.S3.ForcePathStyle = true
	admin := mkUser(t, itName("changed-endpoint"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	pinTestS3Route(t, cfg, &bucket)
	var upstreamDeletes int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			upstreamDeletes++
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotImplemented)
	}))
	defer upstream.Close()
	cfg.Storage.S3.Endpoint = strings.TrimPrefix(upstream.URL, "http://")
	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	if got := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil); got.Code != http.StatusConflict {
		t.Fatalf("changed route accepted: %d %s", got.Code, got.Body.String())
	}
	var remaining int64
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 1 || upstreamDeletes != 0 {
		t.Fatalf("changed route touched upstream or metadata: rows=%d deletes=%d", remaining, upstreamDeletes)
	}
}

func TestIntegrationDeleteBucketLegacyRouteRefused(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("legacy-route"), "password-123", true, false)
	bucket := itBucket(t, admin.ID, func(b *models.Bucket) { b.StorageBackend = "s3" })
	router := itRouter(admin)
	router.DELETE("/buckets/:name", NewBucketHandler(cfg).DeleteBucket)
	if got := itDo(router, http.MethodDelete, "/buckets/"+bucket.Name, nil); got.Code != http.StatusConflict {
		t.Fatalf("legacy unpinned route accepted: %d %s", got.Code, got.Body.String())
	}
}
