package api

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"bkt/internal/config"
	"bkt/internal/models"
	"bkt/internal/storage"

	"github.com/google/uuid"
)

func TestLocalRootIdentityRecoversAfterInterruptedTempWrite(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".bkt-root-id-tmp-interrupted"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := ensureLocalRootIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("invalid root identity %q: %v", id, err)
	}
	again, err := ensureLocalRootIdentity(root)
	if err != nil || again != id {
		t.Fatalf("identity was not durable: %q %q %v", id, again, err)
	}
}

func reconcileRows(n int, age time.Duration, now time.Time) []models.Object {
	rows := make([]models.Object, n)
	for i := range rows {
		rows[i] = models.Object{ID: uuid.New(), Key: fmt.Sprintf("k%03d", i), UpdatedAt: now.Add(-age)}
	}
	return rows
}

func TestPlanReconcilePrune(t *testing.T) {
	now := time.Now()
	old := time.Hour

	t.Run("prunes only old rows absent from a complete listing", func(t *testing.T) {
		rows := reconcileRows(20, old, now)
		rows[1].UpdatedAt = now.Add(-time.Minute) // recently written: kept
		listed := map[string]storage.ObjectInfo{}
		for _, r := range rows[3:] {
			listed[r.Key] = storage.ObjectInfo{Key: r.Key}
		}
		plan := planReconcilePrune(rows, listed, true, now)
		if plan.suspicious || plan.missing != 3 {
			t.Fatalf("plan = %+v", plan)
		}
		if len(plan.stale) != 2 || plan.stale[0].Key != "k000" || plan.stale[1].Key != "k002" {
			t.Errorf("stale = %v", plan.stale)
		}
		if len(plan.keep) != 18 {
			t.Errorf("keep = %d rows", len(plan.keep))
		}
	})

	t.Run("incomplete listing never prunes", func(t *testing.T) {
		rows := reconcileRows(5, old, now)
		plan := planReconcilePrune(rows, map[string]storage.ObjectInfo{}, false, now)
		if len(plan.stale) != 0 || len(plan.keep) != 5 {
			t.Errorf("plan = %+v", plan)
		}
	})

	t.Run("safety valve: most rows missing means misrouted, not deleted", func(t *testing.T) {
		rows := reconcileRows(30, old, now)
		listed := map[string]storage.ObjectInfo{}
		for _, r := range rows[:10] {
			listed[r.Key] = storage.ObjectInfo{Key: r.Key}
		}
		plan := planReconcilePrune(rows, listed, true, now)
		if !plan.suspicious || len(plan.stale) != 0 || len(plan.keep) != 30 {
			t.Fatalf("20/30 missing must trip the valve: suspicious=%v stale=%d keep=%d", plan.suspicious, len(plan.stale), len(plan.keep))
		}
		for i := 1; i < len(plan.keep); i++ {
			if plan.keep[i-1].Key >= plan.keep[i].Key {
				t.Fatal("kept rows not in key order")
			}
		}
	})

	t.Run("small absolute counts do not trip the valve", func(t *testing.T) {
		rows := reconcileRows(10, old, now) // all 10 missing, but <= 10
		plan := planReconcilePrune(rows, map[string]storage.ObjectInfo{}, true, now)
		if plan.suspicious || len(plan.stale) != 10 {
			t.Errorf("plan: suspicious=%v stale=%d", plan.suspicious, len(plan.stale))
		}
	})

	t.Run("unpersisted rows are never stale", func(t *testing.T) {
		rows := []models.Object{{Key: "imported", UpdatedAt: now.Add(-old)}}
		plan := planReconcilePrune(rows, map[string]storage.ObjectInfo{}, true, now)
		if len(plan.stale) != 0 || len(plan.keep) != 1 {
			t.Errorf("plan = %+v", plan)
		}
	})

	t.Run("immutable generations are absent from public physical listings", func(t *testing.T) {
		rows := reconcileRows(1, old, now)
		rows[0].GenerationID = uuid.NewString()
		plan := planReconcilePrune(rows, map[string]storage.ObjectInfo{}, true, now)
		if len(plan.stale) != 0 || len(plan.keep) != 1 || plan.missing != 0 {
			t.Errorf("generation-backed row classified as stale: %+v", plan)
		}
	})
}

func TestS3ConfigLocationChanges(t *testing.T) {
	cur := &models.S3Configuration{
		Endpoint: "s3.example.com", Region: "us-east-1", BucketPrefix: "pfx-",
		UseSSL: true, ForcePathStyle: false,
	}
	str := func(s string) *string { return &s }
	bl := func(b bool) *bool { return &b }

	// The console re-submits every field; unchanged values are not changes,
	// and credentials/name/default are never location changes.
	same := &models.UpdateS3ConfigRequest{
		Name: "renamed", Endpoint: "s3.example.com", Region: "us-east-1", BucketPrefix: str("pfx-"),
		UseSSL: bl(true), ForcePathStyle: bl(false), IsDefault: bl(true),
		AccessKeyID: "NEWKEY", SecretAccessKey: "newsecret",
	}
	if got := s3ConfigLocationChanges(cur, same); len(got) != 0 {
		t.Errorf("unchanged form reported changes: %v", got)
	}
	if got := s3ConfigLocationChanges(cur, &models.UpdateS3ConfigRequest{}); len(got) != 0 {
		t.Errorf("empty request reported changes: %v", got)
	}

	all := &models.UpdateS3ConfigRequest{
		Endpoint: "other.example.com", Region: "eu-west-1", BucketPrefix: str(""),
		UseSSL: bl(false), ForcePathStyle: bl(true),
	}
	want := []string{"endpoint", "region", "bucket_prefix", "use_ssl", "force_path_style"}
	if got := s3ConfigLocationChanges(cur, all); !reflect.DeepEqual(got, want) {
		t.Errorf("changes = %v, want %v", got, want)
	}
}

func TestRenameDestinationKey(t *testing.T) {
	cases := []struct{ src, name, want string }{
		{"a.txt", "b.txt", "b.txt"},
		{"dir/a.txt", "b.txt", "dir/b.txt"},
		{"dir/", "renamed", "renamed/"},
		{"a/dir/", "renamed", "a/renamed/"},
	}
	for _, c := range cases {
		if got := renameDestinationKey(c.src, c.name); got != c.want {
			t.Errorf("renameDestinationKey(%q, %q) = %q, want %q", c.src, c.name, got, c.want)
		}
	}
}

// A bucket without an S3ConfigID is served by the .env S3 settings — never
// by whichever DB configuration is currently the default (that dynamic
// resolution re-routed existing buckets). getStorageBackend must not consult
// the database at all for such a bucket (database.DB is nil in unit tests).
func TestGetStorageBackendUnpinnedS3BucketUsesEnv(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.S3.Endpoint = "env-s3.invalid:9000"
	cfg.Storage.S3.Region = "us-east-1"
	cfg.Storage.S3.AccessKeyID = "envkey"
	cfg.Storage.S3.SecretAccessKey = "envsecret"
	cfg.Storage.S3.BucketPrefix = "env-"
	h := &BucketHandler{config: cfg}
	InvalidateS3ConfigCache()
	// A stale "default" cache entry (as the old resolution stored) must be ignored.
	setS3ConfigInCache("default", &s3ConfigData{Endpoint: "db-default.invalid", Region: "us-east-1", BucketPrefix: "db-"})
	defer InvalidateS3ConfigCache()

	backend, err := h.getStorageBackend(&models.Bucket{Name: "b", StorageBackend: "s3"})
	if err != nil {
		t.Fatalf("getStorageBackend: %v", err)
	}
	s3b, ok := backend.(*storage.S3Storage)
	if !ok {
		t.Fatalf("backend = %T, want *storage.S3Storage", backend)
	}
	if prefix := reflect.ValueOf(s3b).Elem().FieldByName("bucketPrefix").String(); prefix != "env-" {
		t.Errorf("unpinned S3 bucket served with bucket prefix %q, want the .env prefix", prefix)
	}
}
