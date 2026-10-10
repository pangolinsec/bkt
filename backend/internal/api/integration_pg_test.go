package api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/database/pgtest"
	"bkt/internal/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Shared harness for this package's Postgres-gated integration tests (see
// integrationDB). All of them run in ONE database created for this test
// binary on first use and dropped in TestMain, so `go test ./...` can run
// packages in parallel against the same server without interference.

var (
	itDBOnce     sync.Once
	itDBSettings pgtest.Settings
	itDBName     string
	itDBErr      error
)

// packageTestDatabase returns the test binary's database (skipping the test
// when BKT_TEST_POSTGRES_HOST is unset).
func packageTestDatabase(t *testing.T) (pgtest.Settings, string) {
	t.Helper()
	s := pgtest.Require(t)
	itDBOnce.Do(func() {
		itDBSettings = s
		itDBName, itDBErr = s.CreateDatabase("api")
	})
	if itDBErr != nil {
		t.Fatal(itDBErr)
	}
	return itDBSettings, itDBName
}

func TestMain(m *testing.M) {
	code := m.Run()
	if itDBName != "" {
		if database.DB != nil {
			if sqlDB, err := database.DB.DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
		if err := itDBSettings.DropDatabase(itDBName); err != nil {
			fmt.Fprintln(os.Stderr, "drop test database:", err)
		}
	}
	os.Exit(code)
}

// itConfig is integrationDB plus a local storage root in a temp dir.
func itConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := integrationDB(t)
	cfg.Storage.Backend = "local"
	cfg.Storage.RootPath = t.TempDir()
	return cfg
}

// itName returns a unique, S3-valid name with the given prefix.
func itName(prefix string) string {
	return prefix + "-" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
}

// itBucket creates a bucket row (plus its local directory when root != "")
// and removes the row and everything referencing it when the test ends.
func itBucket(t *testing.T, owner uuid.UUID, mutate func(*models.Bucket)) models.Bucket {
	t.Helper()
	b := models.Bucket{ID: uuid.New(), Name: itName("itb"), OwnerID: owner, StorageBackend: "local"}
	if mutate != nil {
		mutate(&b)
	}
	if err := database.DB.Create(&b).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { itDeleteBucketRows(b.ID) })
	return b
}

func itDeleteBucketRows(id uuid.UUID) {
	database.DB.Exec(`DELETE FROM object_versions WHERE bucket_id = ?`, id)
	database.DB.Exec(`DELETE FROM objects WHERE bucket_id = ?`, id)
	database.DB.Exec(`DELETE FROM bucket_policies WHERE bucket_id = ?`, id)
	database.DB.Exec(`DELETE FROM buckets WHERE id = ?`, id)
}

// itRouter is a gin engine whose requests are authenticated as u.
func itRouter(u models.User) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", u.ID)
		c.Set("username", u.Username)
		c.Set("is_admin", u.IsAdmin)
		c.Next()
	})
	return r
}

// itS3Router mounts the S3 routes used by these tests.
func itS3Router(cfg *config.Config, u models.User) *gin.Engine {
	h := NewS3APIHandler(cfg)
	r := itRouter(u)
	r.GET("/:bucket", h.ListObjects)
	r.PUT("/:bucket", h.CreateBucket)
	r.POST("/:bucket", h.HandleBucketPost)
	r.PUT("/:bucket/*key", h.PutObject)
	r.GET("/:bucket/*key", h.GetObject)
	r.HEAD("/:bucket/*key", h.HeadObject)
	return r
}

func itDo(r http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func s3PutString(t *testing.T, r http.Handler, bucket, key, body string) {
	t.Helper()
	if w := itDo(r, http.MethodPut, "/"+bucket+"/"+key, []byte(body)); w.Code != http.StatusOK {
		t.Fatalf("PUT %s/%s: %d %s", bucket, key, w.Code, w.Body.String())
	}
}

func s3GetString(t *testing.T, r http.Handler, bucket, key string) (int, string) {
	t.Helper()
	w := itDo(r, http.MethodGet, "/"+bucket+"/"+key, nil)
	return w.Code, w.Body.String()
}

func itS3ErrCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var e Error
	if err := xml.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("not an S3 error (%d): %s", w.Code, w.Body.String())
	}
	return e.Code
}

func currentRow(t *testing.T, bucketID uuid.UUID, key string) (models.Object, bool) {
	t.Helper()
	var o models.Object
	err := database.DB.Where("bucket_id = ? AND key = ?", bucketID, key).First(&o).Error
	return o, err == nil
}

func versionRows(t *testing.T, bucketID uuid.UUID, key string) []models.ObjectVersion {
	t.Helper()
	var vs []models.ObjectVersion
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucketID, key).Order(versionOrder).Find(&vs).Error; err != nil {
		t.Fatal(err)
	}
	return vs
}
