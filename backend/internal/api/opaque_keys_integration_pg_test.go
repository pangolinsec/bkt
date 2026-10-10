package api

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bkt/internal/models"
)

// itBlobPath / itVersionDir mirror the legacy local backend's blob layout.
func itBlobPath(root, bucket, key string) string {
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	return filepath.Join(root, ".objects", bucket, h[0:2], h[2:4], h)
}

func itVersionDir(root, bucket, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(root, ".objversions", bucket, hex.EncodeToString(sum[:]))
}

func itGenerationPath(root, bucket, key, generationID string) string {
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	return filepath.Join(root, ".objgenerations", bucket, h[0:2], h[2:4], h, generationID)
}

// Every S3-valid key works on a local bucket through the S3 handlers — keys
// that the old key-as-path layout could not hold ("p" next to "p/q") or
// rejected ("a..b", "/leading", "\\") included — and none of them is ever
// used as a filesystem path.
func TestIntegrationS3LocalOpaqueKeys(t *testing.T) {
	cfg := itConfig(t)
	admin := mkUser(t, itName("adm"), "password-123", true, false)
	b := itBucket(t, admin.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	r := itS3Router(cfg, admin)
	r.DELETE("/:bucket/*key", NewS3APIHandler(cfg).DeleteObject)

	cases := []struct{ urlPath, key string }{
		{"p", "p"},
		{"p/q", "p/q"},
		{"m/", "m/"},
		{"m", "m"},
		{"a..b", "a..b"},
		{"/leading", "/leading"}, // URL "/bucket//leading"
		{"%5C", "\\"},
		{"a%5Cb", "a\\b"},
		{"a//b", "a//b"},
		{"../x", "../x"},
		{strings.Repeat("s", 300), strings.Repeat("s", 300)},
	}
	for _, tc := range cases {
		s3PutString(t, r, b.Name, tc.urlPath, "v1:"+tc.key)
		if row, ok := currentRow(t, b.ID, tc.key); !ok || row.Size != int64(len("v1:"+tc.key)) {
			t.Errorf("PUT %q: row %+v ok=%v", tc.key, row, ok)
		}
	}
	for _, tc := range cases {
		if code, body := s3GetString(t, r, b.Name, tc.urlPath); code != http.StatusOK || body != "v1:"+tc.key {
			t.Errorf("GET %q: %d %q", tc.key, code, body)
		}
		row, ok := currentRow(t, b.ID, tc.key)
		if !ok || row.GenerationID == "" {
			t.Errorf("missing immutable generation for %q: %+v", tc.key, row)
			continue
		}
		if _, err := os.Stat(itGenerationPath(cfg.Storage.RootPath, b.Name, tc.key, row.GenerationID)); err != nil {
			t.Errorf("generation of %q: %v", tc.key, err)
		}
		if _, err := os.Stat(itBlobPath(cfg.Storage.RootPath, b.Name, tc.key)); !os.IsNotExist(err) {
			t.Errorf("unexpected mutable public blob for %q: %v", tc.key, err)
		}
	}
	// Nothing was written at a key-derived path: the root holds only the
	// blob-layout trees.
	entries, _ := os.ReadDir(cfg.Storage.RootPath)
	for _, e := range entries {
		if e.Name() != ".objects" && e.Name() != ".objversions" && e.Name() != ".objgenerations" {
			t.Errorf("unexpected entry %q in the storage root", e.Name())
		}
	}

	// Versioned overwrite of "p" archives v1 without touching "p/q"; a
	// versioned delete of "p/q" leaves "p" alone.
	s3PutString(t, r, b.Name, "p", "v2:p")
	vs := versionRows(t, b.ID, "p")
	if len(vs) != 1 {
		t.Fatalf("versions of p: %+v", vs)
	}
	if code, body := s3GetString(t, r, b.Name, "p?versionId="+vs[0].VersionID); code != http.StatusOK || body != "v1:p" {
		t.Errorf("GET p?versionId: %d %q", code, body)
	}
	if vs[0].GenerationID == "" {
		t.Error("archived version has no immutable generation")
	} else if _, err := os.Stat(itGenerationPath(cfg.Storage.RootPath, b.Name, "p", vs[0].GenerationID)); err != nil {
		t.Errorf("archived generation bytes: %v", err)
	}
	if w := itDo(r, http.MethodDelete, "/"+b.Name+"/p/q", nil); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE p/q: %d %s", w.Code, w.Body.String())
	}
	if code, _ := s3GetString(t, r, b.Name, "p/q"); code != http.StatusNotFound {
		t.Errorf("GET p/q after delete: %d", code)
	}
	for key, want := range map[string]string{"p": "v2:p", "m/": "v1:m/", "m": "v1:m"} {
		if code, body := s3GetString(t, r, b.Name, key); code != http.StatusOK || body != want {
			t.Errorf("GET %q: %d %q, want %q", key, code, body, want)
		}
	}

	// Keys S3 itself rejects are still refused.
	if w := itDo(r, http.MethodPut, "/"+b.Name+"/"+strings.Repeat("k", 1025), []byte("x")); w.Code != http.StatusBadRequest {
		t.Errorf("PUT 1025-byte key: %d", w.Code)
	}
	if w := itDo(r, http.MethodPut, "/"+b.Name+"/.bkt-versions/x", []byte("x")); w.Code != http.StatusBadRequest {
		t.Errorf("PUT reserved key: %d", w.Code)
	}
	if w := itDo(r, http.MethodPut, "/"+b.Name+"/bad%FFutf8", []byte("x")); w.Code != http.StatusBadRequest {
		t.Errorf("PUT invalid UTF-8 key: %d", w.Code)
	}
}
