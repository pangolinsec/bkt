package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestLocal(t *testing.T) *LocalStorage {
	t.Helper()
	dir := t.TempDir()
	return NewLocalStorage(dir)
}

func TestLocalBlobLayoutPaths(t *testing.T) {
	ls := newTestLocal(t)
	key := "../../etc/passwd"
	putString(t, ls, key, "x")
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	if keyHash(key) != h {
		t.Fatalf("keyHash(%q) = %q, want %q", key, keyHash(key), h)
	}
	blob := filepath.Join(ls.rootPath, ".objects", "b", h[0:2], h[2:4], h)
	if got, err := os.ReadFile(blob); err != nil || string(got) != "x" {
		t.Fatalf("blob %s: %q, %v", blob, got, err)
	}
	if got, err := os.ReadFile(blob + ".key"); err != nil || string(got) != key {
		t.Fatalf("sidecar: %q, %v", got, err)
	}
	// Nothing but the blob layout exists under the root.
	entries, _ := os.ReadDir(ls.rootPath)
	if len(entries) != 1 || entries[0].Name() != ".objects" {
		t.Errorf("root holds %v, want only .objects", entries)
	}

	// Bucket names are the only caller-supplied path segment left: anything
	// that is not a single, non-hidden segment is refused.
	for _, b := range []string{"", ".", "..", ".versions", ".objects", "a/b", "a\\b", "../x"} {
		if err := ls.PutObject(b, "k", bytes.NewReader(nil), 0, "", nil); err == nil {
			t.Errorf("PutObject accepted bucket %q", b)
		}
		if _, err := ls.BucketExists(b); err == nil {
			t.Errorf("BucketExists accepted bucket %q", b)
		}
		if err := ls.DeleteBucket(b); err == nil {
			t.Errorf("DeleteBucket accepted bucket %q", b)
		}
	}
	if err := ls.PutObject("b", "", bytes.NewReader(nil), 0, "", nil); err == nil {
		t.Error("PutObject accepted an empty key")
	}
}

func TestLocalBucketLifecycle(t *testing.T) {
	ls := newTestLocal(t)
	if ok, err := ls.BucketExists("nb"); ok || err != nil {
		t.Fatalf("BucketExists before create = %v, %v", ok, err)
	}
	if err := ls.CreateBucket("nb", ""); err != nil {
		t.Fatal(err)
	}
	if ok, err := ls.BucketExists("nb"); !ok || err != nil {
		t.Fatalf("BucketExists after create = %v, %v", ok, err)
	}
	if err := ls.PutObject("nb", "k", bytes.NewReader([]byte("v")), 1, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := ls.ArchiveObjectVersion("nb", "k", testVID); err != nil {
		t.Fatal(err)
	}
	// A not-yet-migrated legacy bucket directory also counts as existing.
	if err := os.MkdirAll(filepath.Join(ls.rootPath, "legacy", "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	if ok, _ := ls.BucketExists("legacy"); !ok {
		t.Error("legacy bucket directory not reported as existing")
	}
	_ = os.MkdirAll(filepath.Join(ls.rootPath, ".versions", "nb", "old"), 0o750)
	_ = os.MkdirAll(filepath.Join(ls.rootPath, "nb", "old"), 0o750)
	if err := ls.DeleteBucket("nb"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{".objects/nb", ".objversions/nb", "nb", ".versions/nb"} {
		if _, err := os.Stat(filepath.Join(ls.rootPath, p)); !os.IsNotExist(err) {
			t.Errorf("%s survived DeleteBucket: %v", p, err)
		}
	}
	if ok, _ := ls.BucketExists("nb"); ok {
		t.Error("deleted bucket still exists")
	}
}

func TestDeleteBucketRequiresDurableParentSync(t *testing.T) {
	ls := newTestLocal(t)
	if err := ls.CreateBucket("sync-test", ""); err != nil {
		t.Fatal(err)
	}
	if err := ls.PutObject("sync-test", "old", bytes.NewReader([]byte("old")), 3, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(ls.rootPath, ".objects")
	var syncs int
	ls.syncDir = func(path string) error {
		if path == parent {
			syncs++
			return errors.New("injected parent fsync failure")
		}
		return syncLocalDirectory(path)
	}
	if err := ls.DeleteBucket("sync-test"); err == nil {
		t.Fatal("DeleteBucket accepted unsynced directory removal")
	}
	if syncs != 1 {
		t.Fatalf("expected parent sync attempt, got %d", syncs)
	}
	ls.syncDir = func(path string) error {
		if path == parent {
			syncs++
		}
		return syncLocalDirectory(path)
	}
	if err := ls.DeleteBucket("sync-test"); err != nil {
		t.Fatalf("retry did not sync absent bucket name: %v", err)
	}
	if syncs != 2 {
		t.Fatalf("retry skipped durability sync after prior failure: %d", syncs)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	ls := newTestLocal(t)
	if err := ls.CreateBucket("b", ""); err != nil {
		t.Fatal(err)
	}
	data := []byte("hello world")
	if err := ls.PutObject("b", "dir/file.txt", bytes.NewReader(data), int64(len(data)), "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	rc, err := ls.GetObject("b", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("round trip mismatch: got %q", got)
	}
}

func TestPutObjectAtomicNoTempLeak(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	data := []byte("payload")
	_ = ls.PutObject("b", "k", bytes.NewReader(data), int64(len(data)), "", nil)
	// No leftover temp files should appear in listings.
	objs, err := ls.ListObjects("b", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if strings.Contains(o.Key, ".tmp-upload-") {
			t.Errorf("temp file leaked into listing: %s", o.Key)
		}
	}
	if len(objs) != 1 {
		t.Errorf("expected 1 object, got %d", len(objs))
	}
}

func TestSelfCopyPreservesData(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	data := []byte("keep me intact")
	_ = ls.PutObject("b", "k", bytes.NewReader(data), int64(len(data)), "", nil)

	// Copying an object onto itself must not truncate it.
	if err := ls.CopyObject("b", "k", "k"); err != nil {
		t.Fatalf("self-copy errored: %v", err)
	}
	rc, err := ls.GetObject("b", "k")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("self-copy corrupted data: got %q want %q", got, data)
	}
}

func TestMultipartUploadIDGuard(t *testing.T) {
	ls := newTestLocal(t)
	// A non-UUID uploadID must be rejected before any filesystem access, so a
	// crafted value can't drive AbortMultipartUpload's os.RemoveAll out of root.
	if _, err := ls.multipartDir("../../etc"); err == nil {
		t.Error("multipartDir accepted a non-UUID uploadID")
	}
	if err := ls.AbortMultipartUpload("b", "k", "../../etc"); err == nil {
		t.Error("AbortMultipartUpload accepted a non-UUID uploadID")
	}
	// uuid.Parse also accepts these (it does not even check the braces of the
	// 38-character form); only the canonical form names a staging dir.
	const id = "0b0c3b51-0000-4000-8000-0000000000aa"
	for _, bad := range []string{"/" + id + "/", "." + id + ".", "{" + id + "}", "urn:uuid:" + id, strings.ToUpper(id), strings.ReplaceAll(id, "-", "")} {
		if _, err := ls.multipartDir(bad); err == nil {
			t.Errorf("multipartDir accepted %q", bad)
		}
		if _, err := ls.GetObjectVersion("b", "k", bad); err == nil || !strings.Contains(err.Error(), "invalid version id") {
			t.Errorf("GetObjectVersion accepted version id %q (%v)", bad, err)
		}
	}
	if _, err := ls.multipartDir(id); err != nil {
		t.Errorf("multipartDir rejected a canonical id: %v", err)
	}
}

func TestMultipartRoundTrip(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	uploadID, err := ls.CreateMultipartUpload("b", "big", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	p1 := bytes.Repeat([]byte("A"), 100)
	p2 := bytes.Repeat([]byte("B"), 50)
	if _, err := ls.UploadPart("b", "big", uploadID, 1, bytes.NewReader(p1), int64(len(p1))); err != nil {
		t.Fatal(err)
	}
	if _, err := ls.UploadPart("b", "big", uploadID, 2, bytes.NewReader(p2), int64(len(p2))); err != nil {
		t.Fatal(err)
	}
	if err := ls.CompleteMultipartUpload("b", "big", uploadID, []CompletedPart{{PartNumber: 1}, {PartNumber: 2}}); err != nil {
		t.Fatal(err)
	}
	rc, _ := ls.GetObject("b", "big")
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != 150 || !bytes.Equal(got[:100], p1) || !bytes.Equal(got[100:], p2) {
		t.Errorf("assembled object wrong: len=%d", len(got))
	}
	// The staging directory must be gone after completion.
	if _, err := os.Stat(filepath.Join(ls.rootPath, ".multipart", uploadID)); !os.IsNotExist(err) {
		t.Error("multipart staging dir not cleaned up")
	}
}

func TestCompleteWithNoPartsFails(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	uploadID, _ := ls.CreateMultipartUpload("b", "k", "", nil)
	if err := ls.CompleteMultipartUpload("b", "k", uploadID, nil); err == nil {
		t.Error("completing with zero parts should fail")
	}
}
