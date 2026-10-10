package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"bkt/internal/validation"
)

// Spellings that a filesystem path would have aliased ("a//b" and "./a/b"
// onto "a/b") are distinct keys in the blob layout: writing, copying onto or
// deleting any of them never touches the canonical object.
func TestLocalPathSpellingsAreDistinctKeys(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	data := []byte("secret")
	if err := ls.PutObject("b", "dir/file", bytes.NewReader(data), int64(len(data)), "", nil); err != nil {
		t.Fatal(err)
	}
	aliases := []string{"dir//file", "./dir/file", "dir/./file", "dir/.", "dir//", "dir/./", "dir/.bkt-folder", "dir/sub/../file", "/dir/file", "dir\\file"}
	for _, k := range aliases {
		if _, err := ls.GetObject("b", k); err == nil {
			t.Errorf("GetObject(%q) found an object before it was written", k)
		}
		if err := ls.PutObject("b", k, bytes.NewReader([]byte("x-"+k)), int64(len(k)+2), "", nil); err != nil {
			t.Errorf("PutObject(%q): %v", k, err)
		}
		if got := readString(t, ls, k); got != "x-"+k {
			t.Errorf("GetObject(%q) = %q", k, got)
		}
		if err := ls.CopyObject("b", "dir/file", k); err != nil {
			t.Errorf("CopyObject(-> %q): %v", k, err)
		}
		if got := readString(t, ls, k); got != "secret" {
			t.Errorf("after copy GetObject(%q) = %q", k, got)
		}
		if err := ls.DeleteObject("b", k); err != nil {
			t.Errorf("DeleteObject(%q): %v", k, err)
		}
		if got := readString(t, ls, "dir/file"); got != "secret" {
			t.Fatalf("canonical object changed through %q: %q", k, got)
		}
	}
	if got := listKeys(t, ls, ""); len(got) != 1 || got[0] != "dir/file" {
		t.Errorf("listing = %q", got)
	}
}

func TestLocalArchiveNeverOverwritesExistingVersion(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	const vid = "0b0c3b51-0000-4000-8000-000000000001"

	put := func(s string) {
		t.Helper()
		if err := ls.PutObject("b", "k", bytes.NewReader([]byte(s)), int64(len(s)), "", nil); err != nil {
			t.Fatal(err)
		}
	}
	put("v1")
	if err := ls.ArchiveObjectVersion("b", "k", vid); err != nil {
		t.Fatalf("first archive: %v", err)
	}
	// A second archive under the same version id (e.g. two racing overwrites
	// that both read the same current row) must fail rather than destroy v1.
	put("v2")
	if err := ls.ArchiveObjectVersion("b", "k", vid); err == nil {
		t.Fatal("second archive under the same version id succeeded; archived bytes were overwritten")
	}
	rc, err := ls.GetObjectVersion("b", "k", vid)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if got, _ := io.ReadAll(rc); string(got) != "v1" {
		t.Errorf("archived version = %q, want v1", got)
	}
	// The current object is untouched by the failed archive.
	cur, err := ls.GetObject("b", "k")
	if err != nil {
		t.Fatalf("current object lost after refused archive: %v", err)
	}
	defer cur.Close()
	if got, _ := io.ReadAll(cur); string(got) != "v2" {
		t.Errorf("current = %q, want v2", got)
	}
}

// Keys that look like paths into internal storage are just keys: they are
// stored as blobs of their own inside the bucket's object tree and can never
// reach version storage, multipart staging, another bucket or the root.
func TestLocalInternalStorageUnreachableFromKeys(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	_ = ls.CreateBucket("other", "")
	putString(t, ls, "k", "current")
	if err := ls.ArchiveObjectVersion("b", "k", testVID); err != nil {
		t.Fatal(err)
	}
	keys := []string{"../.versions/b/k", "../.objversions/b/" + keyHash("k") + "/" + testVID, "../.multipart/x", "../other/k", "../../../../etc/passwd", "/etc/passwd"}
	for _, k := range keys {
		putString(t, ls, k, "user data")
	}
	rc, err := ls.GetObjectVersion("b", "k", testVID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "current" {
		t.Errorf("archived version = %q", got)
	}
	if objs, _ := ls.ListObjects("other", ""); len(objs) != 0 {
		t.Errorf("other bucket gained objects: %v", objs)
	}
	for _, name := range []string{".versions", ".multipart", "etc"} {
		if _, err := os.Stat(filepath.Join(ls.rootPath, name)); !os.IsNotExist(err) {
			t.Errorf("%s was created by a user write", name)
		}
	}
	if got := listKeys(t, ls, "../"); len(got) != 5 {
		t.Errorf("listing ../ = %q", got)
	}
	if err := ls.PutObject(".versions", "b/k", bytes.NewReader([]byte("x")), 1, "", nil); err == nil {
		t.Error("a bucket named .versions must not be addressable")
	}
}

func TestGetObjectRangeLocalAndFallback(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	data := []byte("0123456789abcdef")
	_ = ls.PutObject("b", "k", bytes.NewReader(data), int64(len(data)), "", nil)

	rc, err := GetObjectRange(ls, "b", "k", 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "45678" {
		t.Errorf("native range = %q, want 45678", got)
	}

	// A backend without RangeReader falls back to GetObject + skip.
	rc, err = GetObjectRange(noRange{ls}, "b", "k", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "abcdef" {
		t.Errorf("fallback range = %q, want abcdef", got)
	}
}

// noRange hides LocalStorage's RangeReader implementation.
type noRange struct{ StorageBackend }

func TestLocalPartSizes(t *testing.T) {
	ls := newTestLocal(t)
	_ = ls.CreateBucket("b", "")
	id, err := ls.CreateMultipartUpload("b", "k", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ls.UploadPart("b", "k", id, 1, bytes.NewReader(make([]byte, 10)), 10)
	_, _ = ls.UploadPart("b", "k", id, 3, bytes.NewReader(make([]byte, 7)), 7)
	sizes, err := PartSizes(ls, "b", "k", id)
	if err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[1] != 10 || sizes[3] != 7 {
		t.Errorf("PartSizes = %v", sizes)
	}
}

func TestS3ReservedPrefixMatchesValidation(t *testing.T) {
	if s3VersionPrefix != validation.ReservedObjectKeyPrefix {
		t.Fatalf("storage version prefix %q != validation reserved prefix %q", s3VersionPrefix, validation.ReservedObjectKeyPrefix)
	}
	if s3GenerationPrefix != validation.ReservedGenerationKeyPrefix {
		t.Fatalf("storage generation prefix %q != validation reserved prefix %q", s3GenerationPrefix, validation.ReservedGenerationKeyPrefix)
	}
	if err := checkS3UserKey(".bkt-generations/a/b"); err == nil {
		t.Error("checkS3UserKey accepted a generation key")
	}
	if err := checkS3UserKey(".bkt-versions/a/b"); err == nil {
		t.Error("checkS3UserKey accepted a reserved key")
	}
	if err := checkS3UserKey("docs/a"); err != nil {
		t.Errorf("checkS3UserKey rejected a normal key: %v", err)
	}
	if _, err := s3VersionKey(".bkt-versions/x", "0b0c3b51-0000-4000-8000-000000000001"); err == nil {
		t.Error("s3VersionKey accepted a key inside the version keyspace")
	}
}

func TestS3CopySourceEncoding(t *testing.T) {
	cases := map[string]string{
		"plain/key.txt":        "bkt/plain/key.txt",
		"a b+c%d?e#f&g=h":      "bkt/a%20b%2Bc%25d%3Fe%23f%26g%3Dh",
		"dir/ünï.txt":          "bkt/dir/%C3%BCn%C3%AF.txt",
		"keep-_.~chars":        "bkt/keep-_.~chars",
		"folder/":              "bkt/folder/",
		"colon:semi;comma,at@": "bkt/colon%3Asemi%3Bcomma%2Cat%40",
	}
	for key, want := range cases {
		if got := s3CopySource("bkt", key); got != want {
			t.Errorf("s3CopySource(%q) = %q, want %q", key, got, want)
		}
	}
}
