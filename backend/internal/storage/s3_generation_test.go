package storage

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestS3GenerationConditionalPutAndRead(t *testing.T) {
	var mu sync.Mutex
	var stored []byte
	var keyPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, "/bucket/"+s3GenerationPrefix) {
			t.Errorf("unexpected generation path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if keyPath != "" && keyPath != r.URL.Path {
			t.Errorf("generation path changed: %q, %q", keyPath, r.URL.Path)
		}
		keyPath = r.URL.Path
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("generation PUT lacked create-only condition")
			}
			if stored != nil {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			stored, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"etag"`)
		case http.MethodGet:
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Range") == "bytes=0-2" {
				w.Header().Set("Content-Range", "bytes 0-2/9")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(stored[:3])
				return
			}
			_, _ = w.Write(stored)
		case http.MethodDelete:
			stored = nil
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()
	s, err := NewS3Storage(strings.TrimPrefix(srv.URL, "http://"), "us-east-1", "AK", "SK", "", false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	info, err := s.PutGeneration("bucket", "user/key", id, strings.NewReader("old-bytes"), 9, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag != "etag" || info.Size != 9 {
		t.Errorf("generation info %+v", info)
	}
	if _, err := s.PutGeneration("bucket", "user/key", id, strings.NewReader("new-bytes"), 9, "text/plain", nil); err == nil {
		t.Error("conditional PUT replaced an existing generation")
	}
	rc, err := s.GetGenerationRange("bucket", "user/key", id, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || string(got) != "old" {
		t.Errorf("range = %q %v", got, err)
	}
	if err := s.DeleteGeneration("bucket", "user/key", id); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if stored != nil {
		t.Error("generation not removed")
	}
}
