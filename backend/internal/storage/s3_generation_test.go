package storage

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestS3MultipartGenerationUsesPrivatePhysicalKey(t *testing.T) {
	id := uuid.NewString()
	physical, err := s3GenerationKey("user/key", id)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/"+physical {
			t.Errorf("multipart request used public key: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls = append(calls, r.Method+" "+r.URL.RawQuery)
		q, _ := url.ParseQuery(r.URL.RawQuery)
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && q.Get("partNumber") == "1":
			w.Header().Set("ETag", `"part-etag"`)
		case r.Method == http.MethodPost && q.Get("uploadId") == "upload-id":
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("multipart completion lacked create-only condition")
			}
			var complete struct {
				Parts []struct {
					ETag string `xml:"ETag"`
				} `xml:"Part"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&complete); err != nil || len(complete.Parts) != 1 || complete.Parts[0].ETag != `"part-etag"` {
				t.Errorf("completion did not preserve quoted upstream part ETag: %+v %v", complete, err)
			}
			w.Header().Set("ETag", `"complete-etag"`)
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodHead:
			w.Header().Set("ETag", `"complete-etag"`)
			w.Header().Set("Content-Length", "3")
		case r.Method == http.MethodGet && q.Get("uploadId") == "upload-id":
			_, _ = w.Write([]byte(`<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>3</Size><ETag>"part-etag"</ETag></Part></ListPartsResult>`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected multipart request: %s %s", r.Method, r.URL.RawQuery)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()
	s, err := NewS3Storage(strings.TrimPrefix(srv.URL, "http://"), "us-east-1", "AK", "SK", "", false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	uploadID, err := s.CreateGenerationMultipartUpload("bucket", "user/key", id, "text/plain", nil)
	if err != nil || uploadID != "upload-id" {
		t.Fatalf("init: %q %v", uploadID, err)
	}
	etag, err := s.UploadGenerationPart("bucket", "user/key", id, uploadID, 1, strings.NewReader("new"), 3)
	if err != nil || etag != "part-etag" {
		t.Fatalf("part: %q %v", etag, err)
	}
	listed, err := s.ListGenerationParts("bucket", "user/key", id, uploadID)
	if err != nil || len(listed) != 1 || listed[0].PartNumber != 1 || listed[0].Size != 3 {
		t.Fatalf("list parts: %+v %v", listed, err)
	}
	sizes, err := s.GenerationPartSizes("bucket", "user/key", id, uploadID)
	if err != nil || sizes[1] != 3 {
		t.Fatalf("part sizes: %+v %v", sizes, err)
	}
	info, err := s.CompleteGenerationMultipartUpload("bucket", "user/key", id, uploadID, []CompletedPart{{PartNumber: 1, ETag: etag}})
	if err != nil || info == nil || info.Size != 3 || info.ETag != "complete-etag" {
		t.Fatalf("complete: %+v %v", info, err)
	}
	if err := s.AbortGenerationMultipartUpload("bucket", "user/key", id, uploadID); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 7 {
		t.Errorf("expected initiate, part, two lists, complete, head, abort; got %v", calls)
	}
}
