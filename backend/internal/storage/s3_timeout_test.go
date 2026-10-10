package storage

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func respErr(status int) error {
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      errors.New(http.StatusText(status)),
	}}
}

func TestIsS3BucketNotEmptyRequiresErrorCode(t *testing.T) {
	response := func(status int, code string) error {
		return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code},
		}}
	}
	if !IsS3BucketNotEmpty(response(http.StatusConflict, "BucketNotEmpty")) {
		t.Error("BucketNotEmpty refusal should allow guarded orphan cleanup")
	}
	for _, err := range []error{
		response(http.StatusConflict, "OperationAborted"),
		response(http.StatusForbidden, "BucketNotEmpty"),
		respErr(http.StatusConflict),
	} {
		if IsS3BucketNotEmpty(err) {
			t.Errorf("unrelated upstream refusal allowed orphan cleanup: %v", err)
		}
	}
}

func TestClassifyMoveDestinationHead(t *testing.T) {
	if err := classifyMoveDestinationHead(nil, "b", "k"); err == nil {
		t.Error("existing destination must be refused")
	}
	if err := classifyMoveDestinationHead(respErr(404), "b", "k"); err != nil {
		t.Errorf("404 must mean free: %v", err)
	}
	if err := classifyMoveDestinationHead(respErr(403), "b", "k"); err != nil {
		t.Errorf("403 (no s3:ListBucket) must proceed: %v", err)
	}
	if err := classifyMoveDestinationHead(respErr(500), "b", "k"); err == nil {
		t.Error("other upstream errors must fail the archive")
	}
}

// fakeS3NoListBucket mimics AWS for a credential without s3:ListBucket: HEAD
// of a missing key is 403. CopyObject and DeleteObject succeed.
type fakeS3NoListBucket struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeS3NoListBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	f.mu.Unlock()
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusForbidden)
	case r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"e"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>`))
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func TestArchiveProceedsWhenHeadIsForbidden(t *testing.T) {
	fake := &fakeS3NoListBucket{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	s, err := NewS3Storage(strings.TrimPrefix(srv.URL, "http://"), "us-east-1", "AK", "SK", "", false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ArchiveObjectVersion("bkt", "dir/file.txt", "0b0e5a5e-7c1c-4a53-9d0e-0f7f2b0c6a11"); err != nil {
		t.Fatalf("archive failed although the destination HEAD was only forbidden: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.calls) != 3 || !strings.HasPrefix(fake.calls[0], "HEAD") ||
		!strings.HasPrefix(fake.calls[1], "PUT") || !strings.HasPrefix(fake.calls[2], "DELETE") {
		t.Errorf("unexpected upstream calls: %v", fake.calls)
	}
}
