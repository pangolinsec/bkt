package storage

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type generationFailReader struct{}

func (generationFailReader) Read([]byte) (int, error) {
	return 0, errors.New("injected reader failure")
}

func TestLocalGenerationImmutableAndPinnedReader(t *testing.T) {
	ls := NewLocalStorage(t.TempDir())
	id := uuid.NewString()
	info, err := ls.PutGeneration("bucket", "a/b", id, strings.NewReader("old-bytes"), 9, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 9 || info.ETag == "" {
		t.Fatalf("invalid generation info: %+v", info)
	}
	reader, err := ls.GetGeneration("bucket", "a/b", id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ls.PutGeneration("bucket", "a/b", id, strings.NewReader("new-bytes"), 9, "text/plain", nil); err == nil {
		t.Error("duplicate generation replaced committed bytes")
	}
	if _, err := ls.PutGeneration("bucket", "other", id, strings.NewReader("other"), 5, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	rangeReader, err := ls.GetGenerationRange("bucket", "a/b", id, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	ranged, _ := io.ReadAll(rangeReader)
	rangeReader.Close()
	if string(ranged) != "old" {
		t.Errorf("range = %q", ranged)
	}
	if err := ls.DeleteGeneration("bucket", "a/b", id); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(got) != "old-bytes" {
		t.Errorf("open reader lost generation: %q %v", got, err)
	}
	if _, err := ls.GetGeneration("bucket", "a/b", id); err == nil {
		t.Error("deleted generation still opens")
	}
	other, err := ls.GetGeneration("bucket", "other", id)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(other)
	other.Close()
	if err != nil || string(got) != "other" {
		t.Errorf("other key = %q %v", got, err)
	}
}

func TestLocalGenerationFailureNeverPublishes(t *testing.T) {
	ls := NewLocalStorage(t.TempDir())
	id := uuid.NewString()
	if _, err := ls.PutGeneration("bucket", "key", id, generationFailReader{}, 1, "", nil); err == nil {
		t.Fatal("reader failure accepted")
	}
	if _, err := ls.GetGeneration("bucket", "key", id); err == nil {
		t.Error("failed write published bytes")
	}
	if _, err := ls.PutGeneration("bucket", "key", id, strings.NewReader("short"), 6, "", nil); err == nil {
		t.Fatal("short body accepted")
	}
	if _, err := ls.GetGeneration("bucket", "key", id); err == nil {
		t.Error("short write published bytes")
	}
	if _, err := ls.PutGeneration("bucket", "key", id, strings.NewReader("good"), 4, "", nil); err != nil {
		t.Fatal(err)
	}
}
