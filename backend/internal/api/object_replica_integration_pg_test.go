package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"bkt/internal/config"
	"bkt/internal/database"
	"bkt/internal/database/pgtest"
	"bkt/internal/models"

	"github.com/google/uuid"
)

// replicaBody stops after the handler has checked the key's condition, while
// it still holds its key lock, so a second process can reach the same point.
type replicaBody struct {
	data  []byte
	ready bool
}

func (b *replicaBody) Read(p []byte) (int, error) {
	if !b.ready {
		b.ready = true
		fmt.Fprintln(os.Stderr, "BKT_REPLICA_READY")
		var release [1]byte
		if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
			return 0, err
		}
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func replicaConfig(t *testing.T) *config.Config {
	t.Helper()
	settings, ok := pgtest.FromEnv()
	if !ok {
		t.Fatal("replica subprocess requires PostgreSQL")
	}
	cfg := &config.Config{Database: settings.DatabaseConfig(os.Getenv("BKT_REPLICA_DB"))}
	cfg.Storage.Backend = "local"
	cfg.Storage.RootPath = os.Getenv("BKT_REPLICA_ROOT")
	if err := database.Initialize(cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestIntegrationCrossReplicaConditionalPut(t *testing.T) {
	if os.Getenv("BKT_REPLICA_MODE") == "conditional-put" {
		cfg := replicaConfig(t)
		ownerID, err := uuid.Parse(os.Getenv("BKT_REPLICA_OWNER"))
		if err != nil {
			t.Fatal(err)
		}
		bucket := os.Getenv("BKT_REPLICA_BUCKET")
		body := &replicaBody{data: []byte(os.Getenv("BKT_REPLICA_BYTES"))}
		req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/race", body)
		req.ContentLength = int64(len(body.data))
		req.Header.Set("If-None-Match", "*")
		w := httptest.NewRecorder()
		itS3Router(cfg, models.User{ID: ownerID, IsAdmin: true}).ServeHTTP(w, req)
		fmt.Fprintf(os.Stderr, "BKT_REPLICA_STATUS %d\n", w.Code)
		return
	}

	cfg := itConfig(t)
	owner := mkUser(t, itName("replica"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	baseEnv := append(os.Environ(), "BKT_REPLICA_MODE=conditional-put",
		"BKT_REPLICA_DB="+cfg.Database.DBName, "BKT_REPLICA_ROOT="+cfg.Storage.RootPath,
		"BKT_REPLICA_OWNER="+owner.ID.String(), "BKT_REPLICA_BUCKET="+bucket.Name)
	type replica struct {
		cmd     *exec.Cmd
		stdin   io.WriteCloser
		readies chan struct{}
		status  chan int
	}
	start := func(data string) *replica {
		cmd := exec.Command(exe, "-test.run=^TestIntegrationCrossReplicaConditionalPut$", "-test.v")
		cmd.Env = append(baseEnv, "BKT_REPLICA_BYTES="+data)
		cmd.Stdout = io.Discard
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			t.Fatal(err)
		}
		readies := make(chan struct{}, 1)
		status := make(chan int, 1)
		go func() {
			buf := make([]byte, 2048)
			var pending string
			for {
				n, err := stderr.Read(buf)
				pending += string(buf[:n])
				for i := strings.IndexByte(pending, '\n'); i >= 0; i = strings.IndexByte(pending, '\n') {
					line := pending[:i]
					pending = pending[i+1:]
					if line == "BKT_REPLICA_READY" {
						readies <- struct{}{}
					} else if strings.HasPrefix(line, "BKT_REPLICA_STATUS ") {
						code, _ := strconv.Atoi(strings.TrimPrefix(line, "BKT_REPLICA_STATUS "))
						status <- code
					}
				}
				if err != nil {
					return
				}
			}
		}()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return &replica{cmd, stdin, readies, status}
	}
	waitReady := func(r *replica, timeout time.Duration) bool {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-r.readies:
			return true
		case <-timer.C:
			return false
		}
	}
	first := start("first")
	defer first.cmd.Process.Kill()
	if !waitReady(first, 15*time.Second) {
		t.Fatal("first replica never reached the guarded body")
	}
	second := start("second")
	defer second.cmd.Process.Kill()
	// A process-local lock lets the second worker pass the empty-key test and
	// reach its body before the first has written. An atomic DB compare-and-swap
	// could also be correct if it rejects the second writer at commit time.
	_ = waitReady(second, 2*time.Second)
	_, _ = first.stdin.Write([]byte{'x'})
	_ = first.stdin.Close()
	firstErr := first.cmd.Wait()
	_, _ = second.stdin.Write([]byte{'x'})
	_ = second.stdin.Close()
	secondErr := second.cmd.Wait()
	if firstErr != nil || secondErr != nil {
		t.Fatalf("replica processes: first=%v second=%v", firstErr, secondErr)
	}
	select {
	case code := <-first.status:
		if code != http.StatusOK {
			t.Errorf("first replica status %d, want 200", code)
		}
	default:
		t.Error("first replica did not report a status")
	}
	select {
	case code := <-second.status:
		if code != http.StatusPreconditionFailed {
			t.Errorf("second replica status %d, want 412", code)
		}
	default:
		t.Error("second replica did not report a status")
	}
	// The successful create must remain the only current bytes and metadata.
	router := itS3Router(cfg, owner)
	get := itDo(router, http.MethodGet, "/"+bucket.Name+"/race", nil)
	if get.Code != http.StatusOK || get.Body.String() != "first" {
		t.Errorf("current object after cross-replica race: %d %q", get.Code, get.Body.String())
	}
}

func TestIntegrationCrashBetweenBytesAndMetadata(t *testing.T) {
	if os.Getenv("BKT_REPLICA_MODE") == "crash-after-bytes" {
		cfg := replicaConfig(t)
		bucketName := os.Getenv("BKT_REPLICA_BUCKET")
		var bucket models.Bucket
		if err := database.DB.Where("name = ?", bucketName).First(&bucket).Error; err != nil {
			t.Fatal(err)
		}
		generationCommitBoundary = func(boundary string) {
			if boundary == "put" {
				os.Exit(73)
			}
		}
		itDo(itS3Router(cfg, models.User{ID: bucket.OwnerID, IsAdmin: true}), http.MethodPut, "/"+bucketName+"/crash", []byte("new-bytes"))
		t.Fatal("production PUT crash boundary was not reached")
	}
	cfg := itConfig(t)
	owner := mkUser(t, itName("crash"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	router := itS3Router(cfg, owner)
	s3PutString(t, router, bucket.Name, "crash", "old-bytes")
	before := itDo(router, http.MethodHead, "/"+bucket.Name+"/crash", nil)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestIntegrationCrashBetweenBytesAndMetadata$")
	cmd.Env = append(os.Environ(), "BKT_REPLICA_MODE=crash-after-bytes",
		"BKT_REPLICA_DB="+cfg.Database.DBName, "BKT_REPLICA_ROOT="+cfg.Storage.RootPath,
		"BKT_REPLICA_BUCKET="+bucket.Name)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err == nil || !strings.Contains(err.Error(), strconv.Itoa(73)) {
		t.Fatalf("crash subprocess should exit 73, got %v", err)
	}
	// Construct a fresh handler, as a replica taking over the crashed writer.
	afterRouter := itS3Router(cfg, owner)
	after := itDo(afterRouter, http.MethodGet, "/"+bucket.Name+"/crash", nil)
	if after.Code != http.StatusOK || after.Body.String() != "old-bytes" ||
		after.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Errorf("uncommitted bytes visible after writer crash: status=%d body=%q ETag=%q; committed ETag=%q",
			after.Code, after.Body.String(), after.Header().Get("ETag"), before.Header().Get("ETag"))
	}
}
