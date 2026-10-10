package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bkt/internal/database"
	"bkt/internal/models"
)

func holdBucketAdvisoryShared(t *testing.T, name string) func() {
	t.Helper()
	conn, err := database.AdvisoryDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := advisoryBucketKey(name)
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock_shared($1)", key); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return func() {
		if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock_shared($1)", key); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
}

func assertBlockedUntilRelease(t *testing.T, done <-chan *httptest.ResponseRecorder, release func(), want int) {
	t.Helper()
	select {
	case w := <-done:
		release()
		t.Fatalf("bucket config mutation bypassed an in-flight writer: %d %s", w.Code, w.Body.String())
	case <-time.After(150 * time.Millisecond):
	}
	release()
	select {
	case w := <-done:
		if w.Code != want {
			t.Errorf("status %d, want %d: %s", w.Code, want, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bucket config mutation did not complete")
	}
}

func TestIntegrationSettingsWaitForObjectWriter(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("settings-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	release := holdBucketAdvisoryShared(t, bucket.Name)
	router := itRouter(owner)
	router.PUT("/:name/settings", NewBucketHandler(cfg).SetBucketSettings)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- itDo(router, http.MethodPut, "/"+bucket.Name+"/settings", []byte(`{"retention_days":30}`))
	}()
	assertBlockedUntilRelease(t, done, release, http.StatusOK)
}

func TestIntegrationMultipartInitiationWaitsForBucketDeletionLock(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("multipart-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	release, err := lockBucketExclusive(context.Background(), &bucket)
	if err != nil {
		t.Fatal(err)
	}
	s3 := itS3Router(cfg, owner)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- itDo(s3, http.MethodPost, "/"+bucket.Name+"/pending?uploads", nil)
	}()
	select {
	case w := <-done:
		release()
		t.Fatalf("multipart initiation bypassed bucket deletion lock: %d %s", w.Code, w.Body.String())
	case <-time.After(150 * time.Millisecond):
	}
	var uploads int64
	if err := database.DB.Model(&models.MultipartUpload{}).Where("bucket_name = ?", bucket.Name).Count(&uploads).Error; err != nil {
		release()
		t.Fatal(err)
	}
	if uploads != 0 {
		release()
		t.Fatalf("multipart tracking row appeared under exclusive bucket lock: %d", uploads)
	}
	release()
	select {
	case w := <-done:
		if w.Code != http.StatusOK {
			t.Errorf("multipart initiation after lock release: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("multipart initiation did not finish after lock release")
	}
}

func TestIntegrationRESTVersioningWaitsForObjectWriter(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("rest-vers-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	release := holdBucketAdvisoryShared(t, bucket.Name)
	router := itRouter(owner)
	router.PUT("/:name/versioning", NewBucketHandler(cfg).SetBucketVersioning)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- itDo(router, http.MethodPut, "/"+bucket.Name+"/versioning", []byte(`{"versioning":"enabled"}`))
	}()
	assertBlockedUntilRelease(t, done, release, http.StatusOK)
}

func TestIntegrationS3VersioningWaitsForObjectWriter(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("s3-vers-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	release := holdBucketAdvisoryShared(t, bucket.Name)
	router := itRouter(owner)
	router.PUT("/:bucket", NewS3APIHandler(cfg).PutBucketVersioning)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- itDo(router, http.MethodPut, "/"+bucket.Name+"?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	}()
	assertBlockedUntilRelease(t, done, release, http.StatusOK)
}

func TestIntegrationDeleteBucketReloadsRetentionAfterLockWait(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("delete-retention"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	s3PutString(t, itS3Router(cfg, owner), bucket.Name, "held", "retained bytes")
	release := holdBucketAdvisoryShared(t, bucket.Name)
	router := itRouter(owner)
	router.DELETE("/:name", NewBucketHandler(cfg).DeleteBucket)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- itDo(router, http.MethodDelete, "/"+bucket.Name, nil) }()

	// Observe the DELETE blocked on its advisory lock. It has already loaded
	// the initial bucket row at this point, so the new retention value tests
	// whether that stale row is reloaded before any destructive operation.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		err := database.DB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'SELECT pg_advisory_lock(%'`).Scan(&waiting).Error
		if err != nil {
			release()
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case w := <-done:
			release()
			t.Fatalf("DELETE completed before bucket lock released: %d %s", w.Code, w.Body.String())
		default:
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("DELETE never waited on bucket advisory lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Update("retention_days", 30).Error; err != nil {
		release()
		t.Fatal(err)
	}
	release()
	select {
	case w := <-done:
		if w.Code != http.StatusConflict {
			t.Errorf("DELETE after retention raised: %d %s, want 409", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not complete after lock released")
	}
	if status, body := s3GetString(t, itS3Router(cfg, owner), bucket.Name, "held"); status != http.StatusOK || body != "retained bytes" {
		t.Errorf("retained object: status=%d body=%q", status, body)
	}
}

func TestIntegrationDeleteObjectReloadsRetentionAfterSharedLockWait(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("object-retention"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, func(b *models.Bucket) { b.Versioning = models.VersioningEnabled })
	router := itS3Router(cfg, owner)
	router.DELETE("/:bucket/*key", NewS3APIHandler(cfg).DeleteObject)
	s3PutString(t, router, bucket.Name, "held", "retained bytes")
	var current models.Object
	if err := database.DB.Where("bucket_id = ? AND key = ?", bucket.ID, "held").First(&current).Error; err != nil {
		t.Fatal(err)
	}
	if current.VersionID == "" {
		t.Fatal("expected a versioned current object")
	}
	conn, err := database.AdvisoryDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := advisoryBucketKey(bucket.Name)
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", key); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	release := func() {
		if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- itDo(router, http.MethodDelete, "/"+bucket.Name+"/held?versionId="+current.VersionID, nil)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		err := database.DB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'SELECT pg_advisory_lock_shared(%'`).Scan(&waiting).Error
		if err != nil {
			release()
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case w := <-done:
			release()
			t.Fatalf("DELETE completed before shared bucket lock: %d %s", w.Code, w.Body.String())
		default:
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("DELETE never waited for shared bucket lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", bucket.ID).Update("retention_days", 30).Error; err != nil {
		release()
		t.Fatal(err)
	}
	release()
	select {
	case w := <-done:
		if w.Code != http.StatusForbidden {
			t.Errorf("DELETE after retention raised: %d %s, want 403", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not complete after lock released")
	}
	if status, body := s3GetString(t, router, bucket.Name, "held"); status != http.StatusOK || body != "retained bytes" {
		t.Errorf("retained object: status=%d body=%q", status, body)
	}
}

func TestIntegrationCrossBucketLockRefreshesBothBuckets(t *testing.T) {
	itConfig(t)
	owner := mkUser(t, itName("cross-refresh"), "password-123", true, false)
	source := itBucket(t, owner.ID, nil)
	target := itBucket(t, owner.ID, nil)
	conn, err := database.AdvisoryDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := advisoryBucketKey(target.Name)
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", key); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	release := func() {
		if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		unlock, err := lockKeysAcrossBucketsChecked(context.Background(), &source, "src", &target, "dst")
		if err == nil {
			unlock()
		}
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		err := database.DB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'SELECT pg_advisory_lock_shared(%'`).Scan(&waiting).Error
		if err != nil {
			release()
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case err := <-done:
			release()
			t.Fatalf("cross-bucket lock completed before release: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("cross-bucket writer did not wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := database.DB.Model(&models.Bucket{}).Where("id = ?", target.ID).Update("quota_bytes", 1234).Error; err != nil {
		release()
		t.Fatal(err)
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cross-bucket writer did not complete")
	}
	if target.QuotaBytes != 1234 {
		t.Errorf("target config after lock wait = %d, want 1234", target.QuotaBytes)
	}
}

type gateBody struct {
	data    []byte
	entered chan struct{}
	release chan struct{}
	opened  bool
}

func (b *gateBody) Read(p []byte) (int, error) {
	if !b.opened {
		b.opened = true
		close(b.entered)
		<-b.release
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func waitGate(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach body read")
	}
}

func TestIntegrationBucketDeleteWaitsForInflightPut(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("bucket-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	s3 := itS3Router(cfg, owner)
	body := &gateBody{data: []byte("committed"), entered: make(chan struct{}), release: make(chan struct{})}
	putReq := httptest.NewRequest(http.MethodPut, "/"+bucket.Name+"/raced", body)
	putReq.ContentLength = int64(len(body.data))
	putDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		s3.ServeHTTP(w, putReq)
		putDone <- w
	}()
	waitGate(t, body.entered)

	console := itRouter(owner)
	console.DELETE("/:name", NewBucketHandler(cfg).DeleteBucket)
	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { deleteDone <- itDo(console, http.MethodDelete, "/"+bucket.Name, nil) }()
	select {
	case response := <-deleteDone:
		close(body.release)
		t.Fatalf("bucket deletion completed while object upload was in flight: %d %s", response.Code, response.Body.String())
	case <-time.After(150 * time.Millisecond):
	}
	close(body.release)
	select {
	case put := <-putDone:
		if put.Code != http.StatusOK {
			t.Errorf("PUT status %d: %s", put.Code, put.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not complete")
	}
	select {
	case del := <-deleteDone:
		if del.Code != http.StatusOK {
			t.Errorf("DELETE bucket status %d: %s", del.Code, del.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bucket deletion did not complete")
	}
	var n int64
	if err := database.DB.Model(&models.Object{}).Where("bucket_id = ?", bucket.ID).Count(&n).Error; err != nil || n != 0 {
		t.Errorf("object rows after bucket deletion: count=%d err=%v", n, err)
	}
}

func TestIntegrationTaggingRechecksDeletedCurrentObject(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("tag-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	h := NewS3APIHandler(cfg)
	router := itRouter(owner)
	router.PUT("/:bucket/*key", h.PutObject)
	router.DELETE("/:bucket/*key", h.DeleteObject)
	s3PutString(t, router, bucket.Name, "tagged", "old")

	xml := []byte(`<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`)
	body := &gateBody{data: xml, entered: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest(http.MethodPut, "/"+bucket.Name+"/tagged?tagging", body)
	req.ContentLength = int64(len(xml))
	tagDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		tagDone <- w
	}()
	waitGate(t, body.entered)
	del := itDo(router, http.MethodDelete, "/"+bucket.Name+"/tagged", nil)
	if del.Code != http.StatusNoContent {
		t.Fatalf("DELETE current object: %d %s", del.Code, del.Body.String())
	}
	close(body.release)
	select {
	case tag := <-tagDone:
		if tag.Code != http.StatusNotFound {
			t.Errorf("tagging deleted current object returned %d, want 404", tag.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tagging request did not complete")
	}
	var n int64
	if err := database.DB.Model(&models.Object{}).Where("bucket_id = ? AND key = ?", bucket.ID, "tagged").Count(&n).Error; err != nil || n != 0 {
		t.Errorf("deleted object row: count=%d err=%v", n, err)
	}
}

func TestIntegrationCancelledObjectLockWait(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("cancel-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	hold := lockObjectKeys(bucket.Name, "wait")
	defer hold()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPut, "/"+bucket.Name+"/wait", bytes.NewBufferString("x")).WithContext(ctx)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		itS3Router(cfg, owner).ServeHTTP(w, req)
		done <- w
	}()
	lockID := bucket.Name + "\x00wait"
	deadline := time.Now().Add(5 * time.Second)
	for {
		objectKeyLocks.Lock()
		e := objectKeyLocks.m[lockID]
		waiting := e != nil && e.refs >= 2
		objectKeyLocks.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PUT did not wait on the held object lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		hold()
		<-done
		t.Fatal("canceled foreground PUT kept waiting on a key lock")
	}
	if n := objectLockEntries(); n != 1 {
		t.Errorf("canceled waiter leaked local lock entries: %d", n)
	}
}

func TestIntegrationCancelledPostgresAdvisoryWait(t *testing.T) {
	itConfig(t)
	owner := mkUser(t, itName("pg-cancel"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	conn, err := database.AdvisoryDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := advisoryKey(bucket.Name + "\x00wait")
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", key); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = lockObjectKeysChecked(ctx, &bucket, "wait")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("advisory wait error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("canceled advisory wait took %v", elapsed)
	}
	if n := objectLockEntries(); n != 0 {
		t.Errorf("canceled advisory wait leaked %d local lock entries", n)
	}
}

func TestIntegrationDeleteTaggingWaitsForObjectWriter(t *testing.T) {
	cfg := itConfig(t)
	owner := mkUser(t, itName("untag-lock"), "password-123", true, false)
	bucket := itBucket(t, owner.ID, nil)
	h := NewS3APIHandler(cfg)
	router := itRouter(owner)
	router.PUT("/:bucket/*key", h.PutObject)
	router.DELETE("/:bucket/*key", h.DeleteObject)
	s3PutString(t, router, bucket.Name, "tagged", "old")
	hold := lockObjectKeys(bucket.Name, "tagged")
	defer hold()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- itDo(router, http.MethodDelete, "/"+bucket.Name+"/tagged?tagging", nil) }()
	select {
	case response := <-done:
		hold()
		t.Fatalf("DELETE tagging bypassed the object writer: %d", response.Code)
	case <-time.After(100 * time.Millisecond):
	}
	hold()
	select {
	case response := <-done:
		if response.Code != http.StatusNoContent {
			t.Errorf("DELETE tagging status %d: %s", response.Code, response.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE tagging did not complete after writer released lock")
	}
}
