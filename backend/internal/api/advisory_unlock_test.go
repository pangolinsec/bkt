package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

type stalledUnlockDriver struct{ release <-chan struct{} }

func (d stalledUnlockDriver) Open(string) (driver.Conn, error) {
	return stalledUnlockConn{release: d.release}, nil
}

type stalledUnlockConn struct{ release <-chan struct{} }

func (c stalledUnlockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c stalledUnlockConn) Close() error                        { return nil }
func (c stalledUnlockConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c stalledUnlockConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, errors.New("test released stalled unlock")
	}
}

func TestAdvisoryUnlockHasBoundedCleanup(t *testing.T) {
	release := make(chan struct{})
	name := "stalled-advisory-unlock"
	sql.Register(name, stalledUnlockDriver{release: release})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		unlockAdvisory(conn, []advisoryHold{{key: 42}})
		close(done)
	}()
	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
			t.Errorf("advisory cleanup took %v", elapsed)
		}
	case <-time.After(3 * time.Second):
		close(release)
		<-done
		t.Fatal("advisory cleanup ignored its deadline")
	}
}
