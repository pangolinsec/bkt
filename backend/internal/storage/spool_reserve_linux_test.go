//go:build linux

package storage

import (
	"io"
	"math"
	"testing"
)

type spoolReadProbe struct{ calls int }

func (p *spoolReadProbe) Read([]byte) (int, error) {
	p.calls++
	return 0, io.EOF
}

func TestS3GenerationSpoolRejectsUnreservablePayloadBeforeReading(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	probe := &spoolReadProbe{}
	f, _, err := spoolS3Payload(probe, math.MaxInt64, "bkt-s3-capacity-test-*")
	if f != nil {
		_ = f.Close()
	}
	if err == nil || probe.calls != 0 {
		t.Fatalf("unreservable payload consumed client body: file=%v err=%v reads=%d", f, err, probe.calls)
	}
}
