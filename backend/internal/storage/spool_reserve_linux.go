//go:build linux

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// Reserve the declared payload before reading it from a client. Linux
// fallocate accounts for concurrent processes on the same filesystem and
// returns ENOSPC before we consume a valid multi-gigabyte request body.
func reserveS3Spool(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size)
}
