//go:build !linux

package storage

import "os"

// Other platforms retain the seekable-spool path and rely on ordinary write
// errors when the scratch filesystem is full.
func reserveS3Spool(_ *os.File, _ int64) error { return nil }
