//go:build !windows

package storage

import "os"

func detachS3Spool(f *os.File) error { return os.Remove(f.Name()) }
