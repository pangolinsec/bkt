//go:build windows

package storage

import "os"

// Windows cannot unlink a file held open by the signer. The caller removes
// its named scratch file after the SDK finishes and closes the descriptor.
func detachS3Spool(_ *os.File) error { return nil }
