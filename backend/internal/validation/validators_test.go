package validation

import (
	"strings"
	"testing"
)

func TestValidateObjectKeyAcceptsS3Keys(t *testing.T) {
	good := []string{
		"a",
		"file.txt",
		"photos/2024/a.jpg",
		"folder/",           // S3 folder-marker object
		"folder/.keep",      // console folder placeholder
		".hidden",           // dot-files are fine
		"a/.b/c",            // dot-prefixed segments are fine
		"a/b.c/d",           // dots inside segments are fine
		".bkt-versions",     // only the prefix WITH the slash is reserved
		".bkt-versionsX/a",  // not the reserved prefix
		"x/.bkt-versions/y", // reserved only at the start of the key
		"name with spaces+plus%percent?q",
		// Keys are opaque strings: none of these is special to S3 (nor to
		// any bkt backend — the local backend hashes keys).
		"a..b.txt", "..", "../x", "a/../b", "../../etc/passwd",
		"/abs", "//", "/leading", "a//b", "./x", "a/./b", ".", "a/.", "a//",
		"a\\b", "\\", "C:\\Windows\\x",
		"dir/.bkt-folder", ".bkt-folder", // formerly reserved by the local backend
		strings.Repeat("s", 300), // a single 300-byte "segment"
		"ünï/😀",
		strings.Repeat("k", MaxObjectKeyBytes),
		"tab\tand\nnewline",
	}
	for _, k := range good {
		if err := ValidateObjectKey(k); err != nil {
			t.Errorf("ValidateObjectKey(%q) = %v, want nil", k, err)
		}
	}
}

func TestValidateObjectKeyRejects(t *testing.T) {
	bad := []string{
		"",
		"a\x00b",
		"\x00",
		strings.Repeat("k", MaxObjectKeyBytes+1),
		strings.Repeat("ü", MaxObjectKeyBytes/2) + "x", // 1025 bytes, 513 runes
		"bad\xffutf8",
		"\xc3", // truncated multi-byte sequence
	}
	for _, k := range bad {
		if err := ValidateObjectKey(k); err == nil {
			t.Errorf("ValidateObjectKey(%q) = nil, want error", k)
		}
	}
}

func TestValidateObjectKeyRejectsReservedVersionKeyspace(t *testing.T) {
	for _, k := range []string{
		".bkt-versions/",
		".bkt-versions/a",
		".bkt-versions/a/0b0c3b51-0000-4000-8000-000000000000",
		".bkt-generations/",
		".bkt-generations/aa/0b0c3b51-0000-4000-8000-000000000000",
	} {
		if err := ValidateObjectKey(k); err == nil {
			t.Errorf("ValidateObjectKey(%q) = nil, want reserved-key error", k)
		}
		if !IsReservedObjectKey(k) {
			t.Errorf("IsReservedObjectKey(%q) = false, want true", k)
		}
	}
	if IsReservedObjectKey("docs/.bkt-versions/a") {
		t.Error("reserved prefix must only match at the start of the key")
	}
}

func TestEscapeLikeWildcards(t *testing.T) {
	cases := map[string]string{
		"a_b":   `a\_b`,
		"50%":   `50\%`,
		`a\b`:   `a\\b`,
		"plain": "plain",
	}
	for in, want := range cases {
		if got := EscapeLikeWildcards(in); got != want {
			t.Errorf("EscapeLikeWildcards(%q) = %q, want %q", in, got, want)
		}
	}
}
