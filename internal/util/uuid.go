// Package util holds small, dependency-free helpers shared across the
// backend.
package util

import (
	"crypto/rand"
	"fmt"
)

// NewUUID generates a random RFC 4122 version 4 UUID, e.g.
// "550e8400-e29b-41d4-a716-446655440000". Used for public-facing IDs
// (like WaDevice.ID) that intentionally must not be guessable or
// enumerable — sequential auto-increment IDs would let one user probe
// other users' device IDs. Implemented by hand with crypto/rand instead
// of pulling in a UUID library, since this is the only place one is
// needed.
func NewUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the OS can't provide randomness at
		// all, which is unrecoverable for anything security-sensitive —
		// panicking here matches how the standard library treats this
		// same failure mode (e.g. crypto/rand.Read documents that
		// callers should treat an error as fatal).
		panic("util: failed to read random bytes for UUID: " + err.Error())
	}

	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
