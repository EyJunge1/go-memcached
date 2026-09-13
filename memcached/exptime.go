package memcached

import (
	"fmt"
	"math"
	"time"
)

// Exptime is the memcached wire expiration integer.
// 0 never expires; 1..2592000 are relative seconds; values above 2592000 are
// Unix timestamps (32-bit on the server). Negative values are a client
// convention (wire -1) that expire immediately; they are not in protocol.txt.
// flush_all delay 0 omits the token (immediate) and is not the same as item 0.
type Exptime int64

const (
	// ExptimeNever means the item does not expire.
	ExptimeNever Exptime = 0
	// ExptimeImmediate expires the item at once. This is a client convention
	// that sends wire -1; protocol.txt does not define a negative token.
	ExptimeImmediate Exptime = -1

	maxRelativeExpirySecs int64 = 2592000
)

var maxRelativeExpiry = 30 * 24 * time.Hour

// Rel maps a duration onto Exptime using the protocol 30-day rule.
func Rel(d time.Duration) Exptime {
	switch {
	case d == 0:
		return ExptimeNever
	case d < 0:
		return ExptimeImmediate
	case d < time.Second:
		return 1
	case d > maxRelativeExpiry:
		return Exptime(clock().Add(d).Unix())
	default:
		return Exptime(d / time.Second)
	}
}

// UnixExp returns t as a Unix-timestamp Exptime. Times at or before the
// 30-day relative window are rejected so the server cannot treat them as TTL.
// Values above math.MaxInt32 are rejected (protocol Unix expiry is 32-bit).
func UnixExp(t time.Time) (Exptime, error) {
	u := t.Unix()
	if u <= maxRelativeExpirySecs {
		return 0, fmt.Errorf("unix expiry must be > %d: %w", maxRelativeExpirySecs, ErrInvalidArgument)
	}
	if u > math.MaxInt32 {
		return 0, fmt.Errorf("unix expiry exceeds 32-bit: %w", ErrInvalidArgument)
	}
	return Exptime(u), nil
}

// expiryToken is the sole wire encoder for Exptime.
func expiryToken(e Exptime) int64 {
	return int64(e)
}

// recacheToken encodes a remaining-TTL window as relative seconds.
// This is not an item Exptime and must not go through Rel: a window above
// 30 days stays relative seconds (mg R compares remaining TTL). Zero and
// negative omit the flag (ok=false). Sub-second windows encode as 1.
func recacheToken(d time.Duration) (int64, bool) {
	if d <= 0 {
		return 0, false
	}
	secs := int64(d / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs, true
}
