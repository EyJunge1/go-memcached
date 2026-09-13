package memcached

import (
	"fmt"
	"time"
)

// Exptime is the memcached wire expiration integer.
// 0 never expires; negative values expire immediately; 1..2592000 are relative
// seconds; values above 2592000 are Unix timestamps.
type Exptime int64

const (
	// ExptimeNever means the item does not expire.
	ExptimeNever Exptime = 0
	// ExptimeImmediate expires the item at once (wire -1).
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
		return Exptime(nowFunc().Add(d).Unix())
	default:
		return Exptime(d / time.Second)
	}
}

// UnixExp returns t as a Unix-timestamp Exptime. Times at or before the
// 30-day relative window are rejected so the server cannot treat them as TTL.
func UnixExp(t time.Time) (Exptime, error) {
	u := t.Unix()
	if u <= maxRelativeExpirySecs {
		return 0, fmt.Errorf("unix expiry must be > %d: %w", maxRelativeExpirySecs, ErrInvalidArgument)
	}
	return Exptime(u), nil
}

// expiryToken is the sole wire encoder for Exptime.
func expiryToken(e Exptime) int64 {
	return int64(e)
}
