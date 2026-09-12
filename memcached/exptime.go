package memcached

import (
	"fmt"
	"math"
	"time"
)

// Exptime is the item expiry token on the wire.
// 0 is no expiry, 1–2592000 are relative seconds, larger values are Unix time.
// Negative values send as-is (protocol.txt: a negative value expires immediately).
type Exptime int64

const (
	// ExptimeNever leaves the item without an expiry.
	ExptimeNever Exptime = 0
	// ExptimeImmediate expires the item at once. The client sends -1.
	ExptimeImmediate Exptime = -1

	_maxRelativeExpirySecs int64 = 2592000

	_maxRelativeExpiry = 30 * 24 * time.Hour
)

// Rel converts a duration to Exptime under the protocol 30-day split.
func Rel(d time.Duration) Exptime {
	return relAt(d, time.Now())
}

// UnixExp encodes t as a Unix-timestamp Exptime. Times inside the 30-day
// relative window are rejected so the server cannot read them as a TTL.
// Values above math.MaxInt32 are rejected because wire Unix expiry is 32-bit.
func UnixExp(t time.Time) (Exptime, error) {
	u := t.Unix()
	if u <= _maxRelativeExpirySecs {
		return 0, fmt.Errorf("unix expiry must be > %d: %w", _maxRelativeExpirySecs, ErrInvalidArgument)
	}
	if u > math.MaxInt32 {
		return 0, fmt.Errorf("unix expiry exceeds 32-bit: %w", ErrInvalidArgument)
	}
	return Exptime(u), nil
}

// relAt is Rel with an explicit now (tests inject a fixed time).
func relAt(d time.Duration, now time.Time) Exptime {
	switch {
	case d == 0:
		return ExptimeNever
	case d < 0:
		return ExptimeImmediate
	case d < time.Second:
		return 1
	case d > _maxRelativeExpiry:
		return Exptime(now.Add(d).Unix())
	default:
		return Exptime(d / time.Second)
	}
}

// expiryToken returns the integer sent on the wire for this Exptime.
func expiryToken(e Exptime) int64 {
	return int64(e)
}

// recacheToken turns a remaining-TTL window into relative seconds.
// This is not an item Exptime and must not use Rel: a window longer than
// 30 days stays relative (mg R compares remaining TTL). Zero and negative
// omit the flag (ok=false). Sub-second windows become 1.
func recacheToken(d time.Duration) (int64, bool) {
	if d <= 0 {
		return 0, false
	}
	secs := max(int64(d/time.Second), 1)
	return secs, true
}
