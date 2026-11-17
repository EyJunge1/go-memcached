package memcached

import "testing"

func TestGetSizedBytesPoolClasses(t *testing.T) {
	cases := []struct {
		need int
		min  int
	}{
		{-1, 64},
		{0, 64},
		{1, 64},
		{64, 64},
		{65, 128},
		{1024, 1024},
		{65536, 65536},
		{1 << 20, 1 << 20},
	}
	for _, tc := range cases {
		buf := getSizedBytes(tc.need)
		if buf == nil {
			t.Fatalf("need=%d: nil buffer", tc.need)
		}
		if len(buf) != 0 {
			t.Fatalf("need=%d: len=%d want 0", tc.need, len(buf))
		}
		if cap(buf) < tc.min {
			t.Fatalf("need=%d: cap=%d want >= %d", tc.need, cap(buf), tc.min)
		}
		releaseBytes(buf)
	}
}

func TestGetSizedBytesAboveMaxAllocates(t *testing.T) {
	need := (1 << 20) + 1
	buf := getSizedBytes(need)
	if cap(buf) < need {
		t.Fatalf("cap=%d want >= %d", cap(buf), need)
	}
	// Non-pool capacity must be ignored by releaseBytes.
	releaseBytes(buf)
}

func TestReleaseBytesIgnoresOddCaps(t *testing.T) {
	releaseBytes(nil)
	odd := make([]byte, 0, 100)
	releaseBytes(odd) // not a pool class
	releaseBytes(make([]byte, 0, 63))
}

func TestSizedPoolIndex(t *testing.T) {
	if got := sizedPoolIndex(64); got != 0 {
		t.Fatalf("64 -> %d", got)
	}
	if got := sizedPoolIndex(65); got != 1 {
		t.Fatalf("65 -> %d", got)
	}
	if got := sizedPoolIndex((1 << 20) + 1); got != -1 {
		t.Fatalf("oversize -> %d", got)
	}
}
