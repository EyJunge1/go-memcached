package memcached

import (
	"context"
	"testing"
)

func TestPoolIdleLIFO(t *testing.T) {
	pool := newConnPool(4, 4, 0, 0, createConn)
	t.Cleanup(func() { _ = pool.closePool() })

	ctx := context.Background()
	a, err := pool.get(ctx)
	noError(t, err)
	b, err := pool.get(ctx)
	noError(t, err)
	noError(t, pool.put(a))
	noError(t, pool.put(b)) // b is hottest (LIFO)

	c, err := pool.get(ctx)
	noError(t, err)
	same(t, b, c)
	d, err := pool.get(ctx)
	noError(t, err)
	same(t, a, d)
	_ = pool.put(c)
	_ = pool.put(d)

	s := pool.stats()
	if s.IdleHits < 2 {
		t.Fatalf("IdleHits=%d", s.IdleHits)
	}
	if s.IdleMisses < 2 {
		t.Fatalf("IdleMisses=%d", s.IdleMisses)
	}
}

func TestGetSizedBytesRoundTrip(t *testing.T) {
	buf := getSizedBytes(100)
	if cap(buf) < 100 {
		t.Fatalf("cap=%d", cap(buf))
	}
	buf = append(buf, make([]byte, 50)...)
	releaseBytes(buf)
}
