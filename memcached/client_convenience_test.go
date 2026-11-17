package memcached

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestErrorAliases(t *testing.T) {
	isTrue(t, errors.Is(ErrCASConflict, ErrExists))
	isTrue(t, errors.Is(ErrExists, ErrCASConflict))
	isTrue(t, errors.Is(ErrMalformedKey, ErrInvalidKey))
	isTrue(t, errors.Is(ErrCacheMiss, ErrNotFound))
}

func TestNewFromAddrsRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	c, err := NewFromAddrs(srv.Addr())
	c = requireClient(t, c, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	noError(t, c.Store(ctx, Item{Key: []byte("k"), Value: []byte("v"), Flags: 3}))
	var got GetResult
	exp := Rel(0)
	noError(t, c.GetOne(ctx, []byte("k"), &got, GetOpts{Touch: &exp}))
	isTrue(t, got.Found)
	equal(t, []byte("v"), got.Value)
	equal(t, uint32(3), got.Flags)
	noError(t, expectStore(ErrCASConflict)(c.Store(ctx, Item{
		Mode: StoreCAS, Key: []byte("k"), Value: []byte("x"), CAS: 99,
	})))
}

func TestNewFromAddrsEmpty(t *testing.T) {
	_, err := NewFromAddrs()
	isErrorIs(t, err, ErrInvalidAddress)
	_, err = NewFromAddrs(" ")
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestNewProductionFromAddrsAppliesDefaults(t *testing.T) {
	srv := startFakeMemcached(t)
	c, err := NewProductionFromAddrs(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, DefaultFailover(), c.rt.failover)
	equal(t, 30*time.Minute, c.rt.maxLifetime)
	equal(t, 5*time.Minute, c.rt.maxIdleTime)
	equal(t, 30*time.Second, c.rt.idleHealthPing)
	equal(t, 1, len(c.LiveServers()))
}

func TestDiscoveryBackoff(t *testing.T) {
	d := discoveryBackoff(time.Second, 1)
	if d <= 0 || d > time.Second {
		t.Fatalf("fail1=%v", d)
	}
	d4 := discoveryBackoff(time.Second, 4)
	if d4 < 4*time.Second || d4 > 8*time.Second {
		t.Fatalf("fail4=%v", d4)
	}
	d20 := discoveryBackoff(time.Second, 20)
	if d20 > 5*time.Minute {
		t.Fatalf("capped=%v", d20)
	}
}
