package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlushAllRequiresOptIn(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.FlushAll(ctx), ErrFlushAllNotAllowed)
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)

	allowed, err := New(srv.Addr(), WithAllowFlushAll())
	noError(t, err)
	t.Cleanup(func() { _ = allowed.Close() })
	noError(t, allowed.FlushAll(ctx))
	_, err = client.Get(ctx, "k")
	isErrorIs(t, err, ErrNotFound)
}

func TestFlushAllWithNoReplyStillReadsOK(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithAllowFlushAll(), WithNoReply(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	noError(t, client.FlushAll(ctx))
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestFlushAllAfterAndStatsArgs(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAllowFlushAll())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	noError(t, client.FlushAllAfter(ctx, time.Second))
	_, err = client.Get(ctx, "k")
	isErrorIs(t, err, ErrNotFound)

	got, err := client.StatsArgs(ctx, "items")
	noError(t, err)
	equal(t, "items", got["arg"])

	_, err = client.StatsArgs(ctx, "flush_all")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.StatsArgs(ctx, "cachedump")
	isErrorIs(t, err, ErrInvalidArgument)

	reset, err := client.StatsArgs(ctx, "reset")
	noError(t, err)
	equal(t, 0, len(reset))

	noError(t, client.FlushAllStaggered(ctx, time.Second))
}

func TestFlushAllBroadcastsTwoNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithAllowFlushAll(), WithPickBuilder(NewCRC32HashPickBuilder()))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	noError(t, client.Set(ctx, "a-1", []byte("1"), 0, 0))
	// Second node via prefix picker is unused unless we also set a b- key
	// through a client that can reach it; seed both maps directly.
	b.mu.Lock()
	b.items["b-1"] = &fakeItem{value: []byte("2")}
	b.mu.Unlock()
	noError(t, client.FlushAll(ctx))
	a.mu.Lock()
	equal(t, 0, len(a.items))
	a.mu.Unlock()
	b.mu.Lock()
	equal(t, 0, len(b.items))
	b.mu.Unlock()
}
