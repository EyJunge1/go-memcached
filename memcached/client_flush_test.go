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
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	_, err := client.Admin()
	isErrorIs(t, err, ErrAdminNotEnabled)

	adminOnly := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	isErrorIs(t, mustAdmin(t, adminOnly).FlushAll(ctx, FlushOpts{}), ErrFlushAllNotAllowed)
	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)

	allowed := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, allowed).FlushAll(ctx, FlushOpts{}))
	_, err = client.GetOne(ctx, []byte("k"))
	isErrorIs(t, err, ErrNotFound)
}

func TestFlushAllKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	noError(t, mustAdmin(t, client).FlushAll(ctx, FlushOpts{}))
	noError(t, storeOK(client.Set(ctx, []byte("k2"), []byte("v2"), 0, 0)))
	equal(t, int32(1), dials.Load())
}

func TestFlushAllAfterAndStatsArgs(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	noError(t, mustAdmin(t, client).FlushAll(ctx, FlushOpts{Delay: Rel(time.Second)}))
	_, err := client.GetOne(ctx, []byte("k"))
	isErrorIs(t, err, ErrNotFound)
	srv.mu.Lock()
	equal(t, "1", srv.lastFlushDelay)
	srv.mu.Unlock()

	got, err := client.Stats(ctx, "items")
	noError(t, err)
	equal(t, "items", got["arg"])

	_, err = client.Stats(ctx, "cachedump")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.Stats(ctx, "reset")
	isErrorIs(t, err, ErrAdminNotEnabled)
	_, err = client.Stats(ctx, "sizes")
	isErrorIs(t, err, ErrAdminNotEnabled)

	reset, err := mustAdminOn(t, client).Stats(ctx, "reset")
	noError(t, err)
	equal(t, 0, len(reset))

	noError(t, mustAdmin(t, client).FlushAll(ctx, FlushOpts{Stagger: time.Second}))
}

func TestFlushAllAfterWritesDelayToken(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{Delay: Rel(10 * time.Second)}))
	srv.mu.Lock()
	equal(t, "10", srv.lastFlushDelay)
	srv.mu.Unlock()
}

func TestFlushAllBroadcastsTwoNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
	})
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a-1"), []byte("1"), 0, 0)))
	b.mu.Lock()
	b.items["b-1"] = &fakeItem{value: []byte("2")}
	b.mu.Unlock()
	noError(t, mustAdmin(t, client).FlushAll(ctx, FlushOpts{}))
	a.mu.Lock()
	equal(t, 0, len(a.items))
	a.mu.Unlock()
	b.mu.Lock()
	equal(t, 0, len(b.items))
	b.mu.Unlock()
}

func TestFlushAllStaggerWritesPerNodeDelay(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
	})
	noError(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{Stagger: time.Second}))
	delays := map[string]struct{}{}
	a.mu.Lock()
	delays[a.lastFlushDelay] = struct{}{}
	a.mu.Unlock()
	b.mu.Lock()
	delays[b.lastFlushDelay] = struct{}{}
	b.mu.Unlock()
	if _, ok := delays[""]; !ok {
		t.Fatalf("node 0 must send flush_all without delay, got %v", delays)
	}
	if _, ok := delays["1"]; !ok {
		t.Fatalf("node 1 must send flush_all 1, got %v", delays)
	}
}
