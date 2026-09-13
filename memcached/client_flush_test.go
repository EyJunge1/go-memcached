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
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	_, err := client.Admin()
	isErrorIs(t, err, ErrAdminNotEnabled)

	adminOnly := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	isErrorIs(t, mustAdmin(t, adminOnly).FlushAll(ctx, FlushOpts{}), ErrAdminTargetRequired)
	isErrorIs(t, mustAdmin(t, adminOnly).All().FlushAll(ctx, FlushOpts{}), ErrFlushAllNotAllowed)
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, []byte("v"), item.Value)

	allowed := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, allowed).All().FlushAll(ctx, FlushOpts{}))
	items, err := client.Get(ctx, GetOpts{}, []byte("k"))
	noError(t, err)
	hasLen(t, items, 0)
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
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	noError(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("v2")}))
	equal(t, int32(1), dials.Load())
}

func TestFlushAllAfterAndStatsArgs(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	noError(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{Delay: Rel(time.Second)}))
	items, err := client.Get(ctx, GetOpts{}, []byte("k"))
	noError(t, err)
	hasLen(t, items, 0)
	srv.mu.Lock()
	equal(t, "1", srv.lastFlushDelay)
	srv.mu.Unlock()

	all, err := client.StatsAll(ctx, "items")
	noError(t, err)
	hasLen(t, all, 1)
	for _, got := range all {
		equal(t, "items", got["arg"])
	}

	_, err = client.StatsAll(ctx, "cachedump")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.StatsAll(ctx, "detail", "on")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.StatsAll(ctx, "reset")
	isErrorIs(t, err, ErrAdminNotEnabled)
	_, err = client.StatsAll(ctx, "sizes")
	isErrorIs(t, err, ErrAdminNotEnabled)

	reset, err := mustAdminOn(t, client).Stats(ctx, "reset")
	noError(t, err)
	equal(t, 0, len(reset))

	noError(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{Stagger: time.Second}))
}

func TestFlushAllAfterWritesDelayToken(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().FlushAll(context.Background(), FlushOpts{Delay: Rel(10 * time.Second)}))
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
	})
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a-1"), Value: []byte("1")}))
	b.mu.Lock()
	b.items["b-1"] = &fakeItem{value: []byte("2")}
	b.mu.Unlock()
	noError(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{}))
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
	})
	noError(t, mustAdmin(t, client).All().FlushAll(context.Background(), FlushOpts{Stagger: time.Second}))
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

func TestFlushAllOnHitsOneNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	ctx := context.Background()
	a.mu.Lock()
	a.items["keep-a"] = &fakeItem{value: []byte("1")}
	a.mu.Unlock()
	b.mu.Lock()
	b.items["keep-b"] = &fakeItem{value: []byte("2")}
	b.mu.Unlock()

	sv := client.Servers()
	noError(t, mustAdmin(t, client).On(sv[0]).FlushAll(ctx, FlushOpts{}))

	empties := 0
	a.mu.Lock()
	if len(a.items) == 0 {
		empties++
	}
	a.mu.Unlock()
	b.mu.Lock()
	if len(b.items) == 0 {
		empties++
	}
	b.mu.Unlock()
	equal(t, 1, empties)

	noError(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{Delay: Rel(time.Second)}))
}

func TestFlushAllDelayRejectsStagger(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	ctx := context.Background()
	isErrorIs(t, mustAdmin(t, client).All().FlushAll(ctx, FlushOpts{Delay: Rel(time.Second), Stagger: time.Second}), ErrInvalidArgument)
	isErrorIs(t, mustAdmin(t, client).On(client.Servers()[0]).FlushAll(ctx, FlushOpts{Delay: ExptimeImmediate, Stagger: time.Second}), ErrInvalidArgument)
}

func TestFlushAllExpImmediate(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().FlushAll(context.Background(), FlushOpts{Delay: ExptimeImmediate}))
	srv.mu.Lock()
	equal(t, "-1", srv.lastFlushDelay)
	srv.mu.Unlock()
}
