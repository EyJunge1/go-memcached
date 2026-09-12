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
	isErrorIs(t, mustAdmin(t, adminOnly).All().Flush(ctx, FlushOpts{}), ErrFlushAllNotAllowed)
	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, []byte("v"), item.Value)

	allowed := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, allowed).All().Flush(ctx, FlushOpts{}))
	items, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
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
	noError(t, mustAdmin(t, client).All().Flush(ctx, FlushOpts{}))
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
	noError(t, mustAdmin(t, client).All().Flush(ctx, FlushOpts{Delay: new(Rel(time.Second))}))
	items, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
	srv.mu.Lock()
	equal(t, "1", srv.lastFlushDelay)
	srv.mu.Unlock()

	all, err := doStatsAll(ctx, client, "items")
	noError(t, err)
	hasLen(t, all, 1)
	for _, got := range all {
		v, ok := lookupStat(got.Stats, "arg")
		isTrue(t, ok)
		equal(t, "items", v)
	}

	_, err = doStatsAll(ctx, client, "cachedump")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = doStatsAll(ctx, client, "detail", "on")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = doStatsAll(ctx, client, "reset")
	isErrorIs(t, err, ErrAdminNotEnabled)
	_, err = doStatsAll(ctx, client, "sizes")
	isErrorIs(t, err, ErrAdminNotEnabled)

	noError(t, doAdminStatsReset(ctx, mustAdminOn(t, client)))

	noError(t, mustAdmin(t, client).All().Flush(ctx, FlushOpts{Stagger: time.Second}))
}

func TestFlushAllAfterWritesDelayToken(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Delay: new(Rel(10 * time.Second))}))
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
	noError(t, mustAdmin(t, client).All().Flush(ctx, FlushOpts{}))
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
	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Stagger: time.Second}))
	delays := make(map[string]struct{}, 2)
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
	noError(t, mustAdmin(t, client).On(sv[0]).Flush(ctx, FlushOpts{}))

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

	noError(t, mustAdmin(t, client).All().Flush(ctx, FlushOpts{Delay: new(Rel(time.Second))}))
}

func TestFlushAllDelayRejectsStagger(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	ctx := context.Background()
	err := mustAdmin(t, client).All().Flush(ctx, FlushOpts{
		Delay: new(Rel(time.Second)), Stagger: time.Second,
	})
	isErrorIs(t, err, ErrInvalidArgument)
	err = mustAdmin(t, client).On(client.Servers()[0]).Flush(ctx, FlushOpts{
		Delay: new(ExptimeImmediate), Stagger: time.Second,
	})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestFlushAllExpImmediate(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Delay: new(ExptimeImmediate)}))
	srv.mu.Lock()
	equal(t, "-1", srv.lastFlushDelay)
	srv.mu.Unlock()
}

func TestFlushOnRequiresAllowFlushAll(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))
	isErrorIs(t, mustAdminOn(t, client).Flush(ctx, FlushOpts{}), ErrFlushAllNotAllowed)
	equal(t, []byte("v"), mustGet(ctx, t, client, []byte("k")).Value)
}

func TestFlushAllOnAddrRecvError(t *testing.T) {
	addr, dials := startCloseAfterCommand(t, "flush_all")
	client := testClient(t, addr, func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	err := mustAdminOn(t, client).Flush(context.Background(), FlushOpts{})
	isErrorIs(t, err, ErrTransport)
	ver, verErr := doVersion(context.Background(), client)
	noError(t, verErr)
	equal(t, []byte("after-close"), ver)
	greater(t, int(dials.Load()), 1)
}

func TestFlushAllOnAddrBadStatus(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "flush_all":
			return "CLIENT_ERROR bad flush\r\n"
		case "version":
			return "VERSION after-flush\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr, func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	err := mustAdminOn(t, client).Flush(context.Background(), FlushOpts{})
	isErrorIs(t, err, ErrClientError)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-flush"), ver)
	equal(t, int32(1), dials.Load())
}
