package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdminRequiresOptIn(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.Admin()
	isErrorIs(t, err, ErrAdminNotEnabled)
}

func TestAdminSyncCommands(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)

	ctx := context.Background()
	noError(t, admin.Verbosity(ctx, 1, AdminOpts{}))
	noError(t, admin.CacheMemLimit(ctx, 64, AdminOpts{}))
	noError(t, admin.SlabsAutomove(ctx, 1))
	noError(t, admin.LRUMode(ctx, "segmented"))
	noError(t, admin.LRUTune(ctx, 20, 40, 2, 2))
	noError(t, admin.LRUTempTTL(ctx, 60))
	noError(t, admin.LRUCrawlerEnable(ctx, true))
	noError(t, admin.LRUCrawlerSleep(ctx, 100))
	noError(t, admin.LRUCrawlerToCrawl(ctx, 10))
	noError(t, admin.LRUCrawlerCrawl(ctx, "all"))
	noError(t, admin.SlabsReassign(ctx, 1, 2))
	isErrorIs(t, admin.SlabsReassign(ctx, 1, 999), ErrBusy)
	isErrorIs(t, admin.SlabsReassign(ctx, 2, 2), ErrSame)
	isErrorIs(t, admin.LRUMode(ctx, "flat\r\nshutdown"), ErrInvalidArgument)
	isErrorIs(t, admin.LRUCrawlerCrawl(ctx, "all\r\nwatch"), ErrInvalidArgument)
	noError(t, admin.Shutdown(ctx, false))
}

func TestAdminRequiresOnForNodeOps(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdmin(t, client)
	ctx := context.Background()
	isErrorIs(t, admin.Shutdown(ctx, false), ErrAdminTargetRequired)
	isErrorIs(t, admin.SlabsAutomove(ctx, 1), ErrAdminTargetRequired)
	isErrorIs(t, admin.LRUMode(ctx, "flat"), ErrAdminTargetRequired)
	_, err := admin.Watch(ctx, WatchFetchers)
	isErrorIs(t, err, ErrDeadlineRequired)
	_, err = admin.Watch(context.Background(), WatchFetchers)
	isErrorIs(t, err, ErrDeadlineRequired)
	ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = admin.Watch(ctx2, WatchFetchers)
	isErrorIs(t, err, ErrAdminTargetRequired)
}

func TestAdminAllBroadcast(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
	})
	all := mustAdmin(t, client).All()
	ctx := context.Background()
	noError(t, all.Verbosity(ctx, 1, AdminOpts{}))
	noError(t, all.CacheMemLimit(ctx, 32, AdminOpts{}))
	stats, err := all.Stats(ctx)
	noError(t, err)
	equal(t, 2, len(stats))
}

func TestAdminWatchAndDumps(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpSendOK = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	events, err := admin.Watch(ctx, WatchFetchers)
	noError(t, err)
	ev := <-events
	equal(t, "foo", ev.Raw["key"])
	equal(t, uint64(1), ev.GID)
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	meta, err := admin.LRUCrawlerMetaDump(ctx2, "all")
	noError(t, err)
	row := <-meta
	equal(t, "foo", row.Fields["key"])
	_, ok := <-meta
	isFalse(t, ok)

	mg, err := admin.LRUCrawlerMgDump(ctx2, "all")
	noError(t, err)
	line := <-mg
	equal(t, "mg foo", line.Line)
}

func TestAdminDumpWithoutLaunchOK(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	meta, err := admin.LRUCrawlerMetaDump(ctx, "all")
	noError(t, err)
	row := <-meta
	equal(t, "foo", row.Fields["key"])
	_, ok := <-meta
	isFalse(t, ok)

	mg, err := admin.LRUCrawlerMgDump(ctx, "all")
	noError(t, err)
	line := <-mg
	equal(t, "mg foo", line.Line)
}

func TestAdminDumpBusy(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpBusy = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := admin.LRUCrawlerMetaDump(ctx, "all")
	isErrorIs(t, err, ErrBusy)
}

func TestAdminWatchCancelDiscardsConn(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.Pool.MaxIdle = 4
		cfg.Pool.MaxOpen = 100
		cfg.Transport.Dial = func(_ context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return net.Dial(network, address)
		}
	})
	admin := mustAdminOn(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	events, err := admin.Watch(ctx, WatchMutations)
	noError(t, err)
	<-events
	afterWatch := dials.Load()
	cancel()
	time.Sleep(30 * time.Millisecond)
	_, err = client.Version(context.Background())
	noError(t, err)
	if dials.Load() <= afterWatch {
		t.Fatalf("watch cancel should discard the conn; dials watch=%d after=%d", afterWatch, dials.Load())
	}
}

func TestAdminWatchLineCap(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("OK\r\n"))
		huge := make([]byte, adminStreamLineLimit+8)
		for i := range huge {
			huge[i] = 'x'
		}
		_, _ = cn.Write(huge)
	}()

	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, err := admin.Watch(ctx, WatchFetchers)
	noError(t, err)
	ev, ok := <-events
	isTrue(t, ok)
	isErrorIs(t, ev.Err, ErrMalformedResponse)
}
