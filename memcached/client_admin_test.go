package memcached

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseWatchTS(t *testing.T) {
	equal(t, time.Unix(100, 0), parseWatchTS("100"))
	equal(t, time.Unix(100, 789*1000), parseWatchTS("100.000789"))
	equal(t, time.Unix(1619123456, 123*1000), parseWatchTS("1619123456.123"))
	equal(t, time.Unix(100, 0), parseWatchTS("100.1000000"))
	equal(t, time.Time{}, parseWatchTS("not-a-ts"))
}

func TestAdminStatsSizesClearsDestOnError(t *testing.T) {
	client := testClient(t, "127.0.0.1:1", func(cfg *Config) { cfg.AllowAdmin = true })
	dst := []Stat{{Name: []byte("stale")}}
	err := mustAdminOn(t, client).StatsSizes(context.Background(), &dst)
	isError(t, err)
	equal(t, 0, len(dst))
}

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
	noError(t, admin.SlabsAutomove(ctx, SlabsAutomoveNormal))
	noError(t, admin.LRUMode(ctx, LRUSegmented))
	noError(t, admin.LRUTune(ctx, 20, 40, 2, 2))
	noError(t, admin.LRUTempTTL(ctx, 60*time.Second))
	noError(t, admin.LRUCrawlerEnable(ctx, LRUCrawlerEnabled))
	noError(t, admin.LRUCrawlerSleep(ctx, 100*time.Microsecond))
	noError(t, admin.LRUCrawlerToCrawl(ctx, 10))
	noError(t, admin.LRUCrawlerCrawl(ctx, "all"))
	noError(t, admin.SlabsReassign(ctx, 1, 2))
	isErrorIs(t, admin.SlabsReassign(ctx, 1, 999), ErrBusy)
	isErrorIs(t, admin.SlabsReassign(ctx, 2, 2), ErrSame)
	isErrorIs(t, admin.LRUMode(ctx, LRUMode("flat\r\nshutdown")), ErrInvalidArgument)
	isErrorIs(t, admin.LRUCrawlerCrawl(ctx, "all\r\nwatch"), ErrInvalidArgument)
	isErrorIs(t, admin.LRUCrawlerCrawl(ctx, "hash"), ErrInvalidArgument)
	noError(t, admin.Shutdown(ctx, ShutdownImmediate))
}

func TestAdminWatchRequiresDeadline(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	_, err := admin.Watch(context.Background(), WatchFetchers)
	isErrorIs(t, err, ErrDeadlineRequired)
}

func TestAdminAllBroadcast(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
	})
	all := mustAdmin(t, client).All()
	ctx := context.Background()
	noError(t, all.Verbosity(ctx, 1, AdminOpts{}))
	noError(t, all.CacheMemLimit(ctx, 32, AdminOpts{}))
}

func TestAdminAllQuietBroadcast(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
	})
	all := mustAdmin(t, client).All()
	ctx := context.Background()
	noError(t, all.Verbosity(ctx, 1, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, a, "verbosity 1 noreply")
	waitLastCmdContains(t, b, "verbosity 1 noreply")
	noError(t, all.CacheMemLimit(ctx, 32, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, a, "cache_memlimit 32 noreply")
	waitLastCmdContains(t, b, "cache_memlimit 32 noreply")
}

func TestAdminVerbosityRejectsNegative(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	ctx := context.Background()
	isErrorIs(t, mustAdminOn(t, client).Verbosity(ctx, -1, AdminOpts{}), ErrInvalidArgument)
	isErrorIs(t, mustAdmin(t, client).All().Verbosity(ctx, -1, AdminOpts{}), ErrInvalidArgument)
}

func TestAdminOnUnknownServer(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	err := mustAdmin(t, client).On(Server{Address: "127.0.0.1:1"}).Verbosity(context.Background(), 1, AdminOpts{})
	isErrorIs(t, err, ErrInvalidAddress)
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

	watch := mustWatch(ctx, t, admin, WatchFetchers)
	defer watch.Close()
	ev := <-watch.Events
	equal(t, "foo", ev.Raw["key"])
	equal(t, uint64(1), ev.GID)
	equal(t, time.Unix(100, 789*1000), ev.TS)
	// Release the dedicated watch conn before dumps (Close is idempotent with defer).
	watch.Close()
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	meta := mustMetaDump(ctx2, t, admin, "all")
	defer meta.Close()
	row := <-meta.Entries
	equal(t, "foo", row.Fields["key"])
	_, ok := <-meta.Entries
	isFalse(t, ok)

	mg := mustMgDump(ctx2, t, admin, "all")
	defer mg.Close()
	line := <-mg.Entries
	equal(t, "mg foo", line.Line)
}

func TestAdminDumpWithoutLaunchOK(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	meta := mustMetaDump(ctx, t, admin, "all")
	defer meta.Close()
	row := <-meta.Entries
	equal(t, "foo", row.Fields["key"])
	_, ok := <-meta.Entries
	isFalse(t, ok)

	mg := mustMgDump(ctx, t, admin, "all")
	defer mg.Close()
	line := <-mg.Entries
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
	watch := mustWatch(ctx, t, admin, WatchMutations)
	<-watch.Events
	afterWatch := dials.Load()
	watch.Close() // waits for reader; dedicated conn is discarded
	cancel()
	err := client.Version(context.Background(), new([]byte))
	noError(t, err)
	if dials.Load() <= afterWatch {
		t.Fatalf("watch cancel should discard the conn; dials watch=%d after=%d", afterWatch, dials.Load())
	}
}

func TestAdminWatchCloseJoinsReader(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	watch := mustWatch(ctx, t, admin, WatchFetchers)
	<-watch.Events
	done := make(chan struct{})
	go func() {
		watch.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WatchStream.Close did not join reader")
	}
}

func TestAdminWatchLineCap(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("OK\r\n"))
		huge := make([]byte, _adminStreamLineLimit+8)
		for i := range huge {
			huge[i] = 'x'
		}
		_, _ = cn.Write(huge)
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	watch := mustWatch(ctx, t, admin, WatchFetchers)
	defer watch.Close()
	ev, ok := <-watch.Events
	isTrue(t, ok)
	isErrorIs(t, ev.Err, ErrMalformedResponse)
}

func TestAdminDumpSendFails(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = cn.Close()
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := admin.LRUCrawlerMetaDump(ctx, "all")
	isErrorIs(t, err, ErrTransport)
}

func TestAdminDumpOKThenError(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("OK\r\nERROR locked try again later\r\n"))
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := admin.LRUCrawlerMetaDump(ctx, "all")
	isErrorIs(t, err, ErrCommandError)
}

func TestAdminWatchAfterClose(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	noError(t, client.Close())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := admin.Watch(ctx, WatchFetchers)
	isErrorIs(t, err, ErrClosed)
}

func TestAdminShutdownEmptyReplyOK(t *testing.T) {
	addr, _ := startCmdServer(t, func(_ []string) string {
		return ""
	})
	client := testClient(t, addr, func(cfg *Config) { cfg.AllowAdmin = true })
	noError(t, mustAdminOn(t, client).Shutdown(context.Background(), ShutdownImmediate))
}
