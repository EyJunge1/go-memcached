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
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	_, err = client.Admin()
	isErrorIs(t, err, ErrAdminNotEnabled)
}

func TestAdminSyncCommands(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

	ctx := context.Background()
	noError(t, admin.Verbosity(ctx, 1))
	noError(t, admin.CacheMemLimit(ctx, 64))
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

func TestAdminWatchAndDumps(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

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
	equal(t, "foo", row["key"])
	_, ok := <-meta
	isFalse(t, ok)

	mg, err := admin.LRUCrawlerMgDump(ctx2, "all")
	noError(t, err)
	line := <-mg
	equal(t, "mg foo", line)
}

func TestAdminDumpBusy(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpBusy = true
	srv.mu.Unlock()
	client, err := New(srv.Addr(), WithAdminCommands())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)
	_, err = admin.LRUCrawlerMetaDump(context.Background(), "all")
	isErrorIs(t, err, ErrBusy)
}

func TestAdminWatchCancelDiscardsConn(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithAdminCommands(), WithMaxIdleConns(4), WithDialContext(func(_ context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return net.Dial(network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	admin, err := client.Admin()
	noError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
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
