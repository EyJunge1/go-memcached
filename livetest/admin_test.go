package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestAdminGates(t *testing.T) {
	c := setupLiveClient(t)
	_, err := c.Admin()
	isErrorIs(t, err, memcached.ErrAdminNotEnabled)

	admin := setupAdminClient(t)
	_, err = admin.Admin()
	noError(t, err)

	plain := setupLiveClient(t, func(cfg *memcached.Config) {
		cfg.AllowAdmin = true
	})
	gated, err := plain.Admin()
	noError(t, err)
	isErrorIs(t, gated.All().Flush(context.Background(), memcached.FlushOpts{}), memcached.ErrFlushAllNotAllowed)
}

func TestAdminStatsAndCrawler(t *testing.T) {
	c := setupAdminClient(t)
	ctx := context.Background()
	admin, err := c.Admin()
	noError(t, err)
	node := admin.On(c.Servers()[0])

	noError(t, node.Verbosity(ctx, 0, memcached.AdminOpts{}))
	noError(t, node.SlabsAutomove(ctx, 0))
	noError(t, node.LRUTempTTL(ctx, 0))
	noError(t, node.LRUCrawlerSleep(ctx, 100))
	noError(t, node.LRUCrawlerToCrawl(ctx, 100))
	if err := node.LRUCrawlerEnable(ctx, true); err != nil {
		t.Logf("crawler enable: %v", err)
	}

	_, err = node.StatsDetail(ctx, memcached.StatsDetailDump)
	noError(t, err)

	key := liveKey(t, "dump")
	mustStore(t, c, ctx, key, []byte("v"))

	dumpCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ch, err := node.LRUCrawlerMetaDump(dumpCtx, "all")
	noError(t, err)
	found := false
	for e := range ch {
		if e.Err != nil {
			t.Fatalf("metadump: %v", e.Err)
		}
		if e.Fields["key"] == string(key) {
			found = true
		}
	}
	if !found {
		t.Fatalf("metadump missed key %q", key)
	}

	mgCtx, mgCancel := context.WithTimeout(ctx, 3*time.Second)
	defer mgCancel()
	mg, err := node.LRUCrawlerMgDump(mgCtx, "all")
	noError(t, err)
	for e := range mg {
		if e.Err != nil {
			t.Fatalf("mgdump: %v", e.Err)
		}
	}
}

func TestAdminWatch(t *testing.T) {
	c := setupAdminClient(t)
	admin, err := c.Admin()
	noError(t, err)
	node := admin.On(c.Servers()[0])

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := node.Watch(ctx, memcached.WatchMutations)
	noError(t, err)

	key := liveKey(t, "w")
	mustStore(t, c, context.Background(), key, []byte("v"))

	got := 0
	for range ch {
		got++
		cancel()
	}
	if got == 0 {
		t.Fatal("watch got no mutation events")
	}
}

func TestAdminFlush(t *testing.T) {
	c := setupAdminClient(t)
	ctx := context.Background()
	key := liveKey(t, "f")
	mustStore(t, c, ctx, key, []byte("gone"))

	admin, err := c.Admin()
	noError(t, err)
	noError(t, admin.On(c.Servers()[0]).Flush(ctx, memcached.FlushOpts{}))
	mustMiss(t, c, ctx, key)

	mustStore(t, c, ctx, key, []byte("again"))
	noError(t, admin.All().Flush(ctx, memcached.FlushOpts{}))
	mustMiss(t, c, ctx, key)
}
