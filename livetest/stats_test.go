package livetest

import (
	"context"
	"testing"
)

func TestVersionAndStats(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()

	ver, err := c.Version(ctx)
	noError(t, err)
	notEmpty(t, ver)

	all, err := c.VersionAll(ctx)
	noError(t, err)
	hasLen(t, all, 1)
	notEmpty(t, all[0].Version)

	st, err := c.Stats(ctx)
	noError(t, err)
	if _, ok := st["pid"]; !ok {
		t.Fatalf("stats missing pid: %#v", st)
	}

	for _, sub := range []string{"settings", "items", "slabs", "conns"} {
		got, err := c.Stats(ctx, sub)
		noError(t, err)
		if len(got) == 0 {
			t.Fatalf("stats %s empty", sub)
		}
	}

	nodes, err := c.StatsAll(ctx)
	noError(t, err)
	hasLen(t, nodes, 1)
	if _, ok := nodes[0].Stats["pid"]; !ok {
		t.Fatalf("StatsAll missing pid")
	}

	stats := c.PoolStats()
	if len(stats) == 0 {
		t.Fatal("PoolStats empty after Version")
	}
}
