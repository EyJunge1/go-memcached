package memcached

import (
	"context"
	"testing"
	"time"
)

func TestPipelineChunksByMaxPipelineOps(t *testing.T) {
	srv := startFakeMemcached(t)

	client, err := New(Config{
		Servers: []Server{{Address: srv.Addr()}},
		Cluster: Cluster{MaxPipelineOps: 2},
		Pool: Pool{
			MaxOpen: 1,
			MaxIdle: 1,
		},
		Transport: Transport{
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p := client.Pipeline()
	for i := 0; i < 5; i++ {
		key := []byte{byte('a' + i)}
		p.Store(Item{Key: key, Value: []byte("v")})
	}
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// 5 ops / max 2 => 3 waves => 3 checkouts (1 miss + 2 idle hits).
	stats := client.PoolStats()
	if len(stats) == 0 {
		t.Fatal("no pool stats")
	}
	checkouts := stats[0].IdleHits + stats[0].IdleMisses
	if checkouts < 3 {
		t.Fatalf("checkouts=%d want >= 3 (hits=%d misses=%d)", checkouts, stats[0].IdleHits, stats[0].IdleMisses)
	}
}
