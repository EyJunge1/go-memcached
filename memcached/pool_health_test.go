package memcached

import (
	"context"
	"testing"
	"time"
)

func TestTakeColdIdle(t *testing.T) {
	p := newConnPool(2, 2, 0, 0, nil)
	if got := p.takeColdIdle(time.Second); got != nil {
		t.Fatal("empty pool should return nil")
	}

	now := time.Unix(100, 0)
	p.now = func() time.Time { return now }
	young := &mockConn{returnedAt: now.Add(-100 * time.Millisecond)}
	cold := &mockConn{returnedAt: now.Add(-2 * time.Second)}
	young.setConnPool(p)
	cold.setConnPool(p)
	p.idle = []wireConn{cold, young}

	if got := p.takeColdIdle(5 * time.Second); got != nil {
		t.Fatal("cold idle younger than minIdle should stay")
	}
	got := p.takeColdIdle(time.Second)
	if got != cold {
		t.Fatalf("got %#v want cold", got)
	}
	hasLen(t, p.idle, 1)

	p.closed.Store(true)
	if got := p.takeColdIdle(0); got != nil {
		t.Fatal("closed pool should return nil")
	}
}

func TestPingOneIdleHealthy(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}))

	client.mu.RLock()
	var pool *connPool
	var node *addr
	for _, a := range client.addrs {
		pool = client.connPools[a.id()]
		node = a
		break
	}
	client.mu.RUnlock()
	if pool == nil || node == nil {
		t.Fatal("missing pool")
	}
	pool.mu.Lock()
	for _, cn := range pool.idle {
		if c, ok := cn.(*conn); ok {
			c.returnedAt = time.Now().Add(-time.Minute)
		}
	}
	nIdle := len(pool.idle)
	pool.mu.Unlock()
	if nIdle == 0 {
		t.Fatal("expected idle conn after store")
	}

	client.pingOneIdle(ctx, pool, node, time.Millisecond)
	ps := client.PoolStats()
	hasLen(t, ps, 1)
}

func TestPingOneIdleNilWhenEmpty(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	p := newConnPool(1, 1, 0, 0, nil)
	client.pingOneIdle(context.Background(), p, client.addrs[0], time.Second)
}
