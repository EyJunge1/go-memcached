package memcached

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRejectsEmptyServers(t *testing.T) {
	var empty Config
	_, err := New(empty)
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestNewRejectsResolverRefreshWithoutAllowSetServers(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Cluster.ResolverRefresh = time.Second
	_, err = New(cfg)
	isErrorIs(t, err, ErrSetServersNotAllowed)
}

func TestNewUsesRendezvousAndFailoverOff(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	c := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = c.Close() })
	_, ok := c.rt.pickBuilder.(rendezvousPickBuilder)
	isTrue(t, ok)
	equal(t, Failover{}, c.rt.failover)
}

func TestNewAppliesDefaultFailover(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	cfg.Failover = DefaultFailover()
	c := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, DefaultFailover(), c.rt.failover)
}

func TestNewExplicitEmptyFailoverDisables(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	cfg.Failover = Failover{}
	c := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, 0, c.rt.failover.FailureLimit)
	isFalse(t, c.rt.failover.Eject)
}

func TestNsKeyReusesPool(t *testing.T) {
	c := &Client{rt: &runtime{nsPrefix: []byte("ns:")}}
	raw := []byte("k")
	want := []byte("ns:k")
	_, box := c.nsKey(raw)
	putBuilderBox(box)
	allocs := testing.AllocsPerRun(200, func() {
		wired, b := c.nsKey(raw)
		if !bytes.Equal(wired, want) {
			t.Fatalf("got %q", wired)
		}
		putBuilderBox(b)
	})
	if allocs != 0 {
		t.Fatalf("nsKey allocs = %v, want 0", allocs)
	}
}

func TestNamespacePrefixesWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "ns:k")

	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, []byte("v"), item.Value)
}

func TestMinIdleWarmsPool(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.MinIdle = 2
		cfg.Pool.MaxIdle = 4
		cfg.Pool.MaxOpen = 100
	})
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	ps := client.PoolStats()
	hasLen(t, ps, 1)
	greater(t, ps[0].Total, 1)
}

type countingResolver struct {
	inner defaultResolver
	n     atomic.Int32
}

var _ resolver = (*countingResolver)(nil)

func (r *countingResolver) Resolve(spec string) ([]*addr, error) {
	r.n.Add(1)
	return r.inner.Resolve(spec)
}

func TestNamespaceIsIncludedInPick(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "b-"
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("foo"), Value: []byte("v")}))
	b.mu.Lock()
	_, onB := b.items["b-foo"]
	b.mu.Unlock()
	isTrue(t, onB)
}

func TestResolverRefreshRecallsResolve(t *testing.T) {
	srv := startFakeMemcached(t)
	r := &countingResolver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.resolver = r
		cfg.Cluster.ResolverRefresh = 20 * time.Millisecond
		cfg.Cluster.ResolveSpec = srv.Addr()
	})
	time.Sleep(80 * time.Millisecond)
	if r.n.Load() < 2 {
		t.Fatalf("expected refresh resolves, got %d", r.n.Load())
	}
	_ = client
}

type failResolver struct {
	n atomic.Int32
}

var _ resolver = (*failResolver)(nil)

func (r *failResolver) Resolve(string) ([]*addr, error) {
	r.n.Add(1)
	return nil, errors.New("resolve failed")
}

func TestResolverRefreshKeepsStandAndRecordsError(t *testing.T) {
	srv := startFakeMemcached(t)
	r := &failResolver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.resolver = r
		cfg.Cluster.ResolverRefresh = 20 * time.Millisecond
		cfg.Cluster.ResolveSpec = srv.Addr()
	})
	deadline := time.Now().Add(500 * time.Millisecond)
	for client.ResolverErr() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	isError(t, client.ResolverErr())
	servers := client.Servers()
	hasLen(t, servers, 1)
	equal(t, srv.Addr(), servers[0].Address)
	greater(t, int(r.n.Load()), 0)
}

type recordingResolver struct {
	target string
	mu     sync.Mutex
	specs  []string
}

var _ resolver = (*recordingResolver)(nil)

func (r *recordingResolver) Resolve(spec string) ([]*addr, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	return defaultResolver{}.Resolve(r.target)
}

func TestResolverRefreshKeepsResolveSpec(t *testing.T) {
	srv := startFakeMemcached(t)
	const keep = "keep-me.example:11211"
	r := &recordingResolver{target: srv.Addr()}
	_ = testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.resolver = r
		cfg.Cluster.ResolverRefresh = 20 * time.Millisecond
		cfg.Cluster.ResolveSpec = keep
	})
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		r.mu.Lock()
		n := len(r.specs)
		r.mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.mu.Lock()
	specs := append([]string(nil), r.specs...)
	r.mu.Unlock()
	if len(specs) < 2 {
		t.Fatalf("expected refresh resolves, got %d", len(specs))
	}
	for _, spec := range specs {
		equal(t, keep, spec)
	}
}

func TestNewPoolMaxOpenZeroUsesDefault(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 1
	cfg.Pool.MaxOpen = 0
	client := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	equal(t, 100, client.rt.maxOpen)
	equal(t, 10, client.rt.maxIdle)
}

func TestNewPoolPartialAndTimeoutNone(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	cfg.Transport.ReadTimeout = TimeoutNone
	cfg.Pool.MaxIdle = 2
	client := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	equal(t, 3*time.Second, client.rt.dialTimeout)
	equal(t, time.Duration(0), client.rt.readTimeout)
	equal(t, 5*time.Second, client.rt.writeTimeout)
	equal(t, 1, client.rt.retryAttempts)
	equal(t, time.Duration(0), client.rt.retryBackoff)
	equal(t, 100, client.rt.maxOpen)
	equal(t, 2, client.rt.maxIdle)
	equal(t, true, client.rt.tcpNoDelay)
}

func TestNewPoolNegativeMaxIdleMeansNone(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MaxIdle = -1
	client := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = client.Close() })
	equal(t, 0, client.rt.maxIdle)
	equal(t, 100, client.rt.maxOpen)
}
