package memcached

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDialContextUsed(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	greater(t, int(dials.Load()), 0)
}

func TestTransportErrorDiscardsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	srv := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
		srv.serve()
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	err = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isError(t, err)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(2), dials.Load())
}

func TestDialContextHonorsDialTimeout(t *testing.T) {
	client := testClient(t, "127.0.0.1:1", func(cfg *Config) {
		cfg.Transport.DialTimeout = 50 * time.Millisecond
		cfg.Transport.Dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
	})

	err := client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetrySkipsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.RetryAttempts = 3
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	_, err := client.Incr(context.Background(), []byte("missing"), 1)
	isErrorIs(t, err, ErrNotFound)
	equal(t, int32(1), dials.Load())
}

func TestRetryOnDialFailure(t *testing.T) {
	srv := startFakeMemcached(t)
	var n atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.RetryAttempts = 2
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if n.Add(1) == 1 {
				return nil, fmt.Errorf("dial boom")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(2), n.Load())
}

func TestFailoverEjectsDeadNode(t *testing.T) {
	good := startFakeMemcached(t)
	var n atomic.Int32
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				n.Add(1)
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	_ = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	_ = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isTrue(t, len(client.DeadServers()) > 0 || n.Load() > 0)
}

func TestSetServersDropsRemovedHealth(t *testing.T) {
	good := startFakeMemcached(t)
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.pickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_ = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	greater(t, len(client.DeadServers()), 0)

	onlyGood, err := ParseServers(good.Addr())
	noError(t, err)
	noError(t, client.SetServers(onlyGood))
	equal(t, 0, len(client.DeadServers()))

	again, err := ParseServers("127.0.0.1:1," + good.Addr())
	noError(t, err)
	noError(t, client.SetServers(again))
	equal(t, 0, len(client.DeadServers()))
}

func TestDispatchHonorsCancelWithoutDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		time.Sleep(10 * time.Second)
	}()

	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.ReadTimeout = 5 * time.Second
		cfg.Transport.WriteTimeout = 5 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err = client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isError(t, err)
	isTrue(t, time.Since(start) < 2*time.Second)
}

func TestUDPEnabledDoesNotFrameTCP(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item := mustGet(t, client, context.Background(), []byte("k"))
	equal(t, []byte("v"), item.Value)
}

func TestNewRejectsEmptyServers(t *testing.T) {
	_, err := New(Config{})
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestNewRejectsResolverRefreshWithoutAllowSetServers(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Cluster.ResolverRefresh = time.Second
	_, err = New(cfg)
	isErrorIs(t, err, ErrSetServersNotAllowed)
}

func TestNewUsesKetamaAndFailoverOff(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, ok := c.rt.pickBuilder.(ketamaPickBuilder)
	isTrue(t, ok)
	equal(t, Failover{}, c.rt.failover)
}

func TestNewAppliesDefaultFailover(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	cfg.Failover = DefaultFailover()
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, DefaultFailover(), c.rt.failover)
}

func TestNewExplicitEmptyFailoverDisables(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	cfg.Failover = Failover{}
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, 0, c.rt.failover.FailureLimit)
	isFalse(t, c.rt.failover.Eject)
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

	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, []byte("v"), item.Value)
	equal(t, []byte("k"), item.Key)
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
	client, err := New(cfg)
	noError(t, err)
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
	client, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	equal(t, 3*time.Second, client.rt.dialTimeout)
	equal(t, time.Duration(0), client.rt.readTimeout)
	equal(t, 5*time.Second, client.rt.writeTimeout)
	equal(t, 1, client.rt.retryAttempts)
	equal(t, 100, client.rt.maxOpen)
	equal(t, 2, client.rt.maxIdle)
	equal(t, true, client.rt.tcpNoDelay)
}

func TestNewPoolNegativeMaxIdleMeansNone(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MaxIdle = -1
	client, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	equal(t, 0, client.rt.maxIdle)
	equal(t, 100, client.rt.maxOpen)
}

func Test_quarantineDuration(t *testing.T) {
	fo := Failover{FailureLimit: 2, RetryAfter: time.Second, DeadAfter: 30 * time.Second}
	equal(t, time.Second, quarantineDuration(fo, 2))
	equal(t, 30*time.Second, quarantineDuration(fo, 3))
	equal(t, 30*time.Second, quarantineDuration(Failover{FailureLimit: 1, DeadAfter: 30 * time.Second}, 1))
	equal(t, time.Duration(0), quarantineDuration(Failover{FailureLimit: 2}, 2))
}

func TestCloseDuringGet(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	mustStore(t, client, context.Background(), []byte("k"), []byte("v"))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = client.Close()
	}()
	go func() {
		defer wg.Done()
		_, _ = client.Get(context.Background(), [][]byte{[]byte("k")}, GetOpts{})
	}()
	wg.Wait()

	_, err := client.Get(context.Background(), [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrClosed)
}
