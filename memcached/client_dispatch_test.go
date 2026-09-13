package memcached

import (
	"context"
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

	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
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

	err = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	isError(t, err)
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
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

	err := client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
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

	_, err := client.Incr(context.Background(), []byte("missing"), 1, MutateOpts{})
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

	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	equal(t, int32(2), n.Load())
}

func TestFailoverEjectsDeadNode(t *testing.T) {
	good := startFakeMemcached(t)
	var n atomic.Int32
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = &Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				n.Add(1)
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	_ = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	_ = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	isTrue(t, len(client.DeadServers()) > 0 || n.Load() > 0)
}

func TestSetServersDropsRemovedHealth(t *testing.T) {
	good := startFakeMemcached(t)
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.PickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = &Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_ = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
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
	err = client.Set(ctx, []byte("k"), []byte("v"), 0, 0)
	isError(t, err)
	isTrue(t, time.Since(start) < 2*time.Second)
}

func TestUDPEnabledDoesNotFrameTCP(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
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

func TestNewDefaultsToKetamaAndFailover(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	b, ok := c.options.pickBuilder.(ketamaPickBuilder)
	isTrue(t, ok)
	isTrue(t, b.opts.Weighted)
	equal(t, KetamaHashMD5, b.opts.Hash)
	equal(t, 2, c.options.failover.FailureLimit)
	equal(t, time.Second, c.options.failover.RetryAfter)
	equal(t, 30*time.Second, c.options.failover.DeadAfter)
	isTrue(t, c.options.failover.Eject)
}

func TestNewExplicitEmptyFailoverDisables(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211,10.0.0.1:11211")
	noError(t, err)
	cfg.Failover = &Failover{}
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	equal(t, 0, c.options.failover.FailureLimit)
	isFalse(t, c.options.failover.Eject)
}

func TestNamespacePrefixesWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "ns:k")

	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)
	equal(t, []byte("ns:k"), item.Key)
}

func TestMinIdleWarmsPool(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.MinIdle = 2
		cfg.Pool.MaxIdle = 4
		cfg.Pool.MaxOpen = 100
	})
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	ps := client.PoolStats()
	hasLen(t, ps, 1)
	greater(t, ps[0].Total, 1)
}

type countingResolver struct {
	inner defaultResolver
	n     atomic.Int32
}

func (r *countingResolver) Resolve(addr string) ([]*Addr, error) {
	r.n.Add(1)
	return r.inner.Resolve(addr)
}

func TestNamespaceHashWithPrefixChangesPick(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "b-"
		cfg.Cluster.HashWithPrefix = true
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("foo"), []byte("v"), 0, 0)))
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
		cfg.Cluster.Resolver = r
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

func (r *failResolver) Resolve(string) ([]*Addr, error) {
	r.n.Add(1)
	return nil, errors.New("resolve failed")
}

func TestResolverRefreshKeepsStandAndRecordsError(t *testing.T) {
	srv := startFakeMemcached(t)
	r := &failResolver{}
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
		cfg.Cluster.Resolver = r
		cfg.Cluster.ResolverRefresh = 20 * time.Millisecond
		cfg.Cluster.ResolveSpec = srv.Addr()
	})
	deadline := time.Now().Add(500 * time.Millisecond)
	for client.LastResolverError() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	isError(t, client.LastResolverError())
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

func (r *recordingResolver) Resolve(spec string) ([]*Addr, error) {
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
		cfg.Cluster.Resolver = r
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

func TestNewRejectsPoolMaxOpenZero(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 1
	cfg.Pool.MaxOpen = 0
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func Test_quarantineDuration(t *testing.T) {
	fo := Failover{FailureLimit: 2, RetryAfter: time.Second, DeadAfter: 30 * time.Second}
	equal(t, time.Second, quarantineDuration(fo, 2))
	equal(t, 30*time.Second, quarantineDuration(fo, 3))
	equal(t, 30*time.Second, quarantineDuration(Failover{FailureLimit: 1, DeadAfter: 30 * time.Second}, 1))
	equal(t, time.Duration(0), quarantineDuration(Failover{FailureLimit: 2}, 2))
}
