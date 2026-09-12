package memcached

import (
	"context"
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
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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

	err := client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
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

	var missing uint64
	err := client.Incr(context.Background(), []byte("missing"), 1, &missing, ArithmeticOpts{})
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

func TestRetryDelayFullJitter(t *testing.T) {
	equal(t, time.Duration(0), retryDelay(0, 1))
	equal(t, time.Duration(0), retryDelay(25*time.Millisecond, 0))

	base := 25 * time.Millisecond
	var sawPositive bool
	for i := 0; i < 200; i++ {
		d := retryDelay(base, 1)
		if d < 0 || d > base {
			t.Fatalf("attempt1 delay %v out of [0, %v]", d, base)
		}
		if d > 0 {
			sawPositive = true
		}
	}
	isTrue(t, sawPositive)

	d2 := retryDelay(base, 2)
	if d2 < 0 || d2 > 50*time.Millisecond {
		t.Fatalf("attempt2 delay %v out of [0, 50ms]", d2)
	}
	capped := retryDelay(200*time.Millisecond, 8)
	if capped < 0 || capped > _retryBackoffCap {
		t.Fatalf("capped delay %v out of [0, %v]", capped, _retryBackoffCap)
	}
}

func TestWaitRetryCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	isErrorIs(t, waitRetry(ctx, time.Second), context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := waitRetry(ctx, time.Second)
	isErrorIs(t, err, context.Canceled)
	isTrue(t, time.Since(start) < 500*time.Millisecond)
}

func TestMaybeWaitRetrySkipsHop(t *testing.T) {
	a := newAddr("tcp", "127.0.0.1:1")
	b := newAddr("tcp", "127.0.0.1:2")
	client := testClient(t, "127.0.0.1:1,127.0.0.1:2", func(cfg *Config) {
		cfg.Transport.RetryBackoff = time.Second
	})
	start := time.Now()
	noError(t, client.maybeWaitRetry(context.Background(), a, b, 1))
	isTrue(t, time.Since(start) < 50*time.Millisecond)

	client.rt.retryBackoff = 0
	noError(t, client.maybeWaitRetry(context.Background(), a, a, 1))
}

func TestRetryBackoffSameNodeDelays(t *testing.T) {
	// Full jitter can be 0 once; require a positive gap across a few failures.
	const rounds = 8
	base := 30 * time.Millisecond
	gaps := make([]time.Duration, 0, rounds)
	for round := 0; round < rounds; round++ {
		srv := startFakeMemcached(t)
		dialAt := make([]time.Time, 0, 2)
		var mu sync.Mutex
		client := testClient(t, srv.Addr(), func(cfg *Config) {
			cfg.Transport.RetryAttempts = 2
			cfg.Transport.RetryBackoff = base
			cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				mu.Lock()
				dialAt = append(dialAt, time.Now())
				n := len(dialAt)
				mu.Unlock()
				if n == 1 {
					return nil, fmt.Errorf("dial boom")
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}
		})
		noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
		mu.Lock()
		dialAt = requireSlice(t, dialAt)
		hasLen(t, dialAt, 2)
		gap := dialAt[1].Sub(dialAt[0])
		mu.Unlock()
		if gap > base+30*time.Millisecond {
			t.Fatalf("same-node gap %v exceeds bound %v", gap, base)
		}
		gaps = append(gaps, gap)
	}
	var positive int
	for _, g := range gaps {
		if g > time.Millisecond {
			positive++
		}
	}
	if positive == 0 {
		t.Fatalf("expected at least one same-node pause >1ms across %d rounds, gaps=%v", rounds, gaps)
	}
}

func TestRetryBackoffSkippedOnFailoverHop(t *testing.T) {
	good := startFakeMemcached(t)
	var dialAt []time.Time
	var mu sync.Mutex
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 2
		cfg.Transport.RetryBackoff = 200 * time.Millisecond
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			mu.Lock()
			dialAt = append(dialAt, time.Now())
			mu.Unlock()
			if address == "127.0.0.1:1" {
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	start := time.Now()
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	if time.Since(start) >= 150*time.Millisecond {
		t.Fatalf("failover hop waited too long: %v", time.Since(start))
	}
	mu.Lock()
	defer mu.Unlock()
	greater(t, len(dialAt), 1)
}

func TestRetryBackoffCancelDuringWait(t *testing.T) {
	client := testClient(t, "127.0.0.1:1", func(cfg *Config) {
		cfg.Transport.RetryAttempts = 3
		cfg.Transport.RetryBackoff = time.Second
		cfg.Transport.DialTimeout = 20 * time.Millisecond
		cfg.Transport.Dial = func(_ context.Context, _, _ string) (net.Conn, error) {
			return nil, fmt.Errorf("dial boom")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isError(t, err)
	isTrue(t, time.Since(start) < 500*time.Millisecond)
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

func TestFailoverHealthyRoutingDoesNotAllocate(t *testing.T) {
	client := testClient(t, "127.0.0.1:11211,127.0.0.1:11212", func(cfg *Config) {
		cfg.Failover = DefaultFailover()
	})
	addrs, _ := client.snapshotCluster()

	allocs := testing.AllocsPerRun(1000, func() {
		live := client.liveAddrs(addrs)
		if len(live) != len(addrs) {
			t.Fatalf("got %d live addresses, want %d", len(live), len(addrs))
		}
	})
	equal(t, float64(0), allocs)
}

func TestFailoverSingleNodeUsesFastPath(t *testing.T) {
	client := testClient(t, "127.0.0.1:11211", func(cfg *Config) {
		cfg.Failover = DefaultFailover()
	})
	addrs, _ := client.snapshotCluster()

	node, err := client.pickNode([]byte("key"))
	noError(t, err)
	same(t, addrs[0], node)
	notNil(t, client.single.Load())
}

func TestSameResolvedAddrsIncludesOrder(t *testing.T) {
	client := testClient(t, "127.0.0.1:11211,127.0.0.1:11212")
	isTrue(t, client.sameResolvedAddrs([]*addr{
		newAddr("tcp", "127.0.0.1:11211"),
		newAddr("tcp", "127.0.0.1:11212"),
	}))
	isFalse(t, client.sameResolvedAddrs([]*addr{
		newAddr("tcp", "127.0.0.1:11212"),
		newAddr("tcp", "127.0.0.1:11211"),
	}))
	isFalse(t, client.sameResolvedAddrs([]*addr{
		newAddr("tcp", "127.0.0.1:11211"),
	}))
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
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		// Stay blocked until the peer closes instead of sleeping past test end.
		_, _ = cn.Read(buf)
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
	err := client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isError(t, err)
	isTrue(t, time.Since(start) < 2*time.Second)
}

func TestUDPEnabledDoesNotFrameTCP(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item := mustGet(context.Background(), t, client, []byte("k"))
	equal(t, []byte("v"), item.Value)
}

func TestQuarantineDuration(t *testing.T) {
	fo := Failover{FailureLimit: 2, RetryAfter: time.Second, DeadAfter: 30 * time.Second}
	equal(t, time.Second, quarantineDuration(fo, 2))
	equal(t, 30*time.Second, quarantineDuration(fo, 3))
	equal(t, 30*time.Second, quarantineDuration(Failover{FailureLimit: 1, DeadAfter: 30 * time.Second}, 1))
	equal(t, time.Duration(0), quarantineDuration(Failover{FailureLimit: 2}, 2))
}

func TestCloseDuringGet(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	mustStore(context.Background(), t, client, []byte("k"), []byte("v"))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = client.Close()
	}()
	go func() {
		defer wg.Done()
		_, _ = doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	}()
	wg.Wait()

	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrClosed)
}
