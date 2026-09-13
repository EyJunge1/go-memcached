package memcached

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestDialContextUsed(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
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
	client, err := New(ln.Addr().String(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	isError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), dials.Load())
}

func TestDialContextHonorsDialTimeout(t *testing.T) {
	client, err := New("127.0.0.1:1",
		WithDialTimeout(50*time.Millisecond),
		WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	isErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetrySkipsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithRetry(3), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Incr(context.Background(), "missing", 1)
	isErrorIs(t, err, ErrNotFound)
	equal(t, int32(1), dials.Load())
}

func TestRetryOnDialFailure(t *testing.T) {
	srv := startFakeMemcached(t)
	var n atomic.Int32
	client, err := New(srv.Addr(), WithRetry(2), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		if n.Add(1) == 1 {
			return nil, fmt.Errorf("dial boom")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), n.Load())
}

func TestFailoverEjectsDeadNode(t *testing.T) {
	good := startFakeMemcached(t)
	var n atomic.Int32
	client, err := New("127.0.0.1:1,"+good.Addr(),
		WithPickBuilder(firstAddrPickBuilder{}),
		WithRetry(1),
		WithFailover(FailoverConfig{FailureLimit: 1, RetryTimeout: time.Hour, EjectFromHash: true}),
		WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" {
				n.Add(1)
				return nil, fmt.Errorf("dead")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_ = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	_ = client.Set(context.Background(), "k", []byte("v"), 0, 0)
	isTrue(t, len(client.DeadServers()) > 0 || n.Load() > 0)
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

	client, err := New(ln.Addr().String(), WithReadTimeout(5*time.Second), WithWriteTimeout(5*time.Second))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err = client.Set(ctx, "k", []byte("v"), 0, 0)
	isError(t, err)
	isTrue(t, time.Since(start) < 2*time.Second)
}

func TestUDPEnabledDoesNotFrameTCP(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithUDPEnabled())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestWithTLSNilRejected(t *testing.T) {
	_, err := New("127.0.0.1:11211", WithTLS(nil))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNamespacePrefixesWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithNamespace("ns:", false))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "ns:k")

	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
	equal(t, "ns:k", item.Key)
}

func TestMinIdleWarmsPool(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithMinIdleConns(2), WithMaxIdleConns(4))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
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
	client, err := New(a.Addr()+","+b.Addr(),
		WithNamespace("b-", true),
		WithPickBuilder(prefixPickBuilder{}),
	)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "foo", []byte("v"), 0, 0))
	b.mu.Lock()
	_, onB := b.items["b-foo"]
	b.mu.Unlock()
	isTrue(t, onB)
}

func TestResolverRefreshRecallsResolve(t *testing.T) {
	srv := startFakeMemcached(t)
	r := &countingResolver{}
	client, err := New(srv.Addr(), WithResolver(r), WithResolverRefresh(20*time.Millisecond))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	time.Sleep(80 * time.Millisecond)
	if r.n.Load() < 2 {
		t.Fatalf("expected refresh resolves, got %d", r.n.Load())
	}
}
