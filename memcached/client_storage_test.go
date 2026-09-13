package memcached

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFakeServerRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "foo", []byte("bar"), 7, 0))
	item, err := client.Get(ctx, "foo")
	noError(t, err)
	equal(t, []byte("bar"), item.Value)
	equal(t, uint32(7), item.Flags)

	items, err := client.Gets(ctx, "foo")
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("bar"), items[0].Value)
}

func TestAddNotStoredKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.Add(ctx, "k", []byte("other"), 0, 0), ErrNotStored)
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestCasExistsKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	isErrorIs(t, client.Cas(ctx, "k", []byte("other"), 0, 0, 99), ErrExists)
	isErrorIs(t, client.Cas(ctx, "missing", []byte("x"), 0, 0, 1), ErrNotFound)
	noError(t, client.Set(ctx, "k2", []byte("v2"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestDeleteNotFoundKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	isErrorIs(t, client.Delete(ctx, "missing"), ErrNotFound)
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	equal(t, int32(1), dials.Load())
}

func TestSetRejectsCRLFKey(t *testing.T) {
	srv := startFakeMemcached(t)
	var writes atomic.Int32
	client, err := New(srv.Addr(), WithDialContext(func(_ context.Context, network, address string) (net.Conn, error) {
		writes.Add(1)
		return net.Dial(network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	err = client.Set(context.Background(), "a\r\nb", []byte("v"), 0, 0)
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, int32(0), writes.Load())
}

func TestNoReplyOmitsReadAndNextGetWorks(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithNoReply())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0, NoReply()))
	var cmd string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		cmd = srv.lastCmd
		srv.mu.Unlock()
		if strings.Contains(cmd, "noreply") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	contains(t, cmd, "noreply")

	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}
