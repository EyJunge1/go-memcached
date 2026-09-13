package memcached

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetMultiAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "a-one", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b-two", []byte("2"), 0, 0))

	items, err := client.Gets(ctx, "a-one", "b-two")
	noError(t, err)
	hasLen(t, items, 2)

	got := clientMustGetMulti(t, client, "a-one", "b-two")
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	items, err = client.GetAndTouches(ctx, time.Second, "a-one", "b-two")
	noError(t, err)
	hasLen(t, items, 2)
}

func clientMustGetMulti(t *testing.T, client *Client, keys ...string) map[string]*Item {
	t.Helper()
	got, err := client.GetMulti(context.Background(), keys)
	noError(t, err)
	return got
}

func TestGetValueContainsCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	want := []byte("hello\nworld")
	noError(t, client.Set(ctx, "k", want, 0, 0))
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, want, item.Value)
}

func TestGetValueLooksLikeEND(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "k", []byte("END"), 0, 0))
	item, err := client.Get(ctx, "k")
	noError(t, err)
	equal(t, []byte("END"), item.Value)

	noError(t, client.Set(ctx, "k2", []byte("ERROR"), 0, 0))
	item, err = client.Get(ctx, "k2")
	noError(t, err)
	equal(t, []byte("ERROR"), item.Value)
}

func TestClusterGetFollowsSetNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr()+","+b.Addr(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "a-one", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b-two", []byte("2"), 0, 0))

	item, err := client.Get(ctx, "a-one")
	noError(t, err)
	equal(t, []byte("1"), item.Value)

	item, err = client.Get(ctx, "b-two")
	noError(t, err)
	equal(t, []byte("2"), item.Value)

	item, err = client.GetAndTouch(ctx, time.Second, "b-two")
	noError(t, err)
	equal(t, []byte("2"), item.Value)
}

func TestGetMalformedValueDiscardsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	srv := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		r := bufio.NewReader(cn)
		_, _ = r.ReadBytes('\n')
		_, _ = cn.Write([]byte("VALUE k 0 3\r\nabc\r\nVALUE\r\nEND\r\n"))
		srv.serve()
	}()

	var dials atomic.Int32
	client, err := New(ln.Addr().String(), WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Get(context.Background(), "k")
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	equal(t, int32(2), dials.Load())
}

func TestGetMultiPartialNodeFailure(t *testing.T) {
	good := startFakeMemcached(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
	}()

	// Force keys onto known nodes via prefix picker.
	client, err := New(good.Addr()+","+ln.Addr().String(), WithPickBuilder(prefixPickBuilder{}))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	// prefixPickBuilder: first char selects node. Use keys that land on both.
	// If prefix picker isn't available, fall back to setting on good and Gets both.
	noError(t, client.Set(ctx, "a-good", []byte("v"), 0, 0))
	items, err := client.GetMulti(ctx, []string{"a-good", "b-dead"})
	isError(t, err)
	notNil(t, items["a-good"])
	equal(t, []byte("v"), items["a-good"].Value)
}
