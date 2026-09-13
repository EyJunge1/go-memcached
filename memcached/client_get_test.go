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
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a-one"), []byte("1"), 0, 0)))
	noError(t, storeOK(client.Set(ctx, []byte("b-two"), []byte("2"), 0, 0)))

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("a-one"), []byte("b-two"))
	noError(t, err)
	hasLen(t, items, 2)

	got := clientMustGetMulti(t, client, "a-one", "b-two")
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	exp := Rel(time.Second)
	items, err = client.Get(ctx, GetOpts{CAS: true, Touch: &exp}, []byte("a-one"), []byte("b-two"))
	noError(t, err)
	hasLen(t, items, 2)
}

func clientMustGetMulti(t *testing.T, client *Client, keys ...string) map[string]*Item {
	t.Helper()
	bs := make([][]byte, len(keys))
	for i, k := range keys {
		bs[i] = []byte(k)
	}
	got, err := client.GetMulti(context.Background(), bs)
	noError(t, err)
	return got
}

func TestGetValueContainsCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	want := []byte("hello\nworld")
	noError(t, storeOK(client.Set(ctx, []byte("k"), want, 0, 0)))
	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, want, item.Value)
}

func TestGetValueLooksLikeEND(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("END"), 0, 0)))
	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, []byte("END"), item.Value)

	noError(t, storeOK(client.Set(ctx, []byte("k2"), []byte("ERROR"), 0, 0)))
	item, err = client.GetOne(ctx, []byte("k2"))
	noError(t, err)
	equal(t, []byte("ERROR"), item.Value)
}

func TestClusterGetFollowsSetNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a-one"), []byte("1"), 0, 0)))
	noError(t, storeOK(client.Set(ctx, []byte("b-two"), []byte("2"), 0, 0)))

	item, err := client.GetOne(ctx, []byte("a-one"))
	noError(t, err)
	equal(t, []byte("1"), item.Value)

	item, err = client.GetOne(ctx, []byte("b-two"))
	noError(t, err)
	equal(t, []byte("2"), item.Value)

	exp := Rel(time.Second)
	items, err := client.Get(ctx, GetOpts{Touch: &exp}, []byte("b-two"))
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("2"), items[0].Value)
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
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	_, err = client.GetOne(context.Background(), []byte("k"))
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
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

	client := testClient(t, good.Addr()+","+ln.Addr().String(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
		cfg.Failover = &Failover{}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a-good"), []byte("v"), 0, 0)))
	items, err := client.GetMulti(ctx, [][]byte{[]byte("a-good"), []byte("b-dead")})
	isError(t, err)
	notNil(t, items["a-good"])
	equal(t, []byte("v"), items["a-good"].Value)
}

func TestGetValueContainsEmbeddedCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	ctx := context.Background()
	want := []byte("hello\r\nworld")
	noError(t, storeOK(client.Set(ctx, []byte("k"), want, 0, 0)))
	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, want, item.Value)

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("k"))
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, want, items[0].Value)

	got, err := client.GetMulti(ctx, [][]byte{[]byte("k")})
	noError(t, err)
	equal(t, want, got["k"].Value)

	meta, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, want, meta.Item.Value)

	_, err = client.Version(ctx)
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestGetMultiMissIsNotNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	items, err := client.Get(ctx, GetOpts{}, []byte("missing-a"), []byte("missing-b"))
	noError(t, err)
	hasLen(t, items, 0)
	_, err = client.GetOne(ctx, []byte("missing-a"))
	isErrorIs(t, err, ErrNotFound)
}
