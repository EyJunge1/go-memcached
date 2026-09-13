package memcached

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a-one"), Value: []byte("1")}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("b-two"), Value: []byte("2")}))

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("a-one"), []byte("b-two"))
	noError(t, err)
	hasLen(t, items, 2)

	got := itemsByKey(items)
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	exp := Rel(time.Second)
	items, err = client.Get(ctx, GetOpts{CAS: true, Touch: &exp}, []byte("a-one"), []byte("b-two"))
	noError(t, err)
	hasLen(t, items, 2)
}

func itemsByKey(items []*Item) map[string]*Item {
	out := make(map[string]*Item, len(items))
	for _, item := range items {
		out[string(item.Key)] = item
	}
	return out
}

func TestGetValueContainsCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	want := []byte("hello\nworld")
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: want}))
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, want, item.Value)
}

func TestGetValueLooksLikeEND(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("END")}))
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, []byte("END"), item.Value)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("ERROR")}))
	item = mustGet(t, client, ctx, []byte("k2"))
	equal(t, []byte("ERROR"), item.Value)
}

func TestClusterGetFollowsSetNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a-one"), Value: []byte("1")}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("b-two"), Value: []byte("2")}))

	item := mustGet(t, client, ctx, []byte("a-one"))
	equal(t, []byte("1"), item.Value)

	item = mustGet(t, client, ctx, []byte("b-two"))
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

	_, err = client.Get(context.Background(), GetOpts{}, []byte("k"))
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(2), dials.Load())
}

func TestGetPartialNodeFailure(t *testing.T) {
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
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
		cfg.Failover = Failover{}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a-good"), Value: []byte("v")}))
	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("a-good"), []byte("b-dead"))
	isError(t, err)
	got := itemsByKey(items)
	notNil(t, got["a-good"])
	equal(t, []byte("v"), got["a-good"].Value)
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
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: want}))
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, want, item.Value)

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("k"))
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, want, items[0].Value)

	got, err := client.Get(ctx, GetOpts{CAS: true}, []byte("k"))
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, want, got[0].Value)

	meta, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, want, meta.Item.Value)

	_, err = client.Version(ctx)
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestGetMissOmitsMissingKeys(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	items, err := client.Get(ctx, GetOpts{}, []byte("missing-a"), []byte("missing-b"))
	noError(t, err)
	hasLen(t, items, 0)
	items, err = client.Get(ctx, GetOpts{}, []byte("missing-a"))
	noError(t, err)
	hasLen(t, items, 0)
}
