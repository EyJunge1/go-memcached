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

	keys := [][]byte{[]byte("a-one"), []byte("b-two")}
	items, err := doGet(ctx, client, keys, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, items, 2)
	isTrue(t, items[0].Found)
	isTrue(t, items[1].Found)

	got := itemsByKey(keys, items)
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	exp := Rel(time.Second)
	items, err = doGet(ctx, client, keys, GetOpts{ReturnCAS: true, Touch: &exp})
	noError(t, err)
	hasLen(t, items, 2)
	isTrue(t, items[0].Found)
	isTrue(t, items[1].Found)
}

func itemsByKey(keys [][]byte, items []GetResult) map[string]GetResult {
	out := make(map[string]GetResult, len(keys))
	for i, key := range keys {
		if i < len(items) && items[i].Found {
			out[string(key)] = items[i]
		}
	}
	return out
}

func TestGetValueContainsCRLF(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	want := []byte("hello\nworld")
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: want}))
	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, want, item.Value)
}

func TestGetValueLooksLikeEND(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("END")}))
	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, []byte("END"), item.Value)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("ERROR")}))
	item = mustGet(ctx, t, client, []byte("k2"))
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

	item := mustGet(ctx, t, client, []byte("a-one"))
	equal(t, []byte("1"), item.Value)

	item = mustGet(ctx, t, client, []byte("b-two"))
	equal(t, []byte("2"), item.Value)

	exp := Rel(time.Second)
	items, err := doGet(ctx, client, [][]byte{[]byte("b-two")}, GetOpts{Touch: &exp})
	noError(t, err)
	hasLen(t, items, 1)
	isTrue(t, items[0].Found)
	equal(t, []byte("2"), items[0].Value)
}

func TestGetMalformedValueDiscardsConnection(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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

	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(2), dials.Load())
}

func TestGetPartialNodeFailure(t *testing.T) {
	good := startFakeMemcached(t)
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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
	keys := [][]byte{[]byte("a-good"), []byte("b-dead")}
	items, err := doGet(ctx, client, keys, GetOpts{ReturnCAS: true})
	isError(t, err)
	got := itemsByKey(keys, items)
	notNil(t, got["a-good"])
	equal(t, []byte("v"), got["a-good"].Value)
	isFalse(t, items[1].Found)
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
	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, want, item.Value)

	items, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	isTrue(t, items[0].Found)
	equal(t, want, items[0].Value)

	got, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, want, got[0].Value)

	meta, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, want, meta.Item.Value)

	err = client.Version(ctx, new([]byte))
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestGetDestinationContract(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))

	isErrorIs(t, client.Get(ctx, [][]byte{[]byte("k")}, nil, GetOpts{}), ErrInvalidArgument)
	isErrorIs(t, client.Get(ctx, [][]byte{[]byte("k")}, make([]GetResult, 2), GetOpts{}), ErrInvalidArgument)
	noError(t, client.Get(ctx, nil, nil, GetOpts{}))

	want := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	mustStore(ctx, t, client, []byte("k32"), want)
	buf := make([]byte, 0, 32)
	dst := []GetResult{{Value: buf}}
	noError(t, client.Get(ctx, [][]byte{[]byte("k")}, dst, GetOpts{}))
	isTrue(t, dst[0].Found)
	equal(t, []byte("v"), dst[0].Value)
	equal(t, 32, cap(dst[0].Value))
	same(t, buf[:1], dst[0].Value[:1])

	noError(t, client.Get(ctx, [][]byte{[]byte("k32")}, dst, GetOpts{}))
	isTrue(t, dst[0].Found)
	equal(t, want, dst[0].Value)
	same(t, buf[:1], dst[0].Value[:1])
	equal(t, 32, cap(dst[0].Value))

	noError(t, client.Get(ctx, [][]byte{[]byte("missing")}, dst, GetOpts{}))
	isFalse(t, dst[0].Found)
	equal(t, 32, cap(dst[0].Value))

	mustStore(ctx, t, client, []byte("empty"), nil)
	empty, err := doGetOne(ctx, client, []byte("empty"), GetOpts{})
	noError(t, err)
	isTrue(t, empty.Found)
	equal(t, 0, len(empty.Value))

	isErrorIs(t, client.GetOne(ctx, []byte("k"), nil, GetOpts{}), ErrInvalidArgument)
	var one GetResult
	one.Value = make([]byte, 0, 32)
	noError(t, client.GetOne(ctx, []byte("k"), &one, GetOpts{}))
	isTrue(t, one.Found)
	equal(t, []byte("v"), one.Value)
	equal(t, 32, cap(one.Value))

	nsClient := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	mustStore(ctx, t, nsClient, []byte("nk"), []byte("nv"))
	var nsOne GetResult
	nsOne.Value = make([]byte, 0, 8)
	noError(t, nsClient.GetOne(ctx, []byte("nk"), &nsOne, GetOpts{}))
	isTrue(t, nsOne.Found)
	equal(t, []byte("nv"), nsOne.Value)
	equal(t, 8, cap(nsOne.Value))

	isErrorIs(t, client.Meta().Get(ctx, []byte("k"), nil, MetaGet{}), ErrInvalidArgument)
	isErrorIs(t, client.Meta().Pipeline(ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}}, nil), ErrInvalidArgument)
}

func TestGetMissIsIndexAligned(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	items, err := doGet(ctx, client, [][]byte{[]byte("missing-a"), []byte("missing-b")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 2)
	isFalse(t, items[0].Found)
	isFalse(t, items[1].Found)
	items, err = doGet(ctx, client, [][]byte{[]byte("missing-a")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
}
