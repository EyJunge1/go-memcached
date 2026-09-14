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
	items, err := doGet(client, ctx, keys, GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, items, 2)
	isTrue(t, items[0].Found)
	isTrue(t, items[1].Found)

	got := itemsByKey(keys, items)
	equal(t, []byte("1"), got["a-one"].Value)
	equal(t, []byte("2"), got["b-two"].Value)
	notZero(t, got["a-one"].CAS)

	exp := Rel(time.Second)
	items, err = doGet(client, ctx, keys, GetOpts{CAS: true, Touch: &exp})
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
	items, err := doGet(client, ctx, [][]byte{[]byte("b-two")}, GetOpts{Touch: &exp})
	noError(t, err)
	hasLen(t, items, 1)
	isTrue(t, items[0].Found)
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

	_, err = doGet(client, context.Background(), [][]byte{[]byte("k")}, GetOpts{})
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
	keys := [][]byte{[]byte("a-good"), []byte("b-dead")}
	items, err := doGet(client, ctx, keys, GetOpts{CAS: true})
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
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, want, item.Value)

	items, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	isTrue(t, items[0].Found)
	equal(t, want, items[0].Value)

	got, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, want, got[0].Value)

	meta, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, want, meta.Item.Value)

	_, err = client.Version(ctx)
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestGetDestinationContract(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))

	isErrorIs(t, client.Get(ctx, [][]byte{[]byte("k")}, nil, GetOpts{}), ErrInvalidArgument)
	isErrorIs(t, client.Get(ctx, [][]byte{[]byte("k")}, make([]GetResult, 2), GetOpts{}), ErrInvalidArgument)
	noError(t, client.Get(ctx, nil, nil, GetOpts{}))

	want := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	mustStore(t, client, ctx, []byte("k32"), want)
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

	mustStore(t, client, ctx, []byte("empty"), nil)
	empty, err := client.GetOne(ctx, []byte("empty"), GetOpts{})
	noError(t, err)
	isTrue(t, empty.Found)
	equal(t, 0, len(empty.Value))

	isErrorIs(t, client.Meta().Get(ctx, []byte("k"), MetaGet{}, nil), ErrInvalidArgument)
	isErrorIs(t, client.Meta().Pipeline(ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}}, nil), ErrInvalidArgument)
}

func TestGetMissIsIndexAligned(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	items, err := doGet(client, ctx, [][]byte{[]byte("missing-a"), []byte("missing-b")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 2)
	isFalse(t, items[0].Found)
	isFalse(t, items[1].Found)
	items, err = doGet(client, ctx, [][]byte{[]byte("missing-a")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
}

func TestGetStoreHotPathAllocs(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	key := []byte("alloc-key")
	val := []byte("test_value")
	mustStore(t, client, ctx, key, val)
	keys := [][]byte{key}

	reused := []GetResult{{Value: make([]byte, 0, 64)}}
	noError(t, client.Get(ctx, keys, reused, GetOpts{}))

	getReuseAllocs := testing.AllocsPerRun(200, func() {
		if err := client.Get(ctx, keys, reused, GetOpts{}); err != nil || !reused[0].Found {
			t.Fatalf("get reused: %v found=%v", err, reused[0].Found)
		}
	})
	if getReuseAllocs > 8 {
		t.Fatalf("Get reused allocs/op = %.1f, want <= 8 (includes in-process fake server)", getReuseAllocs)
	}

	getOnceAllocs := testing.AllocsPerRun(200, func() {
		dst := make([]GetResult, 1)
		if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			t.Fatalf("get one-shot: %v", err)
		}
	})
	if getOnceAllocs > 12 {
		t.Fatalf("Get one-shot allocs/op = %.1f, want <= 12 (includes in-process fake server)", getOnceAllocs)
	}

	storeAllocs := testing.AllocsPerRun(200, func() {
		if err := client.Store(ctx, Item{Key: key, Value: val}); err != nil {
			t.Fatalf("store: %v", err)
		}
	})
	if storeAllocs > 12 {
		t.Fatalf("Store allocs/op = %.1f, want <= 12 (includes in-process fake server)", storeAllocs)
	}

	meta := client.Meta()
	mg := MetaGet{Return: MetaGetReturn{Value: true}}
	_, err := meta.Set(ctx, key, val, MetaSet{})
	noError(t, err)
	var metaDst MetaResult
	metaDst.Item.Value = make([]byte, 0, 64)
	noError(t, meta.Get(ctx, key, mg, &metaDst))
	metaAllocs := testing.AllocsPerRun(200, func() {
		if _, err := meta.Set(ctx, key, val, MetaSet{}); err != nil {
			t.Fatalf("meta set: %v", err)
		}
		if err := meta.Get(ctx, key, mg, &metaDst); err != nil || metaDst.Err() != nil {
			t.Fatalf("meta get: %v %v", err, metaDst.Err())
		}
	})
	if metaAllocs > 24 {
		t.Fatalf("Meta Set+Get allocs/op = %.1f, want <= 24 (includes in-process fake server)", metaAllocs)
	}

	multiKeys := make([][]byte, 8)
	multiDst := make([]GetResult, 8)
	for i := range multiKeys {
		multiKeys[i] = []byte{byte('m'), byte('0' + i)}
		mustStore(t, client, ctx, multiKeys[i], val)
		multiDst[i].Value = make([]byte, 0, 64)
	}
	noError(t, client.Get(ctx, multiKeys, multiDst, GetOpts{}))
	multiAllocs := testing.AllocsPerRun(100, func() {
		if err := client.Get(ctx, multiKeys, multiDst, GetOpts{}); err != nil {
			t.Fatalf("multiget: %v", err)
		}
		for i := range multiDst {
			if !multiDst[i].Found {
				t.Fatalf("multiget miss at %d", i)
			}
		}
	})
	if multiAllocs > 40 {
		t.Fatalf("MultiGet8 reused allocs/op = %.1f, want <= 40 (includes in-process fake server)", multiAllocs)
	}

	pipeKeys := make([][]byte, 8)
	for i := range pipeKeys {
		pipeKeys[i] = []byte{byte('p'), byte('0' + i)}
	}
	p := client.Pipeline()
	for _, k := range pipeKeys {
		p.Store(Item{Key: k, Value: val})
	}
	noError(t, p.Run(ctx))
	pipeAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range pipeKeys {
			p.Store(Item{Key: k, Value: val})
		}
		if err := p.Run(ctx); err != nil {
			t.Fatalf("pipeline: %v", err)
		}
	})
	if pipeAllocs > 96 {
		t.Fatalf("Pipeline Store x8 allocs/op = %.1f, want <= 96 (includes in-process fake server)", pipeAllocs)
	}

	large := make([]byte, 4096)
	for i := range large {
		large[i] = 'x'
	}
	mustStore(t, client, ctx, key, large)
	reused4k := []GetResult{{Value: make([]byte, 0, 4096)}}
	noError(t, client.Get(ctx, keys, reused4k, GetOpts{}))
	get4kReuse := testing.AllocsPerRun(100, func() {
		if err := client.Get(ctx, keys, reused4k, GetOpts{}); err != nil || !reused4k[0].Found {
			t.Fatalf("get 4k reused: %v", err)
		}
	})
	get4kOnce := testing.AllocsPerRun(100, func() {
		dst := []GetResult{{Value: make([]byte, 0, 4096)}}
		if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			t.Fatalf("get 4k one-shot: %v", err)
		}
	})

	multiOnce := testing.AllocsPerRun(100, func() {
		dst := make([]GetResult, 8)
		if err := client.Get(ctx, multiKeys, dst, GetOpts{}); err != nil {
			t.Fatalf("multiget one-shot: %v", err)
		}
	})

	metaGetOnly := testing.AllocsPerRun(200, func() {
		if err := meta.Get(ctx, key, mg, &metaDst); err != nil || metaDst.Err() != nil {
			t.Fatalf("meta get: %v %v", err, metaDst.Err())
		}
	})

	t.Logf("allocs/op (fake server included): Get reused=%.1f one-shot=%.1f 4k reused=%.1f 4k one-shot=%.1f Store=%.1f Set+Get~Store+Get reused=%.1f Meta Set+Get=%.1f Meta Get reused=%.1f MultiGet8 reused=%.1f one-shot=%.1f Pipeline8=%.1f",
		getReuseAllocs, getOnceAllocs, get4kReuse, get4kOnce, storeAllocs, storeAllocs+getReuseAllocs, metaAllocs, metaGetOnly, multiAllocs, multiOnce, pipeAllocs)
}

func BenchmarkClientGetSmall(b *testing.B) {
	srv := startFakeMemcached(b)
	client := testClient(b, srv.Addr())
	ctx := context.Background()
	key := []byte("alloc-key")
	mustStore(b, client, ctx, key, []byte("test_value"))
	keys := [][]byte{key}
	dst := []GetResult{{Value: make([]byte, 0, 64)}}
	if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			b.Fatal(err)
		}
	}
}
