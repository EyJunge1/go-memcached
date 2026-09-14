package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetaQuietHitReturnsValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := doMetaSet(client, ctx, []byte("k"), []byte("hello\nworld"), MetaSet{})
	noError(t, err)

	item, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Item.Value)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	res, err := doMetaGet(client, context.Background(), []byte("missing"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)

	_, err = doMetaSet(client, context.Background(), []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	item, err := doMetaGet(client, context.Background(), []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := doMetaSet(client, ctx, []byte("k"), []byte("v"), MetaSet{Return: MetaSetReturn{CAS: true}})
	noError(t, err)
	item, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true, Value: true}})
	noError(t, err)
	cas := item.Item.CAS
	matched, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas})
	noError(t, err)
	isTrue(t, matched.Item.CASMatched)
	isNil(t, matched.Item.Value)

	noValue, err := doMetaGet(client, ctx, []byte("k"), MetaGet{CompareCAS: &cas})
	noError(t, err)
	isFalse(t, noValue.Item.CASMatched)

	wrong := cas + 1
	changed, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &wrong})
	noError(t, err)
	equal(t, []byte("v"), changed.Item.Value)
	isFalse(t, changed.Item.CASMatched)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a"), Value: []byte("1")}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("b"), Value: []byte("2")}))
	items, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Item.Value)
	equal(t, []byte("2"), items[1].Item.Value)

	piped, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("k"), Get: &MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas}},
	})
	noError(t, err)
	hasLen(t, piped, 1)
	isTrue(t, piped[0].Item.CASMatched)
	isNil(t, piped[0].Item.Value)
}

func TestMetaGetCheckCASReusesValueCap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := doMetaSet(client, ctx, []byte("k"), []byte("v"), MetaSet{Return: MetaSetReturn{CAS: true}})
	noError(t, err)
	var dst MetaResult
	dst.Item.Value = make([]byte, 0, 32)
	mg := MetaGet{Return: MetaGetReturn{CAS: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	cas := dst.Item.CAS
	equal(t, []byte("v"), dst.Item.Value)
	equal(t, 32, cap(dst.Item.Value))

	mg.CompareCAS = &cas
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	noError(t, dst.Err())
	isTrue(t, dst.Item.CASMatched)
	equal(t, 0, len(dst.Item.Value))
	equal(t, 32, cap(dst.Item.Value))

	wrong := cas + 1
	mg.CompareCAS = &wrong
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	equal(t, []byte("v"), dst.Item.Value)
	isFalse(t, dst.Item.CASMatched)
	equal(t, 32, cap(dst.Item.Value))
}

func TestMetaPipelineReusesValueCap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("a"), []byte("v"))
	mustStore(t, client, ctx, []byte("b"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))

	buf := make([]byte, 0, 32)
	dst := []MetaResult{{Item: MetaItem{Value: buf}}}
	ops := []MetaOp{{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}}}
	noError(t, client.Meta().Pipeline(ctx, ops, dst))
	noError(t, dst[0].Err())
	equal(t, []byte("v"), dst[0].Item.Value)
	equal(t, 32, cap(dst[0].Item.Value))
	same(t, buf[:1], dst[0].Item.Value[:1])

	ops[0].Key = []byte("b")
	noError(t, client.Meta().Pipeline(ctx, ops, dst))
	noError(t, dst[0].Err())
	equal(t, []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), dst[0].Item.Value)
	equal(t, 32, cap(dst[0].Item.Value))
	same(t, buf[:1], dst[0].Item.Value[:1])
}

func TestMetaQuietClientErrorThenMN(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_, err := doMetaGet(client, context.Background(), []byte("cerr"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	isErrorIs(t, err, ErrClientError)
	err = client.Version(context.Background(), new([]byte))
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestMetaGetReturnsWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("foo"), Value: []byte("v")}))
	got, err := doMetaGet(client, ctx, []byte("foo"), MetaGet{Return: MetaGetReturn{Key: true, Value: true}})
	noError(t, err)
	equal(t, []byte("foo"), got.Item.WireKey)
	equal(t, []byte("foo"), got.Item.Key)
	isFalse(t, got.Item.KeyBase64)

	var dst MetaResult
	dst.Item.WireKey = make([]byte, 0, 32)
	mg := MetaGet{Return: MetaGetReturn{Key: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	buf := dst.Item.WireKey
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	if cap(dst.Item.WireKey) != cap(buf) || (cap(buf) > 0 && &dst.Item.WireKey[:cap(dst.Item.WireKey)][0] != &buf[:cap(buf)][0]) {
		t.Fatal("WireKey cap was not reused")
	}
	equal(t, []byte("foo"), dst.Item.WireKey)
}

func TestMetaGetDestKeyDoesNotAliasWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("foo"), []byte("v"))
	var dst MetaResult
	dst.Item.Key = make([]byte, 0, 16)
	dst.Item.WireKey = make([]byte, 0, 16)
	mg := MetaGet{Return: MetaGetReturn{Key: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	equal(t, []byte("foo"), dst.Item.Key)
	equal(t, []byte("ns:foo"), dst.Item.WireKey)
	if sameByteArray(dst.Item.Key, dst.Item.WireKey) {
		t.Fatal("Key aliases WireKey")
	}
	dst.Item.Key = append(dst.Item.Key, 'x')
	equal(t, []byte("ns:foo"), dst.Item.WireKey)
}

func TestMetaPipelineOpaqueKeep(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	dst := []MetaResult{{Item: MetaItem{Opaque: make([]byte, 0, 16)}}}
	noError(t, client.Meta().Pipeline(context.Background(), []MetaOp{{Key: []byte("missing"), Get: &MetaGet{}}}, dst))
	equal(t, MetaStatusEN, dst[0].Status)
	equal(t, 16, cap(dst[0].Item.Opaque))
}

func TestMetaPipelineRejectsInvalidOp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := doMetaPipeline(client, ctx, []MetaOp{{Key: []byte("k")}})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = doMetaPipeline(client, ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}, Set: &MetaSet{}}})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaPipelineArithmetic(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("n"), []byte("10"))

	got, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("n"), Delta: 3, Arithmetic: &MetaArithmetic{Return: MetaArithmeticReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, []byte("13"), got[0].Item.Value)
	equal(t, []byte("0"), got[0].Item.Opaque)
	equal(t, []byte("13"), mustGet(t, client, ctx, []byte("n")).Value)
}

func TestMetaPipelineAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("a-one"), []byte("1"))
	mustStore(t, client, ctx, []byte("b-two"), []byte("2"))

	got, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("a-one"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b-two"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, got, 2)
	equal(t, []byte("1"), got[0].Item.Value)
	equal(t, []byte("2"), got[1].Item.Value)
}

func TestMetaPipelineEmptyAndCanceled(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	got, err := doMetaPipeline(client, context.Background(), nil)
	noError(t, err)
	hasLen(t, got, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = doMetaPipeline(client, ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}})
	isErrorIs(t, err, context.Canceled)
}

func TestMetaPipelineValidateRejects(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	init := uint64(1)

	tests := []struct {
		name string
		op   MetaOp
	}{
		{
			name: "autovivify-set",
			op:   MetaOp{Key: []byte("k"), Body: []byte("v"), Set: &MetaSet{AutoVivify: new(Rel(time.Second))}},
		},
		{
			name: "md-ttl",
			op:   MetaOp{Key: []byte("k"), Delete: &MetaDelete{TTL: new(Rel(time.Second))}},
		},
		{
			name: "ma-j",
			op:   MetaOp{Key: []byte("k"), Arithmetic: &MetaArithmetic{Initial: &init}},
		},
		{
			name: "hints",
			op:   MetaOp{Key: []byte("k"), Get: &MetaGet{ProxyHint: "bad token"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := doMetaPipeline(client, ctx, []MetaOp{tt.op})
			isErrorIs(t, err, ErrInvalidArgument)
		})
	}
}

func TestMetaGetVivifyRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	newCAS := uint64(42)

	res, err := doMetaGet(client, context.Background(), []byte("missing"), MetaGet{
		Vivify: new(Rel(time.Second)),
		NewCAS: &newCAS,
	})
	noError(t, err)
	equal(t, MetaStatusHD, res.Status)
	isTrue(t, res.Item.Won)
	waitLastCmdHasToken(t, srv, "N1")
	waitLastCmdHasToken(t, srv, "E42")
}
