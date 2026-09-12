package memcached

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := doMetaSet(ctx, client, []byte("k"), []byte("v"), MetaSet{Return: MetaSetReturn{CAS: true}})
	noError(t, err)
	item, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true, Value: true}})
	noError(t, err)
	cas := item.Item.CAS
	matched, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas})
	noError(t, err)
	isTrue(t, matched.Item.CASMatched)
	isNil(t, matched.Item.Value)

	noValue, err := doMetaGet(ctx, client, []byte("k"), MetaGet{CompareCAS: &cas})
	noError(t, err)
	isFalse(t, noValue.Item.CASMatched)

	wrong := cas + 1
	changed, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &wrong})
	noError(t, err)
	equal(t, []byte("v"), changed.Item.Value)
	isFalse(t, changed.Item.CASMatched)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a"), Value: []byte("1")}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("b"), Value: []byte("2")}))
	items, err := doMetaPipeline(ctx, client, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Item.Value)
	equal(t, []byte("2"), items[1].Item.Value)

	piped, err := doMetaPipeline(ctx, client, []MetaOp{
		{Key: []byte("k"), Get: &MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas}},
	})
	noError(t, err)
	hasLen(t, piped, 1)
	isTrue(t, piped[0].Item.CASMatched)
	isNil(t, piped[0].Item.Value)
}

func TestMetaPipelineReusesValueCap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("a"), []byte("v"))
	mustStore(ctx, t, client, []byte("b"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))

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
	_, err := doMetaPipeline(ctx, client, []MetaOp{{Key: []byte("k")}})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = doMetaPipeline(ctx, client, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}, Set: &MetaSet{}}})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaPipelineArithmetic(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("n"), []byte("10"))

	got, err := doMetaPipeline(ctx, client, []MetaOp{
		{Key: []byte("n"), Delta: 3, Arithmetic: &MetaArithmetic{Return: MetaArithmeticReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, []byte("13"), got[0].Item.Value)
	equal(t, []byte("0"), got[0].Item.Opaque)
	equal(t, []byte("13"), mustGet(ctx, t, client, []byte("n")).Value)
}

func TestMetaPipelineAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("a-one"), []byte("1"))
	mustStore(ctx, t, client, []byte("b-two"), []byte("2"))

	got, err := doMetaPipeline(ctx, client, []MetaOp{
		{Key: []byte("a-one"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b-two"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, got, 2)
	equal(t, []byte("1"), got[0].Item.Value)
	equal(t, []byte("2"), got[1].Item.Value)
}

func TestMetaPipelineLargeUsesIndexedOpaqueMatching(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	const count = 64
	ops := make([]MetaOp, count)
	dst := make([]MetaResult, count)
	for i := range ops {
		key := []byte("key-" + strconv.Itoa(i))
		mustStore(ctx, t, client, key, key)
		ops[i] = MetaOp{
			Key: key,
			Get: &MetaGet{Return: MetaGetReturn{Value: true}},
		}
	}

	noError(t, client.Meta().Pipeline(ctx, ops, dst))
	for i := range dst {
		equal(t, []byte("key-"+strconv.Itoa(i)), dst[i].Item.Value)
	}
}

func TestMetaPipelineIndexRejectsDuplicateOpaque(t *testing.T) {
	group := make([]preparedMetaOp, _metaPipelineIndexThreshold)
	for i := range group {
		group[i] = preparedMetaOp{hasTok: true, tokNum: uint64(i)}
	}
	group[len(group)-1].tokNum = 0

	_, err := buildMetaPipelineIndex(group)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaPipelineIndexMatchesNumericAndStringOpaque(t *testing.T) {
	group := make([]preparedMetaOp, _metaPipelineIndexThreshold)
	for i := range group {
		group[i] = preparedMetaOp{idx: i, hasTok: true, tokNum: uint64(i)}
	}
	group[3].tok = "custom"
	index, err := buildMetaPipelineIndex(group)
	noError(t, err)
	defer putMetaPipelineIndex(index)

	pos, ok := matchMetaPipelineOpaque([]byte("HD O5\r\n"), group, index)
	isTrue(t, ok)
	equal(t, 5, pos)
	pos, ok = matchMetaPipelineOpaque([]byte("HD Ocustom\r\n"), group, index)
	isTrue(t, ok)
	equal(t, 3, pos)
}

func TestMetaPipelineEmptyAndCanceled(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	got, err := doMetaPipeline(context.Background(), client, nil)
	noError(t, err)
	hasLen(t, got, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = doMetaPipeline(ctx, client, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}})
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
			_, err := doMetaPipeline(ctx, client, []MetaOp{tt.op})
			isErrorIs(t, err, ErrInvalidArgument)
		})
	}
}
