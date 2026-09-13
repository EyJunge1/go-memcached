package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestMetaQuietHitReturnsValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.Meta().Set(ctx, []byte("k"), []byte("hello\nworld"), MetaSet{})
	noError(t, err)

	item, err := client.Meta().Get(ctx, []byte("k"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Item.Value)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	res, err := client.Meta().Get(context.Background(), []byte("missing"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)

	_, err = client.Meta().Set(context.Background(), []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	item, err := client.Meta().Get(context.Background(), []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.Meta().Set(ctx, []byte("k"), []byte("v"), MetaSet{ReturnCAS: true})
	noError(t, err)
	item, err := client.Meta().Get(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true, Value: true}})
	noError(t, err)
	cas := item.Item.CAS
	matched, err := client.Meta().Get(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas})
	noError(t, err)
	isTrue(t, matched.Item.CASMatched)
	isNil(t, matched.Item.Value)

	noValue, err := client.Meta().Get(ctx, []byte("k"), MetaGet{CompareCAS: &cas})
	noError(t, err)
	isFalse(t, noValue.Item.CASMatched)

	wrong := cas + 1
	changed, err := client.Meta().Get(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &wrong})
	noError(t, err)
	equal(t, []byte("v"), changed.Item.Value)
	isFalse(t, changed.Item.CASMatched)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a"), Value: []byte("1")}))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("b"), Value: []byte("2")}))
	items, err := client.Meta().Pipeline(ctx, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Item.Value)
	equal(t, []byte("2"), items[1].Item.Value)

	piped, err := client.Meta().Pipeline(ctx, []MetaOp{
		{Key: []byte("k"), Get: &MetaGet{Return: MetaGetReturn{Value: true}, CompareCAS: &cas}},
	})
	noError(t, err)
	hasLen(t, piped, 1)
	isTrue(t, piped[0].Item.CASMatched)
	isNil(t, piped[0].Item.Value)
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
	_, err := client.Meta().Get(context.Background(), []byte("cerr"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	isErrorIs(t, err, ErrClientError)
	_, err = client.Version(context.Background())
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestMetaGetReturnsWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("foo"), Value: []byte("v")}))
	got, err := client.Meta().Get(ctx, []byte("foo"), MetaGet{Return: MetaGetReturn{Key: true, Value: true}})
	noError(t, err)
	equal(t, []byte("foo"), got.Item.WireKey)
	equal(t, []byte("foo"), got.Item.Key)
	isFalse(t, got.Item.KeyBase64)
}

func TestMetaPipelineRejectsInvalidOp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := client.Meta().Pipeline(ctx, []MetaOp{{Key: []byte("k")}})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.Meta().Pipeline(ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}, Set: &MetaSet{}}})
	isErrorIs(t, err, ErrInvalidArgument)
}
