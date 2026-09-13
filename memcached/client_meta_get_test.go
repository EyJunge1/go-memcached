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
	_, err := client.MetaSet(ctx, []byte("k"), []byte("hello\nworld"), MetaSet{})
	noError(t, err)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Item.Value)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := client.MetaGet(context.Background(), []byte("missing"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	isErrorIs(t, err, ErrNotFound)

	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	item, err := client.MetaGet(context.Background(), []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{ReturnCAS: true})
	noError(t, err)
	item, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true, Value: true}})
	noError(t, err)
	cas := item.Item.CAS
	matched, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CheckCAS: &cas})
	noError(t, err)
	isTrue(t, matched.Item.CASMatched)
	isNil(t, matched.Item.Value)

	noValue, err := client.MetaGet(ctx, []byte("k"), MetaGet{CheckCAS: &cas})
	noError(t, err)
	isFalse(t, noValue.Item.CASMatched)

	wrong := cas + 1
	changed, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}, CheckCAS: &wrong})
	noError(t, err)
	equal(t, []byte("v"), changed.Item.Value)
	isFalse(t, changed.Item.CASMatched)

	noError(t, storeOK(client.Set(ctx, []byte("a"), []byte("1"), 0, 0)))
	noError(t, storeOK(client.Set(ctx, []byte("b"), []byte("2"), 0, 0)))
	items, err := client.MetaPipeline(ctx, []MetaOp{
		{Op: MetaOpGet, Key: []byte("a"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
		{Op: MetaOpGet, Key: []byte("b"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Item.Value)
	equal(t, []byte("2"), items[1].Item.Value)

	piped, err := client.MetaPipeline(ctx, []MetaOp{
		{Op: MetaOpGet, Key: []byte("k"), Flags: MetaGet{Return: MetaGetReturn{Value: true}, CheckCAS: &cas}},
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
	res, err := client.MetaGet(context.Background(), []byte("cerr"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	isErrorIs(t, err, ErrClientError)
	isErrorIs(t, res.Err(), ErrClientError)
	_, err = client.Version(context.Background())
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestMetaGetReturnsWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("foo"), []byte("v"), 0, 0)))
	got, err := client.MetaGet(ctx, []byte("foo"), MetaGet{Return: MetaGetReturn{Key: true, Value: true}})
	noError(t, err)
	equal(t, []byte("foo"), got.WireKey)
	equal(t, []byte("foo"), got.Item.Key)
	isFalse(t, got.KeyBase64)
}
