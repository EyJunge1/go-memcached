package memcached

import (
	"context"
	"testing"
)

func TestIncrDecr(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))
	var got uint64
	noError(t, client.Incr(ctx, []byte("n"), 5, &got, ArithmeticOpts{}))
	equal(t, uint64(15), got)

	noError(t, client.Decr(ctx, []byte("n"), 3, &got, ArithmeticOpts{}))
	equal(t, uint64(12), got)

	isErrorIs(t, client.Incr(ctx, []byte("missing"), 1, &got, ArithmeticOpts{}), ErrNotFound)
	isErrorIs(t, client.Incr(ctx, []byte("n"), 1, nil, ArithmeticOpts{}), ErrInvalidArgument)
}

func TestMetaStoreGetOneAliases(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	var set MetaResult
	noError(t, client.Meta().Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}, &set))
	equal(t, MetaStatusHD, set.Status)

	var got MetaResult
	noError(t, client.Meta().GetOne(ctx, []byte("k"), &got, MetaGetValue()))
	equal(t, MetaStatusVA, got.Status)
	equal(t, []byte("v"), got.Item.Value)
}
