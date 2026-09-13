package memcached

import (
	"context"
	"testing"
)

func TestMetaQuietHitReturnsValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("hello\nworld"))
	noError(t, err)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagNoReply())
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Value)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaGet(context.Background(), []byte("missing"), MetaGetFlagReturnValue(), MetaGetFlagNoReply())
	isErrorIs(t, err, ErrNotFound)

	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"))
	noError(t, err)
	item, err := client.MetaGet(context.Background(), []byte("k"), MetaGetFlagReturnValue())
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestMetaGetCheckCASAndPipeline(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSetFlagReturnCAS())
	noError(t, err)
	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnCAS(), MetaGetFlagReturnValue())
	noError(t, err)
	matched, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS))
	noError(t, err)
	isTrue(t, matched.CASMatched)
	isNil(t, matched.Value)

	noValue, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagCheckCAS(item.CAS))
	noError(t, err)
	isFalse(t, noValue.CASMatched)

	changed, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS+1))
	noError(t, err)
	equal(t, []byte("v"), changed.Value)
	isFalse(t, changed.CASMatched)

	noError(t, client.Set(ctx, "a", []byte("1"), 0, 0))
	noError(t, client.Set(ctx, "b", []byte("2"), 0, 0))
	items, err := client.MetaGetPipeline(ctx, []MetaGetReq{
		{Key: []byte("a"), Options: []MetaGetOption{MetaGetFlagReturnValue()}},
		{Key: []byte("b"), Options: []MetaGetOption{MetaGetFlagReturnValue()}},
	})
	noError(t, err)
	equal(t, 2, len(items))
	equal(t, []byte("1"), items[0].Value)
	equal(t, []byte("2"), items[1].Value)

	piped, err := client.MetaGetPipeline(ctx, []MetaGetReq{
		{Key: []byte("k"), Options: []MetaGetOption{MetaGetFlagReturnValue(), MetaGetFlagCheckCAS(item.CAS)}},
	})
	noError(t, err)
	hasLen(t, piped, 1)
	isTrue(t, piped[0].CASMatched)
	isNil(t, piped[0].Value)
}
