package memcached

import (
	"context"
	"testing"
)

func TestMetaQuietStillReturnsExists(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"))
	noError(t, err)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSetFlagNoReply(), MetaSetFlagCompareCAS(999))
	isErrorIs(t, err, ErrExists)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGetFlagReturnValue())
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestMetaSetModeAdd(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSetFlagModeSwitch(MetaSetModeAdd))
	noError(t, err)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSetFlagModeSwitch(MetaSetModeAdd))
	isErrorIs(t, err, ErrNotStored)
}

func TestMetaSetRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"), MetaSetFlagModeSwitch("set\r\nquit"))
	isErrorIs(t, err, ErrInvalidArgument)
}
