package memcached

import (
	"context"
	"testing"
)

func TestMetaQuietStillReturnsExists(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)

	cas := uint64(999)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSet{Quiet: true, CompareCAS: &cas})
	isErrorIs(t, err, ErrExists)

	item, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaSetModeAdd(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	isErrorIs(t, err, ErrNotStored)
}

func TestMetaSetRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := client.MetaSet(context.Background(), []byte("k"), []byte("v"), MetaSet{Mode: "set\r\nquit"})
	isErrorIs(t, err, ErrInvalidArgument)
}
