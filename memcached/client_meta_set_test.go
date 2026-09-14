package memcached

import (
	"context"
	"testing"
)

func TestMetaQuietStillReturnsExists(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.Meta().Set(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)

	cas := uint64(999)
	res, err := client.Meta().Set(ctx, []byte("k"), []byte("v2"), MetaSet{Quiet: true, CompareCAS: &cas})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrExists)

	item, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaSetModeAdd(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := client.Meta().Set(ctx, []byte("k"), []byte("v"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	res, err := client.Meta().Set(ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)
}

func TestMetaSetRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := client.Meta().Set(context.Background(), []byte("k"), []byte("v"), MetaSet{Mode: "set\r\nquit"})
	isErrorIs(t, err, ErrInvalidArgument)
}
