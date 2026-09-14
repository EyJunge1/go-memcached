package memcached

import (
	"context"
	"testing"
)

func TestMetaQuietStillReturnsExists(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := doMetaSet(client, ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)

	cas := uint64(999)
	res, err := doMetaSet(client, ctx, []byte("k"), []byte("v2"), MetaSet{Quiet: true, CompareCAS: &cas})
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
	_, err := doMetaSet(client, ctx, []byte("k"), []byte("v"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	res, err := doMetaSet(client, ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)
}

func TestMetaMutateDestKeyContract(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	ctx := context.Background()
	key := []byte("k")
	wire := make([]byte, 0, 32)
	var dst MetaResult
	dst.Item.WireKey = wire

	noError(t, client.Meta().Set(ctx, key, []byte("v"), MetaSet{}, &dst))
	if dst.Item.Key != nil {
		t.Fatalf("Key without ReturnKey: %q", dst.Item.Key)
	}
	equal(t, 32, cap(dst.Item.WireKey))
	same(t, wire[:1], dst.Item.WireKey[:1])
	dst.Item.WireKey = append(dst.Item.WireKey, 'x')
	if dst.Item.Key != nil {
		t.Fatal("append on WireKey aliased Key")
	}

	dst.Item.WireKey = dst.Item.WireKey[:0]
	noError(t, client.Meta().Set(ctx, key, []byte("v2"), MetaSet{ReturnKey: true}, &dst))
	equal(t, key, dst.Item.Key)
	if len(dst.Item.Key) > 0 && cap(dst.Item.WireKey) > 0 &&
		&dst.Item.Key[:1][0] == &dst.Item.WireKey[:1][0] &&
		len(dst.Item.Key) != len(dst.Item.WireKey) {
		t.Fatal("Key and WireKey share a buffer at different lengths")
	}

	var del MetaResult
	del.Item.WireKey = make([]byte, 0, 16)
	noError(t, client.Meta().Delete(ctx, key, MetaDelete{}, &del))
	if del.Item.Key != nil {
		t.Fatalf("Delete Key without ReturnKey: %q", del.Item.Key)
	}
	equal(t, 16, cap(del.Item.WireKey))

	mustStore(t, client, ctx, []byte("n"), []byte("1"))
	var arith MetaResult
	arith.Item.WireKey = make([]byte, 0, 16)
	noError(t, client.Meta().Arithmetic(ctx, []byte("n"), 1, MetaArithmetic{}, &arith))
	if arith.Item.Key != nil {
		t.Fatalf("Arithmetic Key without ReturnKey: %q", arith.Item.Key)
	}
	equal(t, 16, cap(arith.Item.WireKey))
}

func TestMetaSetRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := doMetaSet(client, context.Background(), []byte("k"), []byte("v"), MetaSet{Mode: "set\r\nquit"})
	isErrorIs(t, err, ErrInvalidArgument)
}
