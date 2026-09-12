package memcached

import (
	"context"
	"errors"
	"testing"
)

func TestNodeClientGetStoreMulti(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	srvServer := Server{Address: srv.Addr()}
	n := c.On(srvServer)

	noError(t, n.Store(ctx, Item{Key: []byte("nk"), Value: []byte("v")}))
	dst := make([]GetResult, 1)
	noError(t, n.Get(ctx, [][]byte{[]byte("nk")}, dst, GetOpts{}))
	isTrue(t, dst[0].Found)
	equal(t, []byte("v"), dst[0].Value)

	storeDst := make([]MetaResult, 2)
	noError(t, n.Meta().StoreMulti(ctx, []Item{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}, storeDst))
	mdst := make([]MetaResult, 2)
	noError(t, n.Meta().GetMulti(ctx, [][]byte{[]byte("a"), []byte("b")}, mdst, MetaGet{}))
	equal(t, MetaStatusVA, mdst[0].Status)
	equal(t, MetaStatusVA, mdst[1].Status)

	noError(t, n.Meta().DeleteMulti(ctx, [][]byte{[]byte("a"), []byte("b")}, DeleteMissIgnore, make([]MetaResult, 2)))
}

func TestNodeMetaPreparePipelinePinsNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr())
	ctx := context.Background()
	pin := Server{Address: a.Addr()}
	ops := []MetaOp{
		{Key: []byte("pin-a"), Body: []byte("1"), Set: &MetaSet{}},
		{Key: []byte("pin-b"), Body: []byte("2"), Set: &MetaSet{}},
		{Key: []byte("pin-a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	}
	dst := make([]MetaResult, len(ops))
	batch, err := client.On(pin).Meta().PreparePipeline(ops, dst)
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })

	noError(t, batch.Run(ctx))
	equal(t, MetaStatusHD, dst[0].Status)
	equal(t, MetaStatusHD, dst[1].Status)
	equal(t, MetaStatusVA, dst[2].Status)
	equal(t, []byte("1"), dst[2].Item.Value)

	// All keys land on the pinned node even when hashing would fan out.
	var got GetResult
	noError(t, client.On(pin).GetOne(ctx, []byte("pin-a"), &got, GetOpts{}))
	equal(t, []byte("1"), got.Value)
	noError(t, client.On(pin).GetOne(ctx, []byte("pin-b"), &got, GetOpts{}))
	equal(t, []byte("2"), got.Value)
}

func TestAdminRefreshCertsWire(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
	})
	admin, err := c.Admin()
	noError(t, err)
	// Fake server may not implement refresh_certs; accept OK or protocol ERROR.
	err = admin.On(Server{Address: srv.Addr()}).RefreshCerts(context.Background())
	if err != nil && !errors.Is(err, ErrNonexistentCommand) && !errors.Is(err, ErrCommandError) {
		t.Fatalf("unexpected refresh_certs error: %v", err)
	}
}
