package memcached

import (
	"context"
	"errors"
	"testing"
	"time"
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

func TestNodeMetaClientOps(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	meta := c.On(Server{Address: srv.Addr()}).Meta()

	var setDst MetaResult
	noError(t, meta.Set(ctx, []byte("nm-k"), []byte("v1"), &setDst, MetaSet{}))
	equal(t, MetaStatusHD, setDst.Status)

	var storeDst MetaResult
	noError(t, meta.Store(ctx, Item{Key: []byte("nm-s"), Value: []byte("sv")}, &storeDst))
	equal(t, MetaStatusHD, storeDst.Status)

	var getDst MetaResult
	noError(t, meta.Get(ctx, []byte("nm-k"), &getDst, MetaGetValue()))
	equal(t, MetaStatusVA, getDst.Status)
	equal(t, []byte("v1"), getDst.Item.Value)

	var one MetaResult
	noError(t, meta.GetOne(ctx, []byte("nm-s"), &one, MetaGetValue()))
	equal(t, MetaStatusVA, one.Status)
	equal(t, []byte("sv"), one.Item.Value)

	var dbg MetaItemDebug
	noError(t, meta.Debug(ctx, []byte("nm-k"), &dbg, MetaDebug{}))

	fills := 0
	var fillDst MetaResult
	noError(t, meta.GetOrFill(ctx, []byte("nm-fill"), &fillDst, GetOrFillOpts{Vivify: Rel(time.Minute)},
		func(context.Context, []byte) ([]byte, uint32, Exptime, error) {
			fills++
			return []byte("filled"), 1, Rel(time.Minute), nil
		}))
	equal(t, 1, fills)

	var delDst MetaResult
	noError(t, meta.Delete(ctx, []byte("nm-k"), &delDst, MetaDelete{}))
	equal(t, MetaStatusHD, delDst.Status)

	// Closed / unbound node meta returns ErrClosed.
	isErrorIs(t, (*NodeMetaClient)(nil).Get(ctx, []byte("x"), &MetaResult{}, MetaGet{}), ErrClosed)
	isErrorIs(t, (&NodeMetaClient{}).Set(ctx, []byte("x"), []byte("y"), &MetaResult{}, MetaSet{}), ErrClosed)
	isErrorIs(t, (&NodeMetaClient{}).Delete(ctx, []byte("x"), &MetaResult{}, MetaDelete{}), ErrClosed)
	isErrorIs(t, (&NodeMetaClient{}).Debug(ctx, []byte("x"), &MetaItemDebug{}, MetaDebug{}), ErrClosed)
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
