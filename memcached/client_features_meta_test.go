package memcached

import (
	"context"
	"testing"
	"time"
)

func TestMetaSingleCommandStatusCodes(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	res, err := doMetaGet(ctx, client, []byte("missing"), MetaGet{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusEN, res.Status)

	_, err = doMetaSet(ctx, client, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	res, err = doMetaSet(ctx, client, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)
	equal(t, MetaStatusNS, res.Status)

	hit, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true}})
	noError(t, err)
	cas := hit.Item.CAS
	wrong := cas + 1
	res, err = doMetaSet(ctx, client, []byte("k"), []byte("v3"), MetaSet{CompareCAS: &wrong})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrExists)
	equal(t, MetaStatusEX, res.Status)

	res, err = doMetaDelete(ctx, client, []byte("nope"), MetaDelete{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusNF, res.Status)
}

func TestMetaHintsRejectWhitespace(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := doMetaGet(context.Background(), client, []byte("k"), MetaGet{ProxyHint: "bad token"})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = doMetaNoop(context.Background(), client, MetaNoop{ProxyPath: "x y"})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaNoopStatusAndProxyHints(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := doMetaNoop(context.Background(), client, MetaNoop{})
	noError(t, err)
	equal(t, MetaStatusMN, res.Status)

	res, err = doMetaNoop(context.Background(), client, MetaNoop{ProxyHint: "hint", ProxyPath: "/path"})
	noError(t, err)
	equal(t, MetaStatusMN, res.Status)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "Phint")
	contains(t, cmd, "L/path")
}

func TestMetaDebugStatus(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	item, err := doMetaDebug(ctx, client, []byte("missing"), MetaDebug{})
	noError(t, err)
	isErrorIs(t, item.Err(), ErrNotFound)
	equal(t, MetaStatusEN, item.Status)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item, err = doMetaDebug(ctx, client, []byte("k"), MetaDebug{})
	noError(t, err)
	equal(t, MetaStatusME, item.Status)

	bin := []byte("bin-key")
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: bin, Value: []byte("v")}))
	item, err = doMetaDebug(ctx, client, bin, MetaDebug{BinaryKey: true})
	noError(t, err)
	equal(t, bin, item.Key)
	isTrue(t, item.KeyBase64)
	equal(t, base64Encode(bin), item.WireKey)
}

func TestMetaDebugDestContract(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))

	wire := make([]byte, 0, 32)
	dst := MetaItemDebug{WireKey: wire}
	noError(t, client.Meta().Debug(ctx, []byte("k"), &dst, MetaDebug{}))
	equal(t, MetaStatusME, dst.Status)
	equal(t, []byte("k"), dst.Key)
	equal(t, []byte("ns:k"), dst.WireKey)
	equal(t, 32, cap(dst.WireKey))
	same(t, wire[:1], dst.WireKey[:1])
	if sameByteArray(dst.Key, dst.WireKey) {
		t.Fatal("Key aliases WireKey")
	}

	isErrorIs(t, client.Meta().Debug(ctx, []byte("k"), nil, MetaDebug{}), ErrInvalidArgument)
}

func TestMetaGetRecacheWindow(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Recache: -time.Second})
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	_, err = doMetaGet(ctx, client, []byte("k"), MetaGet{Recache: 31 * 24 * time.Hour, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "R2678400")

	_, err = doMetaGet(ctx, client, []byte("k"), MetaGet{Recache: 5 * time.Second, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	srv.mu.Lock()
	cmd = srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "R5")
}

func TestMetaArithmeticQuietReturnValueIsRejected(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))
	_, err := doMetaArith(ctx, client, []byte("n"), 1, MetaArithmetic{Quiet: true, Return: MetaArithmeticReturn{Value: true}})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaDeleteMissAndQuietNS(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	res, err := doMetaDelete(ctx, client, []byte("missing"), MetaDelete{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	res, err = doMetaDelete(ctx, client, []byte("missing"), MetaDelete{Quiet: true})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)

	_, err = doMetaSet(ctx, client, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	res, err = doMetaSet(ctx, client, []byte("k"), []byte("v2"), MetaSet{Quiet: true, Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)

	res, err = doMetaArith(ctx, client, []byte("nope"), 1, MetaArithmetic{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
}

func TestMetaGetMissKeepsENFlags(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := doMetaGet(context.Background(), client, []byte("missing"), MetaGet{Return: MetaGetReturn{Key: true}})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusEN, res.Status)
}

func TestMetaDeleteInvalidateTTL(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))
	cas := uint64(1)
	newCAS := uint64(2)

	_, err := doMetaDelete(ctx, client, []byte("k"), MetaDelete{
		Invalidate: true,
		TTL:        new(Rel(5 * time.Second)),
		CompareCAS: &cas,
		NewCAS:     &newCAS,
	})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "I")
	waitLastCmdHasToken(t, srv, "T5")
	waitLastCmdHasToken(t, srv, "C1")
	waitLastCmdHasToken(t, srv, "E2")

	got, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), got.Item.Value)
	isTrue(t, got.Item.Stale)
}

func TestMetaDeleteRemoveValueOnlyWire(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))

	_, err := doMetaDelete(ctx, client, []byte("k"), MetaDelete{RemoveValueOnly: true})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "x")
}

func TestMetaSetAutoVivifyAppend(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))

	_, err := doMetaSet(ctx, client, []byte("k"), []byte("x"), MetaSet{
		Mode:       MetaSetModeAppend,
		AutoVivify: new(Rel(time.Second)),
	})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "MA")
	waitLastCmdHasToken(t, srv, "N1")
}
