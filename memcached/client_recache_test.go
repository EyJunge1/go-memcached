package memcached

import (
	"context"
	"testing"
	"time"
)

func TestGetOrVivifyWonOnMiss(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	res, won, err := client.GetOrVivify(context.Background(), []byte("k"), Rel(30*time.Second))
	noError(t, err)
	isTrue(t, won)
	isTrue(t, res.Item.Won)
}

func TestGetEarlyRecacheWonThenBusy(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))

	res, won, err := client.GetEarlyRecache(ctx, []byte("k"), Rel(5*time.Second))
	noError(t, err)
	isTrue(t, won)
	equal(t, []byte("v"), res.Item.Value)

	res, won, err = client.GetEarlyRecache(ctx, []byte("k"), Rel(5*time.Second))
	noError(t, err)
	isFalse(t, won)
	isTrue(t, res.Item.RecacheBusy)
}

func TestInvalidateMarksStale(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	noError(t, client.Invalidate(ctx, []byte("k"), Rel(10*time.Second)))

	res, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	isTrue(t, res.Item.Stale)
	equal(t, []byte("v"), res.Item.Value)
}

func TestMetaFlagCombosRejected(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	ttl := Rel(10 * time.Second)
	initial := uint64(1)

	_, err := client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{AutoVivify: &ttl})
	isErrorIs(t, err, ErrInvalidArgument)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{AutoVivify: &ttl, Mode: MetaSetModeAdd})
	isErrorIs(t, err, ErrInvalidArgument)

	_, err = client.MetaDelete(ctx, []byte("k"), MetaDelete{TTL: &ttl})
	isErrorIs(t, err, ErrInvalidArgument)

	_, err = client.MetaArithmetic(ctx, []byte("k"), 1, MetaArithmetic{Initial: &initial})
	isErrorIs(t, err, ErrInvalidArgument)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{AutoVivify: &ttl, Mode: MetaSetModeAppend})
	noError(t, err)
}

func TestMetaProxyHintsOnWire(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := client.MetaGet(ctx, []byte("missing"), MetaGet{P: "hint", L: "/path"})
	isErrorIs(t, err, ErrNotFound)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "Phint")
	contains(t, cmd, "L/path")
}

func TestMetaDebugUnknownFields(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	item, err := client.MetaDebug(ctx, []byte("k"), MetaDebug{})
	noError(t, err)
	equal(t, []byte("k"), item.Key)
	equal(t, "yes", item.Fields["extra"])
	equal(t, "", item.Fields["exp"])
}
