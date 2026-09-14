package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestTextPipeline(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	a := liveKey(t, "a")
	b := liveKey(t, "b")

	noError(t, c.Pipeline().
		Store(memcached.Item{Mode: memcached.StoreSet, Key: a, Value: []byte("va")}).
		Store(memcached.Item{Mode: memcached.StoreSet, Key: b, Value: []byte("vb")}).
		Touch(a, memcached.Rel(time.Hour)).
		Run(ctx))

	equal(t, []byte("va"), mustGet(t, c, ctx, a).Value)
	equal(t, []byte("vb"), mustGet(t, c, ctx, b).Value)

	noError(t, c.Pipeline().Delete(b).Run(ctx))
	mustMiss(t, c, ctx, b)

	items, err := doGet(c, ctx, [][]byte{a}, memcached.GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	noError(t, c.Pipeline().
		Store(memcached.Item{Mode: memcached.StoreCAS, Key: a, Value: []byte("va2"), CAS: items[0].CAS}).
		Run(ctx))
	equal(t, []byte("va2"), mustGet(t, c, ctx, a).Value)
}

func TestMetaPipeline(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "m")

	got, err := doMetaPipeline(c, ctx, []memcached.MetaOp{
		{Key: key, Body: []byte("v"), Set: &memcached.MetaSet{ReturnCAS: true}},
		{Key: key, Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true, Flags: true}}},
	})
	noError(t, err)
	hasLen(t, got, 2)
	noError(t, got[0].Err())
	noError(t, got[1].Err())
	equal(t, []byte("v"), got[1].Item.Value)
}
