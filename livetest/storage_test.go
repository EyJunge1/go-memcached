package livetest

import (
	"context"
	"testing"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestStoreModes(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "k")

	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreReplace, Key: key, Value: []byte("nope")}), memcached.ErrNotStored)
	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreAppend, Key: key, Value: []byte("x")}), memcached.ErrNotStored)
	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StorePrepend, Key: key, Value: []byte("x")}), memcached.ErrNotStored)

	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreAdd, Key: key, Value: []byte("mid")}))
	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreAdd, Key: key, Value: []byte("other")}), memcached.ErrNotStored)
	equal(t, []byte("mid"), mustGet(t, c, ctx, key).Value)

	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreReplace, Key: key, Value: []byte("mid"), Flags: 7}))
	item := mustGet(t, c, ctx, key)
	equal(t, []byte("mid"), item.Value)
	equal(t, uint32(7), item.Flags)

	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StorePrepend, Key: key, Value: []byte("pre")}))
	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreAppend, Key: key, Value: []byte("post")}))
	equal(t, []byte("premidpost"), mustGet(t, c, ctx, key).Value)
}

func TestStoreCAS(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "cas")
	mustStore(t, c, ctx, key, []byte("v1"))

	items, err := c.Get(ctx, [][]byte{key}, memcached.GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	cas := items[0].CAS
	notZero(t, cas)

	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreCAS, Key: key, Value: []byte("v2"), CAS: cas}))
	equal(t, []byte("v2"), mustGet(t, c, ctx, key).Value)

	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreCAS, Key: key, Value: []byte("nope"), CAS: cas}), memcached.ErrExists)
	isErrorIs(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreCAS, Key: liveKey(t, "missing"), Value: []byte("x"), CAS: 1}), memcached.ErrNotFound)
}

func TestStoreQuiet(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "q")
	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key, Value: []byte("quiet"), Quiet: true}))
	equal(t, []byte("quiet"), mustGet(t, c, ctx, key).Value)
}

func TestGetMissAndPartial(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	hit := liveKey(t, "hit")
	miss := liveKey(t, "miss")
	mustStore(t, c, ctx, hit, []byte("v"))

	mustMiss(t, c, ctx, miss)

	items, err := c.Get(ctx, [][]byte{hit, miss}, memcached.GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("v"), items[0].Value)
	equal(t, hit, items[0].Key)
}
