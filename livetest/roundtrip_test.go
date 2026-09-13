package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestClassicReadCommandsRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := setupLiveClient(t)

	value := []byte("hello hello hello hello hello hello")
	flag := uint32(0x1234)
	key1 := []byte("TestClassicReadCommandsRoundTrip_1")
	key2 := []byte("TestClassicReadCommandsRoundTrip_2")

	noError(t, client.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key1, Value: value, Flags: flag}))
	noError(t, client.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key2, Value: value, Flags: flag}))

	assertItem := func(item memcached.Item) {
		equal(t, value, item.Value)
		equal(t, flag, item.Flags)
	}

	assertItem(mustGet(t, client, ctx, key1))

	items, err := client.Get(ctx, [][]byte{key1, key2}, memcached.GetOpts{CAS: true})
	noError(t, err)
	hasLen(t, items, 2)
	for _, item := range items {
		assertItem(item)
		notZero(t, item.CAS)
	}

	exp := memcached.Rel(time.Second)
	items, err = client.Get(ctx, [][]byte{key1}, memcached.GetOpts{Touch: &exp})
	noError(t, err)
	hasLen(t, items, 1)
	assertItem(items[0])

	items, err = client.Get(ctx, [][]byte{key1, key2}, memcached.GetOpts{CAS: true, Touch: &exp})
	noError(t, err)
	hasLen(t, items, 2)
	for _, item := range items {
		assertItem(item)
		notZero(t, item.CAS)
	}
}

func TestMetaReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := setupLiveClient(t)

	key := []byte("TestMetaReadRoundTrip")
	value := []byte("hello hello hello hello hello hello")
	flag := uint32(0x2345)

	stored, err := client.Meta().Set(ctx, key, value, memcached.MetaSet{Flags: flag})
	noError(t, err)
	equal(t, flag, stored.Item.Flags)

	item, err := client.Meta().Get(ctx, key, memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true, Flags: true}})
	noError(t, err)
	equal(t, value, item.Item.Value)
	equal(t, flag, item.Item.Flags)
}
