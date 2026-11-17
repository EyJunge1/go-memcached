package memcached

import (
	"context"
	"testing"
	"time"
)

func TestFakeTextExpiryHonored(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	noError(t, client.Store(ctx, Item{
		Mode: StoreSet, Key: []byte("short"), Value: []byte("v"), Exp: Rel(time.Second),
	}))
	var hit GetResult
	noError(t, client.GetOne(ctx, []byte("short"), &hit, GetOpts{}))
	isTrue(t, hit.Found)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var got GetResult
		noError(t, client.GetOne(ctx, []byte("short"), &got, GetOpts{}))
		if !got.Found {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("item still present after TTL")
}

func TestFakeMetaTTLEchoAndExpiry(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	ttl := Rel(2 * time.Second)
	_, err := doMetaSet(ctx, client, []byte("m"), []byte("v"), MetaSet{
		TTL:    &ttl,
		Flags:  7,
		Return: MetaSetReturn{CAS: true},
	})
	noError(t, err)

	item, err := doMetaGet(ctx, client, []byte("m"), MetaGetItem())
	noError(t, err)
	noError(t, item.Err())
	equal(t, []byte("v"), item.Item.Value)
	equal(t, uint32(7), item.Item.Flags)
	isTrue(t, item.Item.CAS != 0)
	isTrue(t, item.Item.TTL > 0 && item.Item.TTL <= 2*time.Second)

	// CAS update without T must keep remaining TTL (meta semantics).
	cas := item.Item.CAS
	_, err = doMetaSet(ctx, client, []byte("m"), []byte("v2"), MetaSet{
		CompareCAS: &cas,
	})
	noError(t, err)
	kept, err := doMetaGet(ctx, client, []byte("m"), MetaGetItem())
	noError(t, err)
	noError(t, kept.Err())
	equal(t, []byte("v2"), kept.Item.Value)
	isTrue(t, kept.Item.TTL > 0)

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		miss, err := doMetaGet(ctx, client, []byte("m"), MetaGetItem())
		noError(t, err)
		if miss.Err() != nil {
			isErrorIs(t, miss.Err(), ErrNotFound)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("meta item still present after TTL")
}

func TestFakeExpiryFromToken(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	equal(t, time.Time{}, fakeExpiryFromToken(0, now))
	isTrue(t, fakeExpiryFromToken(-1, now).Before(now))
	equal(t, now.Add(30*time.Second), fakeExpiryFromToken(30, now))
	equal(t, time.Unix(1_800_000_000, 0), fakeExpiryFromToken(1_800_000_000, now))
}
