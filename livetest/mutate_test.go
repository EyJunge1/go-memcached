package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestDeleteTouchIncrDecr(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "n")

	isErrorIs(t, c.Delete(ctx, key, memcached.MutateOpts{}), memcached.ErrNotFound)
	isErrorIs(t, c.Touch(ctx, key, memcached.Rel(time.Second), memcached.MutateOpts{}), memcached.ErrNotFound)
	_, err := c.Incr(ctx, key, 1)
	isErrorIs(t, err, memcached.ErrNotFound)

	mustStore(t, c, ctx, key, []byte("10"))
	n, err := c.Incr(ctx, key, 3)
	noError(t, err)
	equal(t, uint64(13), n)
	equal(t, []byte("13"), mustGet(t, c, ctx, key).Value)

	n, err = c.Decr(ctx, key, 1)
	noError(t, err)
	equal(t, uint64(12), n)

	noError(t, c.Touch(ctx, key, memcached.Rel(time.Hour), memcached.MutateOpts{}))
	equal(t, []byte("12"), mustGet(t, c, ctx, key).Value)

	noError(t, c.Delete(ctx, key, memcached.MutateOpts{Quiet: true}))
	mustMiss(t, c, ctx, key)
}

func TestGetAndTouch(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "gat")
	mustStore(t, c, ctx, key, []byte("v"))

	exp := memcached.Rel(time.Hour)
	items, err := c.Get(ctx, [][]byte{key}, memcached.GetOpts{Touch: &exp, CAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("v"), items[0].Value)
	notZero(t, items[0].CAS)
}
