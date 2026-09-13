package memcached

import (
	"context"
	"testing"
)

func TestIncrDecr(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))
	got, err := client.Incr(ctx, []byte("n"), 5, MutateOpts{})
	noError(t, err)
	equal(t, uint64(15), got)

	got, err = client.Decr(ctx, []byte("n"), 3, MutateOpts{})
	noError(t, err)
	equal(t, uint64(12), got)

	_, err = client.Incr(ctx, []byte("missing"), 1, MutateOpts{})
	isErrorIs(t, err, ErrNotFound)
}
