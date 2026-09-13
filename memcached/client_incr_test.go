package memcached

import (
	"context"
	"testing"
)

func TestIncrDecr(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("n"), []byte("10"), 0, 0)))
	got, err := client.Incr(ctx, []byte("n"), 5, MutateOpts{})
	noError(t, err)
	equal(t, uint64(15), got)

	got, err = client.Decr(ctx, []byte("n"), 3, MutateOpts{})
	noError(t, err)
	equal(t, uint64(12), got)

	_, err = client.Incr(ctx, []byte("missing"), 1, MutateOpts{})
	isErrorIs(t, err, ErrNotFound)
}
