package memcached

import (
	"context"
	"testing"
)

func TestIncrDecrAndNoReply(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Set(ctx, "n", []byte("10"), 0, 0))
	got, err := client.Incr(ctx, "n", 5)
	noError(t, err)
	equal(t, uint64(15), got)

	got, err = client.Decr(ctx, "n", 3)
	noError(t, err)
	equal(t, uint64(12), got)

	_, err = client.Incr(ctx, "missing", 1)
	isErrorIs(t, err, ErrNotFound)

	quiet, err := New(srv.Addr(), WithNoReply())
	noError(t, err)
	t.Cleanup(func() { _ = quiet.Close() })
	got, err = quiet.Incr(ctx, "n", 1)
	noError(t, err)
	equal(t, uint64(0), got)
}
