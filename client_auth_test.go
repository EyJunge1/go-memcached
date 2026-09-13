package memcached

import (
	"context"
	"testing"
)

func TestOpaqueTokenRejectedAndAuth(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr(), WithAuth("u", "p"))
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaGet(context.Background(), []byte("k"), MetaGetFlagOpaqueToken("bad token"))
	isErrorIs(t, err, ErrInvalidArgument)

	plain, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	isErrorIs(t, plain.Authenticate(context.Background(), "u\r\n", "p"), ErrInvalidArgument)

	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0, NoReply()))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}
