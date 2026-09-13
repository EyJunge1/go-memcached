package memcached

import (
	"context"
	"testing"
)

func TestMetaArithmeticRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client, err := New(srv.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.MetaArithmetic(context.Background(), []byte("k"), 1, MetaArithmeticFlagModeSwitch("I\r\nflush_all\r\n"))
	isErrorIs(t, err, ErrInvalidArgument)
}
