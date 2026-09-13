package memcached

import (
	"context"
	"testing"
)

func TestMetaArithmeticRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := client.MetaArithmetic(context.Background(), []byte("k"), 1, MetaArithmetic{Mode: "I\r\nflush_all\r\n"})
	isErrorIs(t, err, ErrInvalidArgument)
}
