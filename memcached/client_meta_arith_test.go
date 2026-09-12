package memcached

import (
	"context"
	"testing"
	"time"
)

func TestMetaArithmeticRejectsInjectedMode(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	_, err := doMetaArith(context.Background(), client, []byte("k"), 1, MetaArithmetic{Mode: MetaArithmeticMode(255)})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestMetaArithmeticAutoCreate(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	init := uint64(10)
	cas := uint64(1)
	newCAS := uint64(9)
	ttl := Rel(time.Second)

	res, err := doMetaArith(context.Background(), client, []byte("n"), 2, MetaArithmetic{
		AutoCreate: new(ttl),
		Initial:    &init,
		Return:     MetaArithmeticReturn{Value: true},
		CompareCAS: &cas,
		NewCAS:     &newCAS,
		TTL:        new(ttl),
	})
	noError(t, err)
	equal(t, []byte("12"), res.Item.Value)
	waitLastCmdHasToken(t, srv, "N1")
	waitLastCmdHasToken(t, srv, "J10")
	waitLastCmdHasToken(t, srv, "C1")
	waitLastCmdHasToken(t, srv, "E9")
	waitLastCmdHasToken(t, srv, "T1")
}
