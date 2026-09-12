package memcached

import (
	"context"
	"testing"
)

func TestDeferredMetaBatch(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()

	d := c.Meta().Deferred(ctx)
	waitA := d.Set([]byte("da"), []byte("1"), MetaSet{})
	waitB := d.Set([]byte("db"), []byte("2"), MetaSet{})
	noError(t, d.Flush())
	ra, err := waitA()
	noError(t, err)
	equal(t, MetaStatusHD, ra.Status)
	rb, err := waitB()
	noError(t, err)
	equal(t, MetaStatusHD, rb.Status)

	d2 := c.Meta().Deferred(ctx)
	getA := d2.Get([]byte("da"), MetaGet{Return: MetaGetReturn{Value: true}})
	getB := d2.Get([]byte("db"), MetaGet{Return: MetaGetReturn{Value: true}})
	ga, err := getA()
	noError(t, err)
	equal(t, []byte("1"), ga.Item.Value)
	gb, err := getB()
	noError(t, err)
	equal(t, []byte("2"), gb.Item.Value)

	d3 := c.Meta().Deferred(ctx)
	waitN := d3.Set([]byte("dn"), []byte("10"), MetaSet{})
	waitInc := d3.Arithmetic([]byte("dn"), 5, MetaArithmetic{})
	noError(t, d3.Flush())
	rn, err := waitN()
	noError(t, err)
	equal(t, MetaStatusHD, rn.Status)
	ri, err := waitInc()
	noError(t, err)
	equal(t, MetaStatusHD, ri.Status)
	got, err := doMetaGet(ctx, c, []byte("dn"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("15"), got.Item.Value)
}

func TestDeferredMetaCopiesInputs(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()

	key := []byte("copy-key")
	val := []byte("copy-val")
	opaque := []byte("tok1")

	d := c.Meta().Deferred(ctx)
	wait := d.Set(key, val, MetaSet{Opaque: opaque})
	key[0] = 'X'
	val[0] = 'X'
	opaque[0] = 'X'

	noError(t, d.Flush())
	res, err := wait()
	noError(t, err)
	equal(t, MetaStatusHD, res.Status)

	got, err := doMetaGet(ctx, c, []byte("copy-key"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("copy-val"), got.Item.Value)
}
