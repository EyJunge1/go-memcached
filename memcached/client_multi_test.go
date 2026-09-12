package memcached

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStoreMultiDeleteMulti(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	m := c.Meta()

	storeDst := make([]MetaResult, 2)
	noError(t, m.StoreMulti(ctx, []Item{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}, storeDst))
	dst := make([]MetaResult, 2)
	noError(t, m.GetMulti(ctx, [][]byte{[]byte("a"), []byte("b")}, dst, MetaGet{}))
	equal(t, MetaStatusVA, dst[0].Status)
	equal(t, []byte("1"), dst[0].Item.Value)
	equal(t, MetaStatusVA, dst[1].Status)

	storeDst = make([]MetaResult, 2)
	noError(t, m.StoreMulti(ctx, []Item{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Value: []byte("4")},
	}, storeDst))
	equal(t, MetaStatusHD, storeDst[0].Status)
	equal(t, MetaStatusHD, storeDst[1].Status)

	delDst := make([]MetaResult, 2)
	noError(t, m.DeleteMulti(ctx, [][]byte{[]byte("a"), []byte("missing")}, DeleteMissIgnore, delDst))
	var got GetResult
	noError(t, c.GetOne(ctx, []byte("a"), &got, GetOpts{}))
	isFalse(t, got.Found)
	noError(t, c.GetOne(ctx, []byte("b"), &got, GetOpts{}))
	isTrue(t, got.Found)

	err := m.DeleteMulti(ctx, [][]byte{[]byte("gone")}, DeleteMissStrict, make([]MetaResult, 1))
	isTrue(t, errors.Is(err, ErrNotFound))

	err = m.DeleteMulti(ctx, [][]byte{[]byte("gone")}, DeleteMissMode(0), make([]MetaResult, 1))
	isTrue(t, errors.Is(err, ErrInvalidArgument))
}

func TestGetOrFill(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()

	var dst MetaResult
	fills := 0
	err := c.Meta().GetOrFill(ctx, []byte("gf"), &dst, GetOrFillOpts{
		Vivify:  Rel(time.Minute),
		Recache: 30 * time.Second,
	}, func(_ context.Context, _ []byte) ([]byte, uint32, Exptime, error) {
		fills++
		return []byte("filled"), 9, Rel(time.Minute), nil
	})
	noError(t, err)
	equal(t, 1, fills)
	var got GetResult
	noError(t, c.GetOne(ctx, []byte("gf"), &got, GetOpts{}))
	isTrue(t, got.Found)
	equal(t, []byte("filled"), got.Value)

	fills = 0
	err = c.Meta().GetOrFill(ctx, []byte("gf"), &dst, GetOrFillOpts{
		Vivify: Rel(time.Minute),
	}, func(_ context.Context, _ []byte) ([]byte, uint32, Exptime, error) {
		fills++
		return []byte("again"), 0, Rel(time.Minute), nil
	})
	noError(t, err)
	// Hit without win should not refill.
	equal(t, 0, fills)
}

func TestNodeClientPinned(t *testing.T) {
	s1 := startFakeMemcached(t)
	s2 := startFakeMemcached(t)
	c := testClient(t, s1.Addr()+","+s2.Addr())
	ctx := context.Background()
	sv := c.Servers()
	hasLen(t, sv, 2)

	n := c.On(sv[0])
	noError(t, n.Store(ctx, Item{Key: []byte("pin"), Value: []byte("v")}))
	var got GetResult
	noError(t, n.GetOne(ctx, []byte("pin"), &got, GetOpts{}))
	isTrue(t, got.Found)
	equal(t, []byte("v"), got.Value)
	noError(t, n.Touch(ctx, []byte("pin"), Rel(time.Minute), MutateOpts{}))
	noError(t, n.Delete(ctx, []byte("pin"), MutateOpts{}))
	noError(t, n.GetOne(ctx, []byte("pin"), &got, GetOpts{}))
	isFalse(t, got.Found)

	noError(t, n.Store(ctx, Item{Key: []byte("n"), Value: []byte("10")}))
	var inc, dec uint64
	noError(t, n.Incr(ctx, []byte("n"), 5, &inc, ArithmeticOpts{}))
	equal(t, uint64(15), inc)
	noError(t, n.Decr(ctx, []byte("n"), 3, &dec, ArithmeticOpts{}))
	equal(t, uint64(12), dec)

	var arith MetaResult
	noError(t, n.Meta().Arithmetic(ctx, []byte("n"), 1, &arith, MetaArithmetic{
		Return: MetaArithmeticReturn{Value: true},
	}))
	equal(t, []byte("13"), arith.Item.Value)

	var noop MetaResult
	noError(t, n.Meta().Noop(ctx, &noop, MetaNoop{}))
	equal(t, MetaStatusMN, noop.Status)

	d := n.Meta().Deferred(ctx)
	waitDef := d.Set([]byte("ndef"), []byte("1"), MetaSet{})
	noError(t, d.Flush())
	rd, err := waitDef()
	noError(t, err)
	equal(t, MetaStatusHD, rd.Status)

	isErrorIs(t, c.On(Server{Address: "127.0.0.1:9"}).Store(ctx, Item{Key: []byte("x"), Value: []byte("y")}), ErrInvalidAddress)
	isErrorIs(t, c.On(Server{Address: "127.0.0.1:9"}).Delete(ctx, []byte("x"), MutateOpts{}), ErrInvalidAddress)
	var missing uint64
	isErrorIs(t, c.On(Server{Address: "127.0.0.1:9"}).Incr(ctx, []byte("x"), 1, &missing, ArithmeticOpts{}), ErrInvalidAddress)
}

func TestApplyCacheLatencyTimeouts(t *testing.T) {
	var cfg Config
	cfg.ApplyCacheLatencyTimeouts()
	equal(t, 100*time.Millisecond, cfg.Transport.DialTimeout)
	equal(t, 100*time.Millisecond, cfg.Transport.ReadTimeout)
	equal(t, 100*time.Millisecond, cfg.Transport.WriteTimeout)
}
