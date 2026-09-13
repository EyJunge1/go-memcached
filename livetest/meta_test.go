package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestMetaSetModesAndStatus(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "k")

	res, err := c.Meta().Get(ctx, key, memcached.MetaGet{})
	noError(t, err)
	isErrorIs(t, res.Err(), memcached.ErrNotFound)

	_, err = c.Meta().Set(ctx, key, []byte("v"), memcached.MetaSet{Flags: 0x11, ReturnCAS: true})
	noError(t, err)
	got, err := c.Meta().Get(ctx, key, memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true, Flags: true, CAS: true, Key: true}})
	noError(t, err)
	noError(t, got.Err())
	equal(t, []byte("v"), got.Item.Value)
	equal(t, uint32(0x11), got.Item.Flags)
	notZero(t, got.Item.CAS)
	equal(t, key, got.Item.Key)

	res, err = c.Meta().Set(ctx, key, []byte("v2"), memcached.MetaSet{Mode: memcached.MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), memcached.ErrNotStored)

	_, err = c.Meta().Set(ctx, key, []byte("v"), memcached.MetaSet{Mode: memcached.MetaSetModeReplace, Flags: 0x11})
	noError(t, err)
	_, err = c.Meta().Set(ctx, key, []byte("X"), memcached.MetaSet{Mode: memcached.MetaSetModePrepend})
	noError(t, err)
	_, err = c.Meta().Set(ctx, key, []byte("Y"), memcached.MetaSet{Mode: memcached.MetaSetModeAppend})
	noError(t, err)
	got, err = c.Meta().Get(ctx, key, memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true, CAS: true}})
	noError(t, err)
	noError(t, got.Err())
	equal(t, []byte("XvY"), got.Item.Value)

	wrong := got.Item.CAS + 1
	res, err = c.Meta().Set(ctx, key, []byte("nope"), memcached.MetaSet{CompareCAS: &wrong})
	noError(t, err)
	isErrorIs(t, res.Err(), memcached.ErrExists)
}

func TestMetaDeleteArithmeticDebugNoop(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "n")

	res, err := c.Meta().Delete(ctx, key, memcached.MetaDelete{})
	noError(t, err)
	isErrorIs(t, res.Err(), memcached.ErrNotFound)

	dbg, err := c.Meta().Debug(ctx, key, memcached.MetaDebug{})
	noError(t, err)
	isErrorIs(t, dbg.Err(), memcached.ErrNotFound)

	ttl := memcached.Rel(time.Hour)
	initial := uint64(10)
	arith, err := c.Meta().Arithmetic(ctx, key, 2, memcached.MetaArithmetic{
		AutoCreate:  &ttl,
		Initial:     &initial,
		ReturnValue: true,
		ReturnCAS:   true,
	})
	noError(t, err)
	noError(t, arith.Err())
	equal(t, []byte("10"), arith.Item.Value)

	arith, err = c.Meta().Arithmetic(ctx, key, 2, memcached.MetaArithmetic{ReturnValue: true})
	noError(t, err)
	noError(t, arith.Err())
	equal(t, []byte("12"), arith.Item.Value)

	arith, err = c.Meta().Arithmetic(ctx, key, 1, memcached.MetaArithmetic{
		Mode:        memcached.MetaArithmeticModeDecr,
		ReturnValue: true,
	})
	noError(t, err)
	noError(t, arith.Err())
	equal(t, []byte("11"), arith.Item.Value)

	dbg, err = c.Meta().Debug(ctx, key, memcached.MetaDebug{})
	noError(t, err)
	noError(t, dbg.Err())
	equal(t, key, dbg.Key)
	notZero(t, dbg.CAS)

	noop, err := c.Meta().Noop(ctx, memcached.MetaNoop{})
	noError(t, err)
	equal(t, memcached.MetaStatusMN, noop.Status)

	_, err = c.Meta().Delete(ctx, key, memcached.MetaDelete{})
	noError(t, err)
	mustMiss(t, c, ctx, key)
}

func TestMetaVivifyRecacheInvalidate(t *testing.T) {
	c := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "r")

	vivify := memcached.Rel(time.Minute)
	res, err := c.Meta().Get(ctx, key, memcached.MetaGet{
		Vivify: &vivify,
		Return: memcached.MetaGetReturn{Value: true, CAS: true},
	})
	noError(t, err)
	noError(t, res.Err())

	mustStore(t, c, ctx, key, []byte("fresh"))
	res, err = c.Meta().Get(ctx, key, memcached.MetaGet{
		Recache: 30 * time.Second,
		Return:  memcached.MetaGetReturn{Value: true},
	})
	noError(t, err)
	noError(t, res.Err())
	equal(t, []byte("fresh"), res.Item.Value)

	ttl := memcached.Rel(time.Second)
	_, err = c.Meta().Delete(ctx, key, memcached.MetaDelete{Invalidate: true, TTL: &ttl})
	noError(t, err)
}
