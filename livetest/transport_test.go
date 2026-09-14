package livetest

import (
	"context"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestUDP(t *testing.T) {
	c := setupUDPClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ver, err := doVersion(c, ctx)
	noError(t, err)
	notEmpty(t, ver)
}

func TestUDPGetAndMeta(t *testing.T) {
	tcp := setupLiveClient(t)
	udp := setupUDPClient(t)
	ctx := context.Background()
	key := liveKey(t, "udp")
	mustStore(t, tcp, ctx, key, []byte("via-tcp"))

	items, err := doGet(udp, ctx, [][]byte{key}, memcached.GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("via-tcp"), items[0].Value)

	got, err := doMetaGet(udp, ctx, key, memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}})
	noError(t, err)
	noError(t, got.Err())
	equal(t, []byte("via-tcp"), got.Item.Value)

	noop, err := doMetaNoop(udp, ctx, memcached.MetaNoop{})
	noError(t, err)
	equal(t, memcached.MetaStatusMN, noop.Status)

	dbg, err := doMetaDebug(udp, ctx, key, memcached.MetaDebug{})
	noError(t, err)
	noError(t, dbg.Err())
}

func TestUDPRejectsMutation(t *testing.T) {
	udp := setupUDPClient(t)
	ctx := context.Background()
	key := liveKey(t, "mut")
	isErrorIs(t, udp.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key, Value: []byte("x")}), memcached.ErrNotSupported)

	exp := memcached.Rel(time.Second)
	_, err := doGet(udp, ctx, [][]byte{key}, memcached.GetOpts{Touch: &exp})
	isErrorIs(t, err, memcached.ErrNotSupported)

	_, err = doStatsAll(udp, ctx)
	isErrorIs(t, err, memcached.ErrNotSupported)
}

func TestUnix(t *testing.T) {
	c := setupUnixClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ver, err := doVersion(c, ctx)
	noError(t, err)
	notEmpty(t, ver)

	key := liveKey(t, "unix")
	mustStore(t, c, ctx, key, []byte("sock"))
	equal(t, []byte("sock"), mustGet(t, c, ctx, key).Value)
}
