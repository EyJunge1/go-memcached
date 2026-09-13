package memcached

import (
	"context"
	"testing"
	"time"
)

func TestUDPGetRoundTrip(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-udp", ver)
}

func TestUDPRejectsMutation(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr())
	_, err := client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	isErrorIs(t, err, ErrNotSupported)
	_, err = client.MetaGet(context.Background(), []byte("k"), MetaGet{Touch: expPtr(Rel(time.Second))})
	isErrorIs(t, err, ErrNotSupported)
	_, err = client.StatsAll(context.Background())
	isErrorIs(t, err, ErrNotSupported)
	_, err = client.MetaPipeline(context.Background(), []MetaOp{
		{Op: MetaOpGet, Key: []byte("a"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
		{Op: MetaOpSet, Key: []byte("b"), Body: []byte("v"), Flags: MetaSet{}},
	})
	isErrorIs(t, err, ErrNotSupported)
}

func TestUDPMetaGetAndPingAllowed(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	got, err := client.MetaGet(context.Background(), []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), got.Item.Value)
	noError(t, client.Ping(context.Background()))
}

func TestUDPAdminBroadcastRejected(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	err := mustAdmin(t, client).All().Verbosity(context.Background(), 1)
	isErrorIs(t, err, ErrNotSupported)
}

func TestUDPLateResponseWithOldIDIgnored(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.spoofFirst = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestUDPReservedIsMalformed(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.reserved = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	_, err := client.GetOne(context.Background(), []byte("k"))
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestUDPRejectsFlushAll(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	err := mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{})
	isErrorIs(t, err, ErrNotSupported)
}

func TestWatchRejectsUDP(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := mustAdminOn(t, client).Watch(ctx, WatchFetchers)
	isErrorIs(t, err, ErrNotSupported)
}

func expPtr(e Exptime) *Exptime { return &e }
