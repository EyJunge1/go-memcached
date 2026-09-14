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
	item := mustGet(t, client, context.Background(), []byte("k"))
	equal(t, []byte("v"), item.Value)
	ver, err := doVersion(client, context.Background())
	noError(t, err)
	equal(t, []byte("fake-udp"), ver)
}

func TestUDPRejectsMutation(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr())
	err := client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrNotSupported)
	_, err = doMetaGet(client, context.Background(), []byte("k"), MetaGet{Touch: expPtr(Rel(time.Second))})
	isErrorIs(t, err, ErrNotSupported)
	_, err = doStatsAll(client, context.Background())
	isErrorIs(t, err, ErrNotSupported)
	_, err = doStats(client, context.Background())
	isErrorIs(t, err, ErrNotSupported)
	_, err = doMetaPipeline(client, context.Background(), []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("b"), Body: []byte("v"), Set: &MetaSet{}},
	})
	isErrorIs(t, err, ErrNotSupported)
}

func TestUDPMetaGetAndVersionAllAllowed(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	got, err := doMetaGet(client, context.Background(), []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), got.Item.Value)
	errVer := client.VersionAll(context.Background(), new([]NodeVersion))
	noError(t, errVer)
}

func TestUDPMetaDebugAllowed(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	item, err := doMetaDebug(client, context.Background(), []byte("k"), MetaDebug{})
	noError(t, err)
	equal(t, MetaStatusME, item.Status)
	equal(t, []byte("k"), item.Key)
}

func TestUDPAdminBroadcastRejected(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	err := mustAdmin(t, client).All().Verbosity(context.Background(), 1, AdminOpts{})
	isErrorIs(t, err, ErrNotSupported)
}

func TestUDPLateResponseWithOldIDIgnored(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.spoofFirst = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	item := mustGet(t, client, context.Background(), []byte("k"))
	equal(t, []byte("v"), item.Value)
}

func TestUDPReservedIsMalformed(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	srv.mu.Lock()
	srv.items["k"] = []byte("v")
	srv.reserved = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr())
	_, err := doGet(client, context.Background(), [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestUDPRejectsFlushAll(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	err := mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{})
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
