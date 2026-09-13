package memcached

import (
	"context"
	"net"
	"testing"
)

func TestVersionAllAndSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })

	ctx := context.Background()
	_, errVer := client.VersionAll(ctx)
	noError(t, errVer)

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	servers, err := ParseServers(a.Addr() + "," + deadAddr)
	noError(t, err)
	noError(t, client.SetServers(servers))
	_, errVer = client.VersionAll(ctx)
	isError(t, errVer)

	servers, err = ParseServers(a.Addr() + "," + b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	_, errVer = client.VersionAll(ctx)
	noError(t, errVer)

	servers, err = ParseServers(a.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	_, errVer = client.VersionAll(ctx)
	noError(t, errVer)
}

func TestSetServersRequiresPrivilege(t *testing.T) {
	a := startFakeMemcached(t)
	client := testClient(t, a.Addr())
	servers, err := ParseServers(a.Addr())
	noError(t, err)
	isErrorIs(t, client.SetServers(servers), ErrSetServersNotAllowed)
}

func TestVersionAllStatsAllAndServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
	})

	ctx := context.Background()
	servers := client.Servers()
	hasLen(t, servers, 2)
	equal(t, a.Addr(), servers[0].Address)

	vers, err := client.VersionAll(ctx)
	noError(t, err)
	hasLen(t, vers, 2)
	for _, v := range vers {
		equal(t, "fake-1.0", v)
	}

	stats, err := client.StatsAll(ctx)
	noError(t, err)
	hasLen(t, stats, 2)
	for _, st := range stats {
		notNil(t, st)
		equal(t, "fake-1.0", st["version"])
	}

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	ps := client.PoolStats()
	greater(t, len(ps), 0)
	greater(t, ps[0].Total, 0)
}

func TestUnixSocketRoundTrip(t *testing.T) {
	srv, path := startFakeMemcachedUnix(t)
	_ = srv
	client := testClient(t, "unix://"+path)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item := mustGet(t, client, context.Background(), []byte("k"))
	equal(t, []byte("v"), item.Value)
}
