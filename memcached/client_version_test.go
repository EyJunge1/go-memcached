package memcached

import (
	"context"
	"net"
	"testing"
)

func TestPingAndSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })

	ctx := context.Background()
	noError(t, client.Ping(ctx))

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	servers, err := ParseServers(a.Addr() + "," + deadAddr)
	noError(t, err)
	noError(t, client.SetServers(servers))
	isError(t, client.Ping(ctx))

	servers, err = ParseServers(a.Addr() + "," + b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	noError(t, client.Ping(ctx))

	servers, err = ParseServers(a.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	noError(t, client.Ping(ctx))
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
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
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

	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
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
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}
