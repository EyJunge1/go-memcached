package memcached

import (
	"context"
	"net"
	"testing"
)

func TestPingAndSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	noError(t, client.Ping(ctx))

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	noError(t, client.SetServers(a.Addr()+","+deadAddr))
	isError(t, client.Ping(ctx))

	noError(t, client.SetServers(a.Addr()+","+b.Addr()))
	noError(t, client.Ping(ctx))

	noError(t, client.SetServers(a.Addr()))
	noError(t, client.Ping(ctx))
}

func TestVersionAllStatsAllAndServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client, err := New(a.Addr() + "," + b.Addr())
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

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
		equal(t, "fake-1.0", st.Version)
	}

	noError(t, client.Set(ctx, "k", []byte("v"), 0, 0))
	ps := client.PoolStats()
	greater(t, len(ps), 0)
	greater(t, ps[0].Total, 0)
}

func TestUnixSocketRoundTrip(t *testing.T) {
	srv, path := startFakeMemcachedUnix(t)
	_ = srv
	client, err := New("unix://" + path)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	noError(t, client.Set(context.Background(), "k", []byte("v"), 0, 0))
	item, err := client.Get(context.Background(), "k")
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}
