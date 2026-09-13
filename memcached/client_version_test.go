package memcached

import (
	"context"
	"net"
	"testing"
	"time"
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
	client := testClient(t, a.Addr()+","+b.Addr())

	ctx := context.Background()
	servers := client.Servers()
	hasLen(t, servers, 2)
	equal(t, a.Addr(), servers[0].Address)

	vers, err := client.VersionAll(ctx)
	noError(t, err)
	hasLen(t, vers, 2)
	for _, v := range vers {
		equal(t, "fake-1.0", v.Version)
	}

	stats, err := client.StatsAll(ctx)
	noError(t, err)
	hasLen(t, stats, 2)
	for _, st := range stats {
		notNil(t, st.Stats)
		equal(t, "fake-1.0", st.Stats["version"])
	}

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	ps := client.PoolStats()
	greater(t, len(ps), 0)
	greater(t, ps[0].Total, 0)
}

func lastCmdOf(srv *fakeMemcached) string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.lastCmd
}

func TestVersionAndStatsHitFirstLiveNotPicker(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	c := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr()+","+c.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = lastAddrPickBuilder{}
	})

	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	equal(t, "version", lastCmdOf(a))
	if lastCmdOf(b) == "version" || lastCmdOf(c) == "version" {
		t.Fatalf("Version used picker last node, want first live")
	}

	_, err = client.Stats(context.Background())
	noError(t, err)
	contains(t, lastCmdOf(a), "stats")
}

func TestVersionAndStatsSkipDeadFirstServer(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	c := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr()+","+c.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = lastAddrPickBuilder{}
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
	})
	addrs, _ := client.snapshotCluster()
	client.noteFailure(addrs[0])
	hasLen(t, client.DeadServers(), 1)

	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	equal(t, "version", lastCmdOf(b))
	if lastCmdOf(c) == "version" {
		t.Fatalf("Version retried via picker last node")
	}

	_, err = client.Stats(context.Background(), "items")
	noError(t, err)
	contains(t, lastCmdOf(b), "stats")
}

func TestVersionRetryReevaluatesFirstLive(t *testing.T) {
	good := startFakeMemcached(t)
	last := startFakeMemcached(t)
	client := testClient(t, "127.0.0.1:1,"+good.Addr()+","+last.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = lastAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 2
		cfg.Transport.DialTimeout = 50 * time.Millisecond
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
	})

	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
	equal(t, "version", lastCmdOf(good))
	if lastCmdOf(last) == "version" {
		t.Fatalf("retry used Ketama/last picker, want first remaining live")
	}
}

func TestCloseThenOpsReturnErrClosed(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowSetServers = true
	})
	noError(t, client.Close())
	noError(t, client.Close())

	_, err := client.Version(context.Background())
	isErrorIs(t, err, ErrClosed)
	_, err = client.Stats(context.Background())
	isErrorIs(t, err, ErrClosed)
	err = client.Store(context.Background(), Item{Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrClosed)
	_, err = client.Admin()
	isErrorIs(t, err, ErrClosed)
	err = client.SetServers(client.Servers())
	isErrorIs(t, err, ErrClosed)
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
