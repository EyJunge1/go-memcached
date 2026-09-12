package memcached

import (
	"context"
	"testing"
	"time"
)

func TestVersionAllAndSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })

	ctx := context.Background()
	errVer := client.VersionAll(ctx, new([]NodeVersion))
	noError(t, errVer)

	dead := mustListen(t, "tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	servers, err := ParseServers(a.Addr() + "," + deadAddr)
	noError(t, err)
	noError(t, client.SetServers(servers))
	errVer = client.VersionAll(ctx, new([]NodeVersion))
	isError(t, errVer)

	servers, err = ParseServers(a.Addr() + "," + b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	errVer = client.VersionAll(ctx, new([]NodeVersion))
	noError(t, errVer)

	servers, err = ParseServers(a.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	errVer = client.VersionAll(ctx, new([]NodeVersion))
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

	vers, err := doVersionAll(ctx, client)
	noError(t, err)
	hasLen(t, vers, 2)
	for _, v := range vers {
		equal(t, []byte("fake-1.0"), v.Version)
	}

	stats, err := doStatsAll(ctx, client)
	noError(t, err)
	hasLen(t, stats, 2)
	for _, st := range stats {
		v, ok := lookupStat(st.Stats, "version")
		isTrue(t, ok)
		equal(t, "fake-1.0", v)
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

	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("fake-1.0"), ver)
	equal(t, "version", lastCmdOf(a))
	if lastCmdOf(b) == "version" || lastCmdOf(c) == "version" {
		t.Fatalf("Version used picker last node, want first live")
	}

	_, err = doStats(context.Background(), client)
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

	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("fake-1.0"), ver)
	equal(t, "version", lastCmdOf(b))
	if lastCmdOf(c) == "version" {
		t.Fatalf("Version retried via picker last node")
	}

	_, err = doStats(context.Background(), client, "items")
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

	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("fake-1.0"), ver)
	equal(t, "version", lastCmdOf(good))
	if lastCmdOf(last) == "version" {
		t.Fatalf("retry used last picker, want first remaining live")
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

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "version", run: func() error { return client.Version(context.Background(), new([]byte)) }},
		{name: "stats", run: func() error { _, err := doStats(context.Background(), client); return err }},
		{name: "store", run: func() error {
			return client.Store(context.Background(), Item{Key: []byte("k"), Value: []byte("v")})
		}},
		{name: "admin", run: func() error { _, err := client.Admin(); return err }},
		{name: "set servers", run: func() error { return client.SetServers(client.Servers()) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isErrorIs(t, tt.run(), ErrClosed)
		})
	}
}

func TestVersionStatsDestContract(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "version", run: func() error { return client.Version(ctx, nil) }},
		{name: "version all", run: func() error { return client.VersionAll(ctx, nil) }},
		{name: "stats", run: func() error { return client.Stats(ctx, nil) }},
		{name: "stats all", run: func() error { return client.StatsAll(ctx, nil) }},
		{name: "stats sizes", run: func() error { return mustAdminOn(t, client).StatsSizes(ctx, nil) }},
		{name: "stats detail", run: func() error {
			return mustAdminOn(t, client).StatsDetail(ctx, nil, StatsDetailOn)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isErrorIs(t, tt.run(), ErrInvalidArgument)
		})
	}

	ver := make([]byte, 0, 32)
	noError(t, client.Version(ctx, &ver))
	equal(t, []byte("fake-1.0"), ver)
	equal(t, 32, cap(ver))

	all := make([]NodeVersion, 0, 1)
	all = append(all, NodeVersion{Version: make([]byte, 0, 32)})
	noError(t, client.VersionAll(ctx, &all))
	hasLen(t, all, 1)
	equal(t, []byte("fake-1.0"), all[0].Version)
	equal(t, 32, cap(all[0].Version))

	stats := []Stat{{Name: make([]byte, 0, 16), Value: make([]byte, 0, 16)}}
	noError(t, client.Stats(ctx, &stats))
	v, ok := lookupStat(stats, "version")
	isTrue(t, ok)
	equal(t, "fake-1.0", v)
	equal(t, 16, cap(stats[0].Name))
}

func TestUnixSocketRoundTrip(t *testing.T) {
	srv, path := startFakeMemcachedUnix(t)
	_ = srv
	client := testClient(t, "unix://"+path)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("fake-1.0"), ver)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item := mustGet(context.Background(), t, client, []byte("k"))
	equal(t, []byte("v"), item.Value)
}
