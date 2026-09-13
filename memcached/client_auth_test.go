package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestOpaqueTokenRejectedAndAuth(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})

	_, err := client.MetaGet(context.Background(), []byte("k"), MetaGet{Opaque: "bad token"})
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
	equal(t, []byte("v"), item.Value)
}

func TestAuthDoesNotLeaveCredentialItem(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})
	err := client.Ping(context.Background())
	isErrorIs(t, err, ErrInvalidArgument)
	srv.mu.Lock()
	line := srv.lastSet
	n := srv.authSets
	srv.mu.Unlock()
	contains(t, line, "set __mc_auth 0 0")
	greater(t, n, 0)

	plain := testClient(t, srv.Addr())
	_, err = plain.GetOne(context.Background(), authProbeKey)
	isErrorIs(t, err, ErrNotFound)
}

func TestAuthLeavesUnrelatedProbeKey(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	srv.mu.Lock()
	srv.items[string(authProbeKey)] = &fakeItem{value: []byte("app-data")}
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})
	noError(t, client.Ping(context.Background()))
	item, err := client.GetOne(context.Background(), authProbeKey)
	noError(t, err)
	equal(t, []byte("app-data"), item.Value)
}

func TestAuthHitsEveryNode(t *testing.T) {
	a := startFakeMemcachedAuth(t)
	b := startFakeMemcachedAuth(t)
	var dials atomic.Int32
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	noError(t, client.Ping(context.Background()))
	equal(t, int32(2), dials.Load())
	a.mu.Lock()
	aSets := a.authSets
	a.mu.Unlock()
	b.mu.Lock()
	bSets := b.authSets
	b.mu.Unlock()
	equal(t, 1, aSets)
	equal(t, 1, bSets)
}

func TestAuthRejectsWhitespaceUser(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Auth = &Auth{User: "u p", Pass: "p"}
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNewRejectsAuthOnUDP(t *testing.T) {
	cfg, err := testConfig("udp://127.0.0.1:11211")
	noError(t, err)
	cfg.Auth = &Auth{User: "u", Pass: "p", AllowPlaintext: true}
	_, err = New(cfg)
	isErrorIs(t, err, ErrNotSupported)
}

func TestNewRejectsEmptyAuthUser(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Auth = &Auth{User: "", Pass: "p"}
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNewRejectsPlaintextAuthRemote(t *testing.T) {
	cfg, err := testConfig("10.1.2.3:11211")
	noError(t, err)
	cfg.Auth = &Auth{User: "u", Pass: "p"}
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestAuthAllowsUnix(t *testing.T) {
	srv, path := startFakeMemcachedUnix(t)
	srv.tokenAuth = true
	srv.authUser = "u"
	srv.authPass = "p"
	client := testClient(t, "unix://"+path, func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})
	noError(t, client.Ping(context.Background()))
}

func TestSetServersRejectsAuthBypass(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
		cfg.AllowSetServers = true
	})

	remote, err := ParseServers("10.1.2.3:11211")
	noError(t, err)
	isErrorIs(t, client.SetServers(remote), ErrInvalidArgument)
	equal(t, srv.Addr(), client.Servers()[0].Address)

	udp, err := ParseServers("udp://127.0.0.1:11211")
	noError(t, err)
	isErrorIs(t, client.SetServers(udp), ErrNotSupported)
	equal(t, srv.Addr(), client.Servers()[0].Address)
}
