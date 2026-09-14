package memcached

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
)

func Test_newAuthProbeKey(t *testing.T) {
	a, err := newAuthProbeKey()
	noError(t, err)
	b, err := newAuthProbeKey()
	noError(t, err)
	isTrue(t, isAuthProbeKey(string(a)))
	isTrue(t, isAuthProbeKey(string(b)))
	if string(a) == string(b) {
		t.Fatalf("expected unique probe keys, got %q", a)
	}
	isFalse(t, isAuthProbeKey("__mc_auth"))
}

func Test_newAuthProbeKeyRequiresEntropy(t *testing.T) {
	orig := readAuthRand
	readAuthRand = func([]byte) (int, error) { return 0, errors.New("no rand") }
	t.Cleanup(func() { readAuthRand = orig })

	_, err := newAuthProbeKey()
	isError(t, err)
}

func TestValidateAuthPeerRequiresActualLoopback(t *testing.T) {
	rt := &runtime{}
	node := newAddrWeighted("tcp", "localhost:11211", 0)
	loopback := &conn{addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 11211}}
	noError(t, validateAuthPeer(loopback, node, rt))

	remote := &conn{addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 11211}}
	isErrorIs(t, validateAuthPeer(remote, node, rt), ErrInvalidArgument)

	rt.customDial = true
	noError(t, validateAuthPeer(remote, node, rt))
}

func TestOpaqueTokenRejectedAndAuth(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})

	_, err := doMetaGet(client, context.Background(), []byte("k"), MetaGet{Opaque: []byte("bad token")})
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item := mustGet(t, client, context.Background(), []byte("k"))
	equal(t, []byte("v"), item.Value)
}

func TestAuthDoesNotLeaveCredentialItem(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})
	err := client.VersionAll(context.Background(), new([]NodeVersion))
	isErrorIs(t, err, ErrInvalidArgument)
	srv.mu.Lock()
	line := srv.lastSet
	n := srv.authSets
	srv.mu.Unlock()
	contains(t, line, "set __mc_auth_")
	greater(t, n, 0)

	plain := testClient(t, srv.Addr())
	srv.mu.Lock()
	for k := range srv.items {
		if isAuthProbeKey(k) {
			srv.mu.Unlock()
			t.Fatalf("leftover probe key %q", k)
		}
	}
	srv.mu.Unlock()
	items, err := doGet(plain, context.Background(), [][]byte{[]byte("__mc_auth")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
}

func TestAuthLeavesUnrelatedProbeKey(t *testing.T) {
	appKey := []byte("__mc_auth")
	srv := startFakeMemcachedAuth(t)
	srv.mu.Lock()
	srv.items[string(appKey)] = &fakeItem{value: []byte("app-data")}
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
	})
	errVer := client.VersionAll(context.Background(), new([]NodeVersion))
	noError(t, errVer)
	item := mustGet(t, client, context.Background(), appKey)
	equal(t, []byte("app-data"), item.Value)
}

func TestAuthHitsEveryNode(t *testing.T) {
	a := startFakeMemcachedAuth(t)
	b := startFakeMemcachedAuth(t)
	var dials atomic.Int32
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "p"}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	errVer := client.VersionAll(context.Background(), new([]NodeVersion))
	noError(t, errVer)
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
	errVer := client.VersionAll(context.Background(), new([]NodeVersion))
	noError(t, errVer)
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
	isErrorIs(t, client.SetServers(udp), ErrMixedTransport)
	equal(t, srv.Addr(), client.Servers()[0].Address)
}
