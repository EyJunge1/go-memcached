package memcached

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineStoreUsesItemExp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v"), Exp: Rel(30 * time.Second)}).
		Run(context.Background())
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "set k 0 30 1")
}

func TestPipelineStoreThenDelete(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Delete([]byte("k")).
		Run(ctx)
	noError(t, err)
	items, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
}

func TestPipelineAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("a-one"), Value: []byte("1")}).
		Store(Item{Mode: StoreSet, Key: []byte("b-two"), Value: []byte("2")}).
		Run(ctx)
	noError(t, err)
	item := mustGet(t, client, ctx, []byte("a-one"))
	equal(t, []byte("1"), item.Value)
	item = mustGet(t, client, ctx, []byte("b-two"))
	equal(t, []byte("2"), item.Value)
}

func TestNewRequireTLSWithoutTLS(t *testing.T) {
	_, err := New(Config{
		Servers:   []Server{{Address: "127.0.0.1:11211"}},
		Transport: Transport{RequireTLS: true},
	})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNewRequireTLSWithTLS(t *testing.T) {
	c, err := New(Config{
		Servers:   []Server{{Address: "127.0.0.1:11211"}},
		Transport: Transport{RequireTLS: true, TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
	})
	noError(t, err)
	_ = c.Close()
}

func TestUnixRejectsWorldWritable(t *testing.T) {
	_, path := startFakeMemcachedUnix(t)
	noError(t, os.Chmod(path, 0o666))
	client, err := New(Config{Servers: []Server{{Network: "unix", Address: path}}})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	err = client.Version(context.Background(), new([]byte))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestUnixSocketMaxModeRejectsExtraBits(t *testing.T) {
	_, path := startFakeMemcachedUnix(t)
	client, err := New(Config{
		Servers:   []Server{{Network: "unix", Address: path}},
		Transport: Transport{UnixSocketMaxMode: 0o700},
	})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	err = client.Version(context.Background(), new([]byte))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestUnixSocketAnyModeAllowsWorldWritable(t *testing.T) {
	_, path := startFakeMemcachedUnix(t)
	noError(t, os.Chmod(path, 0o666))
	client := testClient(t, "unix://"+path, func(cfg *Config) {
		cfg.Transport.UnixSocketAnyMode = true
	})
	ver, err := doVersion(client, context.Background())
	noError(t, err)
	equal(t, []byte("fake-1.0"), ver)
}

func TestVersionAllClearsDeadServer(t *testing.T) {
	good := startFakeMemcached(t)
	var dead atomic.Bool
	dead.Store(true)
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "127.0.0.1:1" && dead.Load() {
				return nil, errors.New("dead")
			}
			if address == "127.0.0.1:1" {
				address = good.Addr()
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_ = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	greater(t, len(client.DeadServers()), 0)
	dead.Store(false)
	errVer := client.VersionAll(context.Background(), new([]NodeVersion))
	noError(t, errVer)
	equal(t, 0, len(client.DeadServers()))
}

func TestStatsAllPartialNodeFailure(t *testing.T) {
	good := startFakeMemcached(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
	}()

	client := testClient(t, good.Addr()+","+ln.Addr().String(), func(cfg *Config) {
		cfg.Failover = Failover{}
	})
	stats, err := doStatsAll(client, context.Background())
	isError(t, err)
	equal(t, 1, len(stats))
}

func TestGetCASPartialNodeFailure(t *testing.T) {
	good := startFakeMemcached(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		_ = cn.Close()
	}()

	client := testClient(t, good.Addr()+","+ln.Addr().String(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
		cfg.Failover = Failover{}
	})
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a-good"), Value: []byte("v")}))
	items, err := doGet(client, ctx, [][]byte{[]byte("a-good"), []byte("b-dead")}, GetOpts{ReturnCAS: true})
	isError(t, err)
	hasLen(t, items, 2)
	isTrue(t, items[0].Found)
	equal(t, []byte("v"), items[0].Value)
	notZero(t, items[0].CAS)
	isFalse(t, items[1].Found)
}

func TestTouchNotFoundKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	isErrorIs(t, client.Touch(context.Background(), []byte("missing"), Rel(time.Second), MutateOpts{}), ErrNotFound)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(1), dials.Load())
}

func TestFakeGatStoresExpiry(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	exp := Rel(5 * time.Second)
	items, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{Touch: &exp})
	noError(t, err)
	hasLen(t, items, 1)
	srv.mu.Lock()
	got := srv.items["k"].exptime
	cmd := srv.lastCmd
	srv.mu.Unlock()
	equal(t, "5", got)
	contains(t, cmd, "gat 5 k")
}

func TestMetaPipelineMissIsEmptyItem(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("hit"), Value: []byte("v")}))
	items, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("hit"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Key: []byte("miss"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, items, 2)
	equal(t, []byte("v"), items[0].Item.Value)
	equal(t, MetaStatusEN, items[1].Status)
	isErrorIs(t, items[1].Err(), ErrNotFound)
	isNil(t, items[1].Item.Value)
}
