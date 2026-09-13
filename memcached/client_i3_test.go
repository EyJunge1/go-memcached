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

func TestPipelineSetUsesItemExp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	err := client.Pipeline().
		Set(&Item{Key: []byte("k"), Value: []byte("v"), Exp: Rel(30 * time.Second)}).
		Flush(context.Background())
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "set k 0 30 1")
}

func TestPipelineSetThenDelete(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	err := client.Pipeline().
		Set(&Item{Key: []byte("k"), Value: []byte("v")}).
		Delete([]byte("k")).
		Flush(ctx)
	noError(t, err)
	_, err = client.GetOne(ctx, []byte("k"))
	isErrorIs(t, err, ErrNotFound)
}

func TestPipelineAcrossNodes(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	err := client.Pipeline().
		Set(&Item{Key: []byte("a-one"), Value: []byte("1")}).
		Set(&Item{Key: []byte("b-two"), Value: []byte("2")}).
		Flush(ctx)
	noError(t, err)
	item, err := client.GetOne(ctx, []byte("a-one"))
	noError(t, err)
	equal(t, []byte("1"), item.Value)
	item, err = client.GetOne(ctx, []byte("b-two"))
	noError(t, err)
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
	_, err = client.Version(context.Background())
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
	_, err = client.Version(context.Background())
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestUnixSocketAnyModeAllowsWorldWritable(t *testing.T) {
	_, path := startFakeMemcachedUnix(t)
	noError(t, os.Chmod(path, 0o666))
	client := testClient(t, "unix://"+path, func(cfg *Config) {
		cfg.Transport.UnixSocketAnyMode = true
	})
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "fake-1.0", ver)
}

func TestPingClearsDeadServer(t *testing.T) {
	good := startFakeMemcached(t)
	var dead atomic.Bool
	dead.Store(true)
	client := testClient(t, "127.0.0.1:1,"+good.Addr(), func(cfg *Config) {
		cfg.Cluster.PickBuilder = firstAddrPickBuilder{}
		cfg.Transport.RetryAttempts = 1
		cfg.Failover = &Failover{FailureLimit: 1, DeadAfter: time.Hour, Eject: true}
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
	_, _ = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	greater(t, len(client.DeadServers()), 0)
	dead.Store(false)
	noError(t, client.Ping(context.Background()))
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
		cfg.Failover = &Failover{}
	})
	stats, err := client.StatsAll(context.Background())
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
		cfg.Cluster.PickBuilder = prefixPickBuilder{}
		cfg.Failover = &Failover{}
	})
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a-good"), []byte("v"), 0, 0)))
	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("a-good"), []byte("b-dead"))
	isError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("v"), items[0].Value)
	notZero(t, items[0].CAS)
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
	isErrorIs(t, client.Touch(context.Background(), []byte("missing"), Rel(time.Second)), ErrNotFound)
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	equal(t, int32(1), dials.Load())
}

func TestFakeGatStoresExpiry(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	exp := Rel(5 * time.Second)
	items, err := client.Get(ctx, GetOpts{Touch: &exp}, []byte("k"))
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
	noError(t, storeOK(client.Set(ctx, []byte("hit"), []byte("v"), 0, 0)))
	items, err := client.MetaPipeline(ctx, []MetaOp{
		{Op: MetaOpGet, Key: []byte("hit"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
		{Op: MetaOpGet, Key: []byte("miss"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
	})
	noError(t, err)
	hasLen(t, items, 2)
	equal(t, []byte("v"), items[0].Item.Value)
	isNil(t, items[1].Item.Value)
}

func TestMurmur3Hash2MatchesConcat(t *testing.T) {
	h := newMurmur3(42)
	a, b := []byte("tcp-127.0.0.1:112110"), []byte("user:1")
	joined := append(append([]byte{}, a...), b...)
	equal(t, h.hash(joined), h.hash2(a, b))
}
