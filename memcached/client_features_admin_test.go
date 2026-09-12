package memcached

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFlushAllRejectsNegative(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	isErrorIs(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Stagger: -time.Second}), ErrInvalidArgument)
}

func TestFlushOptsQuietAndStagger(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{
		Stagger: time.Second,
	}))
	delays := make(map[string]struct{}, 2)
	a.mu.Lock()
	delays[a.lastFlushDelay] = struct{}{}
	a.mu.Unlock()
	b.mu.Lock()
	delays[b.lastFlushDelay] = struct{}{}
	b.mu.Unlock()
	if _, ok := delays[""]; !ok {
		t.Fatalf("node 0 delay omitted, got %v", delays)
	}
	if _, ok := delays["1"]; !ok {
		t.Fatalf("node 1 delay 1, got %v", delays)
	}

	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Quiet: true}))
	if !waitEitherLastCmdContains(t, a, b, "noreply") {
		t.Fatalf("quiet flush_all missing noreply")
	}
}

func TestFlushDelayForAddsInTimeDomain(t *testing.T) {
	fn := flushDelayFor(FlushOpts{Delay: new(Rel(10 * time.Second))})
	equal(t, Exptime(10), fn(0))
	equal(t, Exptime(10), fn(1))
	fn = flushDelayFor(FlushOpts{Stagger: time.Second})
	equal(t, ExptimeNever, fn(0))
	equal(t, Exptime(1), fn(1))
	equal(t, ExptimeNever, flushDelayFor(FlushOpts{})(0))
}

func TestAdminQuietNoreply(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	noError(t, admin.Verbosity(context.Background(), 1, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "verbosity 1 noreply")

	noError(t, admin.CacheMemLimit(context.Background(), 32, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "cache_memlimit 32 noreply")
}

func TestAuthRejectsWrongPassword(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "wrong"}
	})
	err := client.VersionAll(context.Background(), new([]NodeVersion))
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	contains(t, errText(t, err), "authentication failure")
	if strings.Contains(errText(t, err), "wrong") {
		t.Fatalf("auth error exposed password: %v", err)
	}

	_, err = doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	err = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
}

func TestSetServersRejectsFamilySwitch(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })
	udp, err := ParseServers("udp://127.0.0.1:11211")
	noError(t, err)
	isErrorIs(t, client.SetServers(udp), ErrMixedTransport)
	equal(t, srv.Addr(), client.Servers()[0].Address)
}

func TestNewRejectsResolveSpecFamilySwitch(t *testing.T) {
	_, err := New(Config{
		Servers: []Server{{Address: "127.0.0.1:11211"}},
		Cluster: Cluster{ResolveSpec: "udp://127.0.0.1:11211"},
	})
	isErrorIs(t, err, ErrMixedTransport)
}

func TestEmptyServersIsInvalidAddress(t *testing.T) {
	var empty Config
	_, err := New(empty)
	isErrorIs(t, err, ErrInvalidAddress)

	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })
	noError(t, client.SetServers(nil))
	ctx := context.Background()
	_, err = doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrInvalidAddress)
	contains(t, errText(t, err), "no servers configured")
	err = client.Pipeline().Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).Run(ctx)
	isErrorIs(t, err, ErrInvalidAddress)
	_, err = doMetaPipeline(ctx, client, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}})
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestAdminMgDumpEmptyEN(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpEmpty = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dump := mustMgDump(ctx, t, admin, "all")
	defer dump.Close()
	_, ok := <-dump.Entries
	isFalse(t, ok)
}
