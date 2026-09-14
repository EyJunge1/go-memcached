package livetest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EyJunge1/go-memcached/memcached"
)

func tcpAddr() string {
	if v := os.Getenv("MEMCACHED_TCP"); v != "" {
		return v
	}
	return "127.0.0.1:11211"
}

func udpAddr() string {
	if v := os.Getenv("MEMCACHED_UDP"); v != "" {
		return v
	}
	return "udp://127.0.0.1:11211"
}

func unixAddr() string {
	if v := os.Getenv("MEMCACHED_UNIX"); v != "" {
		return v
	}
	return "unix:///tmp/memcached.sock"
}

func transport() memcached.Transport {
	return memcached.Transport{
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

func setupLiveClient(t *testing.T, tweak ...func(*memcached.Config)) *memcached.Client {
	t.Helper()
	cfg := memcached.Config{
		Servers:   []memcached.Server{{Address: tcpAddr()}},
		Transport: transport(),
	}
	for _, fn := range tweak {
		fn(&cfg)
	}
	c, err := memcached.New(cfg)
	if err != nil {
		t.Skipf("no memcached on %s: %v", tcpAddr(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.VersionAll(ctx, new([]memcached.NodeVersion)); err != nil {
		_ = c.Close()
		t.Skipf("no memcached on %s: %v", tcpAddr(), err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return c
}

func setupAdminClient(t *testing.T) *memcached.Client {
	t.Helper()
	return setupLiveClient(t, func(cfg *memcached.Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
		cfg.AllowSetServers = true
	})
}

func setupUDPClient(t *testing.T) *memcached.Client {
	t.Helper()
	servers, err := memcached.ParseServers(udpAddr())
	if err != nil {
		t.Skipf("invalid UDP addr %s: %v", udpAddr(), err)
	}
	c, err := memcached.New(memcached.Config{
		Servers:   servers,
		Transport: transport(),
	})
	if err != nil {
		t.Skipf("no UDP memcached on %s: %v", udpAddr(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.Version(ctx, new([]byte)); err != nil {
		_ = c.Close()
		t.Skipf("no UDP memcached on %s: %v", udpAddr(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func setupUnixClient(t *testing.T) *memcached.Client {
	t.Helper()
	servers, err := memcached.ParseServers(unixAddr())
	if err != nil {
		t.Skipf("invalid unix addr %s: %v", unixAddr(), err)
	}
	c, err := memcached.New(memcached.Config{
		Servers:   servers,
		Transport: transport(),
	})
	if err != nil {
		t.Skipf("no unix memcached at %s: %v", unixAddr(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.VersionAll(ctx, new([]memcached.NodeVersion)); err != nil {
		_ = c.Close()
		t.Skipf("no unix memcached at %s: %v", unixAddr(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func liveKey(t *testing.T, suffix string) []byte {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	return fmt.Appendf(nil, "%s/%s/%d", name, suffix, time.Now().UnixNano())
}

func noError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func isErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v, want errors.Is(..., %v)", err, target)
	}
}

func mustGet(t *testing.T, c *memcached.Client, ctx context.Context, key []byte) memcached.GetResult {
	t.Helper()
	got, err := doGetOne(c, ctx, key, memcached.GetOpts{})
	noError(t, err)
	if !got.Found {
		t.Fatalf("get miss for %q", key)
	}
	return got
}

func mustMiss(t *testing.T, c *memcached.Client, ctx context.Context, key []byte) {
	t.Helper()
	got, err := doGetOne(c, ctx, key, memcached.GetOpts{})
	noError(t, err)
	if got.Found {
		t.Fatalf("want miss for %q, got %#v", key, got)
	}
}

func doGetOne(c *memcached.Client, ctx context.Context, key []byte, opts memcached.GetOpts) (memcached.GetResult, error) {
	var dst memcached.GetResult
	err := c.GetOne(ctx, key, &dst, opts)
	return dst, err
}

func doVersion(c *memcached.Client, ctx context.Context) ([]byte, error) {
	var dst []byte
	err := c.Version(ctx, &dst)
	return dst, err
}

func doVersionAll(c *memcached.Client, ctx context.Context) ([]memcached.NodeVersion, error) {
	var dst []memcached.NodeVersion
	err := c.VersionAll(ctx, &dst)
	return dst, err
}

func doStats(c *memcached.Client, ctx context.Context, args ...string) ([]memcached.Stat, error) {
	var dst []memcached.Stat
	err := c.Stats(ctx, &dst, args...)
	return dst, err
}

func doStatsAll(c *memcached.Client, ctx context.Context, args ...string) ([]memcached.NodeStats, error) {
	var dst []memcached.NodeStats
	err := c.StatsAll(ctx, &dst, args...)
	return dst, err
}

func doAdminStatsDetail(a *memcached.AdminNode, ctx context.Context, mode memcached.StatsDetailMode) ([]memcached.Stat, error) {
	var dst []memcached.Stat
	err := a.StatsDetail(ctx, &dst, mode)
	return dst, err
}

func lookupStat(stats []memcached.Stat, name string) (string, bool) {
	for i := range stats {
		if string(stats[i].Name) == name {
			return string(stats[i].Value), true
		}
	}
	return "", false
}

func doGet(c *memcached.Client, ctx context.Context, keys [][]byte, opts memcached.GetOpts) ([]memcached.GetResult, error) {
	dst := make([]memcached.GetResult, len(keys))
	err := c.Get(ctx, keys, dst, opts)
	return dst, err
}

func doMetaGet(c *memcached.Client, ctx context.Context, key []byte, f memcached.MetaGet) (memcached.MetaResult, error) {
	var dst memcached.MetaResult
	err := c.Meta().Get(ctx, key, f, &dst)
	return dst, err
}

func doMetaSet(c *memcached.Client, ctx context.Context, key, value []byte, f memcached.MetaSet) (memcached.MetaResult, error) {
	var dst memcached.MetaResult
	err := c.Meta().Set(ctx, key, value, f, &dst)
	return dst, err
}

func doMetaDelete(c *memcached.Client, ctx context.Context, key []byte, f memcached.MetaDelete) (memcached.MetaResult, error) {
	var dst memcached.MetaResult
	err := c.Meta().Delete(ctx, key, f, &dst)
	return dst, err
}

func doMetaArith(c *memcached.Client, ctx context.Context, key []byte, delta uint64, f memcached.MetaArithmetic) (memcached.MetaResult, error) {
	var dst memcached.MetaResult
	err := c.Meta().Arithmetic(ctx, key, delta, f, &dst)
	return dst, err
}

func doMetaNoop(c *memcached.Client, ctx context.Context, f memcached.MetaNoop) (memcached.MetaResult, error) {
	var dst memcached.MetaResult
	err := c.Meta().Noop(ctx, f, &dst)
	return dst, err
}

func doMetaDebug(c *memcached.Client, ctx context.Context, key []byte, f memcached.MetaDebug) (memcached.MetaItemDebug, error) {
	var dst memcached.MetaItemDebug
	err := c.Meta().Debug(ctx, key, f, &dst)
	return dst, err
}

func doMetaPipeline(c *memcached.Client, ctx context.Context, ops []memcached.MetaOp) ([]memcached.MetaResult, error) {
	dst := make([]memcached.MetaResult, len(ops))
	err := c.Meta().Pipeline(ctx, ops, dst)
	return dst, err
}

func mustStore(t *testing.T, c *memcached.Client, ctx context.Context, key, value []byte) {
	t.Helper()
	noError(t, c.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key, Value: value}))
}

func equal(t *testing.T, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
}

func hasLen(t *testing.T, got any, n int) {
	t.Helper()
	if reflect.ValueOf(got).Len() != n {
		t.Fatalf("want len %d, got %d", n, reflect.ValueOf(got).Len())
	}
}

func notEmpty(t *testing.T, got []byte) {
	t.Helper()
	if len(got) == 0 {
		t.Fatal("want non-empty")
	}
}

func notZero[T comparable](t *testing.T, got T) {
	t.Helper()
	var zero T
	if got == zero {
		t.Fatal("want non-zero")
	}
}
