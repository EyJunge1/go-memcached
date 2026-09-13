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
	if _, err := c.VersionAll(ctx); err != nil {
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
	if _, err := c.Version(ctx); err != nil {
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
	if _, err := c.VersionAll(ctx); err != nil {
		_ = c.Close()
		t.Skipf("no unix memcached at %s: %v", unixAddr(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func liveKey(t *testing.T, suffix string) []byte {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	return []byte(fmt.Sprintf("%s/%s/%d", name, suffix, time.Now().UnixNano()))
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

func mustGet(t *testing.T, c *memcached.Client, ctx context.Context, key []byte) memcached.Item {
	t.Helper()
	items, err := c.Get(ctx, [][]byte{key}, memcached.GetOpts{})
	noError(t, err)
	if len(items) == 0 {
		t.Fatalf("get miss for %q", key)
	}
	return items[0]
}

func mustMiss(t *testing.T, c *memcached.Client, ctx context.Context, key []byte) {
	t.Helper()
	items, err := c.Get(ctx, [][]byte{key}, memcached.GetOpts{})
	noError(t, err)
	if len(items) != 0 {
		t.Fatalf("want miss for %q, got %#v", key, items)
	}
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

func notEmpty(t *testing.T, got string) {
	t.Helper()
	if got == "" {
		t.Fatal("want non-empty string")
	}
}

func notZero[T comparable](t *testing.T, got T) {
	t.Helper()
	var zero T
	if got == zero {
		t.Fatal("want non-zero")
	}
}
