package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	ours "github.com/EyJunge1/go-memcached/memcached"
	"github.com/bradfitz/gomemcache/memcache"
	rainycape "github.com/rainycape/memcache"
	yeqown "github.com/yeqown/memcached"
)

var (
	smallValue = []byte("test_value")
	largeValue = make([]byte, 4096)
	errMiss    = errors.New("cache miss")
)

type payload struct {
	name string
	val  []byte
}

func payloadSizes() []payload {
	return []payload{
		{name: "small", val: smallValue},
		{name: "4k", val: largeValue},
	}
}

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

func benchKey(name string, n uint64) string {
	return fmt.Sprintf("b/%s/%d", name, n)
}

func requireTCP(tb testing.TB) {
	tb.Helper()
	c := newOursTCP(tb)
	_ = c
}

func requireUDP(tb testing.TB) {
	tb.Helper()
	c := newOursUDP(tb)
	_ = c
}

func oursTransport() ours.Transport {
	return ours.Transport{
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

func newOursTCP(tb testing.TB) *ours.Client {
	tb.Helper()
	c, err := ours.New(ours.Config{
		Servers:   []ours.Server{{Address: tcpAddr()}},
		Transport: oursTransport(),
	})
	if err != nil {
		tb.Skipf("no memcached on %s: %v", tcpAddr(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Version(ctx); err != nil {
		_ = c.Close()
		tb.Skipf("no memcached on %s: %v", tcpAddr(), err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

func newOursUDP(tb testing.TB) *ours.Client {
	tb.Helper()
	servers, err := ours.ParseServers(udpAddr())
	if err != nil {
		tb.Skipf("invalid UDP addr %s: %v", udpAddr(), err)
	}
	c, err := ours.New(ours.Config{
		Servers:   servers,
		Transport: oursTransport(),
	})
	if err != nil {
		tb.Skipf("no UDP memcached on %s: %v", udpAddr(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Version(ctx); err != nil {
		_ = c.Close()
		tb.Skipf("no UDP memcached on %s: %v", udpAddr(), err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

func newYeqown(tb testing.TB, addr string) yeqown.Client {
	tb.Helper()
	c, err := yeqown.New(addr)
	if err != nil {
		tb.Skipf("yeqown: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Version(ctx); err != nil {
		_ = c.Close()
		tb.Skipf("yeqown no memcached on %s: %v", addr, err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

func newBradfitz(tb testing.TB) *memcache.Client {
	tb.Helper()
	c := memcache.New(tcpAddr())
	c.Timeout = 3 * time.Second
	c.MaxIdleConns = 10
	if err := c.Ping(); err != nil {
		tb.Skipf("bradfitz no memcached on %s: %v", tcpAddr(), err)
	}
	return c
}

func newRainycape(tb testing.TB) *rainycape.Client {
	tb.Helper()
	c, err := rainycape.New(tcpAddr())
	if err != nil {
		tb.Skipf("rainycape: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	if err := c.Set(&rainycape.Item{Key: "b/rainycape/probe", Value: []byte("v")}); err != nil {
		tb.Skipf("rainycape binary not available: %v", err)
	}
	return c
}

func seedKeys(tb testing.TB, value []byte, n int) []string {
	tb.Helper()
	c := newOursTCP(tb)
	ctx := context.Background()
	keys := make([]string, n)
	prefix := benchKey(tb.Name(), uint64(time.Now().UnixNano()))
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/%d", prefix, i)
		if err := c.Store(ctx, ours.Item{Key: []byte(keys[i]), Value: value}); err != nil {
			tb.Fatalf("seed %s: %v", keys[i], err)
		}
	}
	return keys
}

func asKeyBytes(keys []string) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = []byte(k)
	}
	return out
}

type setGetter interface {
	set(ctx context.Context, key string, value []byte) error
	get(ctx context.Context, key string) error
}

type oursSG struct{ c *ours.Client }

func (s oursSG) set(ctx context.Context, key string, value []byte) error {
	return s.c.Store(ctx, ours.Item{Key: []byte(key), Value: value})
}

func (s oursSG) get(ctx context.Context, key string) error {
	items, err := s.c.Get(ctx, [][]byte{[]byte(key)}, ours.GetOpts{})
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return errMiss
	}
	return nil
}

type yeqownSG struct{ c yeqown.Client }

func (s yeqownSG) set(ctx context.Context, key string, value []byte) error {
	return s.c.Set(ctx, key, value, 0, 0)
}

func (s yeqownSG) get(ctx context.Context, key string) error {
	_, err := s.c.Get(ctx, key)
	return err
}

type bradfitzSG struct{ c *memcache.Client }

func (s bradfitzSG) set(_ context.Context, key string, value []byte) error {
	return s.c.Set(&memcache.Item{Key: key, Value: value})
}

func (s bradfitzSG) get(_ context.Context, key string) error {
	_, err := s.c.Get(key)
	return err
}

type rainycapeSG struct{ c *rainycape.Client }

func (s rainycapeSG) set(_ context.Context, key string, value []byte) error {
	return s.c.Set(&rainycape.Item{Key: key, Value: value})
}

func (s rainycapeSG) get(_ context.Context, key string) error {
	_, err := s.c.Get(key)
	return err
}

type tcpFactory struct {
	name string
	open func(tb testing.TB) setGetter
}

func tcpFactories() []tcpFactory {
	return []tcpFactory{
		{name: "ours", open: func(tb testing.TB) setGetter { return oursSG{c: newOursTCP(tb)} }},
		{name: "yeqown", open: func(tb testing.TB) setGetter { return yeqownSG{c: newYeqown(tb, tcpAddr())} }},
		{name: "bradfitz", open: func(tb testing.TB) setGetter { return bradfitzSG{c: newBradfitz(tb)} }},
		{name: "rainycape", open: func(tb testing.TB) setGetter { return rainycapeSG{c: newRainycape(tb)} }},
	}
}
