package memcached

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func testConfig(addr string, tweak ...func(*Config)) (Config, error) {
	servers, err := ParseServers(addr)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{Servers: servers}
	for _, fn := range tweak {
		fn(&cfg)
	}
	return cfg, nil
}

func mustAdmin(t *testing.T, c *Client) *Admin {
	t.Helper()
	admin, err := c.Admin()
	noError(t, err)
	return admin
}

func mustAdminOn(t *testing.T, c *Client) *AdminNode {
	t.Helper()
	sv := c.Servers()
	if len(sv) == 0 {
		t.Fatal("no servers")
	}
	return mustAdmin(t, c).On(sv[0])
}

func delayPtr(e Exptime) *Exptime {
	return &e
}

func testClient(t testing.TB, addr string, tweak ...func(*Config)) *Client {
	t.Helper()
	cfg, err := testConfig(addr, tweak...)
	noError(t, err)
	c, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func noError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustStore(t testing.TB, c *Client, ctx context.Context, key, value []byte) {
	t.Helper()
	noError(t, c.Store(ctx, Item{Mode: StoreSet, Key: key, Value: value}))
}

func mustGet(t *testing.T, c *Client, ctx context.Context, key []byte) GetResult {
	t.Helper()
	got, err := doGetOne(c, ctx, key, GetOpts{})
	noError(t, err)
	if !got.Found {
		t.Fatalf("get miss for %q", key)
	}
	return got
}

func doGetOne(c *Client, ctx context.Context, key []byte, opts GetOpts) (GetResult, error) {
	var dst GetResult
	err := c.GetOne(ctx, key, &dst, opts)
	return dst, err
}

func doVersion(c *Client, ctx context.Context) ([]byte, error) {
	var dst []byte
	err := c.Version(ctx, &dst)
	return dst, err
}

func doVersionAll(c *Client, ctx context.Context) ([]NodeVersion, error) {
	var dst []NodeVersion
	err := c.VersionAll(ctx, &dst)
	return dst, err
}

func doStats(c *Client, ctx context.Context, args ...string) ([]Stat, error) {
	var dst []Stat
	err := c.Stats(ctx, &dst, args...)
	return dst, err
}

func doStatsAll(c *Client, ctx context.Context, args ...string) ([]NodeStats, error) {
	var dst []NodeStats
	err := c.StatsAll(ctx, &dst, args...)
	return dst, err
}

func doAdminStats(a *AdminNode, ctx context.Context, args ...string) ([]Stat, error) {
	var dst []Stat
	err := a.Stats(ctx, &dst, args...)
	return dst, err
}

func doAdminStatsDetail(a *AdminNode, ctx context.Context, mode StatsDetailMode) ([]Stat, error) {
	var dst []Stat
	err := a.StatsDetail(ctx, &dst, mode)
	return dst, err
}

func lookupStat(stats []Stat, name string) (string, bool) {
	for i := range stats {
		if string(stats[i].Name) == name {
			return string(stats[i].Value), true
		}
	}
	return "", false
}

func debugField(item MetaItemDebug, key string) string {
	for i := range item.Fields {
		if string(item.Fields[i].Key) == key {
			return string(item.Fields[i].Value)
		}
	}
	return ""
}

func doGet(c *Client, ctx context.Context, keys [][]byte, opts GetOpts) ([]GetResult, error) {
	dst := make([]GetResult, len(keys))
	err := c.Get(ctx, keys, dst, opts)
	return dst, err
}

func doMetaGet(c *Client, ctx context.Context, key []byte, f MetaGet) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Get(ctx, key, f, &dst)
	return dst, err
}

func doMetaSet(c *Client, ctx context.Context, key, value []byte, f MetaSet) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Set(ctx, key, value, f, &dst)
	return dst, err
}

func doMetaDelete(c *Client, ctx context.Context, key []byte, f MetaDelete) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Delete(ctx, key, f, &dst)
	return dst, err
}

func doMetaArith(c *Client, ctx context.Context, key []byte, delta uint64, f MetaArithmetic) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Arithmetic(ctx, key, delta, f, &dst)
	return dst, err
}

func doMetaNoop(c *Client, ctx context.Context, f MetaNoop) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Noop(ctx, f, &dst)
	return dst, err
}

func doMetaDebug(c *Client, ctx context.Context, key []byte, f MetaDebug) (MetaItemDebug, error) {
	var dst MetaItemDebug
	err := c.Meta().Debug(ctx, key, f, &dst)
	return dst, err
}

func doMetaPipeline(c *Client, ctx context.Context, ops []MetaOp) ([]MetaResult, error) {
	dst := make([]MetaResult, len(ops))
	err := c.Meta().Pipeline(ctx, ops, dst)
	return dst, err
}

func expectStore(want error) func(error) error {
	return func(err error) error {
		if !errors.Is(err, want) {
			return fmt.Errorf("got %v, want errors.Is(..., %v)", err, want)
		}
		return nil
	}
}

func isError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}

func isErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v, want errors.Is(..., %v)", err, target)
	}
}

func equal(t *testing.T, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
}

func isTrue(t *testing.T, got bool) {
	t.Helper()
	if !got {
		t.Fatal("want true")
	}
}

func isFalse(t *testing.T, got bool) {
	t.Helper()
	if got {
		t.Fatal("want false")
	}
}

func isNil(t *testing.T, got any) {
	t.Helper()
	if !isNilValue(got) {
		t.Fatalf("want nil, got %#v", got)
	}
}

func notNil(t *testing.T, got any) {
	t.Helper()
	if isNilValue(got) {
		t.Fatal("want non-nil")
	}
}

func notZero[T comparable](t *testing.T, got T) {
	t.Helper()
	var zero T
	if got == zero {
		t.Fatal("want non-zero")
	}
}

func hasLen(t *testing.T, got any, n int) {
	t.Helper()
	gotLen := reflect.ValueOf(got).Len()
	if gotLen != n {
		t.Fatalf("want len %d, got %d", n, gotLen)
	}
}

func contains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("%q does not contain %q", s, sub)
	}
}

func lessOrEqual[T int | int64](t *testing.T, got, wantMax T) {
	t.Helper()
	if got > wantMax {
		t.Fatalf("got %v, want <= %v", got, wantMax)
	}
}

func greater[T ~int | ~int64](t *testing.T, got, wantMin T) {
	t.Helper()
	if got <= wantMin {
		t.Fatalf("got %v, want > %v", got, wantMin)
	}
}

func same(t *testing.T, want, got any) {
	t.Helper()
	if reflect.ValueOf(want).Pointer() != reflect.ValueOf(got).Pointer() {
		t.Fatal("want same pointer")
	}
}

func isNilValue(got any) bool {
	if got == nil {
		return true
	}
	v := reflect.ValueOf(got)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
