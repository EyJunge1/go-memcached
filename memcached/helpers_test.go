package memcached

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
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
	if admin == nil {
		t.Fatal("nil admin")
	}
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

func testClient(t testing.TB, addr string, tweak ...func(*Config)) *Client {
	t.Helper()
	cfg, err := testConfig(addr, tweak...)
	noError(t, err)
	c := mustNewClient(t, cfg)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func mustNewClient(t testing.TB, cfg Config) *Client {
	t.Helper()
	c, err := New(cfg)
	return requireClient(t, c, err)
}

func requireClient(t testing.TB, c *Client, err error) *Client {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("nil client")
	}
	return c
}

func noError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustListen(t testing.TB, network, address string) net.Listener {
	t.Helper()
	ln, err := net.Listen(network, address)
	if err != nil {
		t.Fatalf("listen %s %s: %v", network, address, err)
	}
	if ln == nil {
		t.Fatal("nil listener")
	}
	return ln
}

func mustTLSListen(t testing.TB, network, address string, cfg *tls.Config) net.Listener {
	t.Helper()
	ln, err := tls.Listen(network, address, cfg)
	if err != nil {
		t.Fatalf("tls listen %s %s: %v", network, address, err)
	}
	if ln == nil {
		t.Fatal("nil tls listener")
	}
	return ln
}

func mustWatch(ctx context.Context, t testing.TB, a *AdminNode, topics ...WatchTopic) *WatchStream {
	t.Helper()
	w, err := a.Watch(ctx, topics...)
	noError(t, err)
	if w == nil {
		t.Fatal("nil watch stream")
	}
	return w
}

func mustMetaDump(ctx context.Context, t testing.TB, a *AdminNode, selector string) *DumpStream {
	t.Helper()
	d, err := a.LRUCrawlerMetaDump(ctx, selector)
	noError(t, err)
	if d == nil {
		t.Fatal("nil meta dump stream")
	}
	return d
}

func mustMgDump(ctx context.Context, t testing.TB, a *AdminNode, selector string) *DumpStream {
	t.Helper()
	d, err := a.LRUCrawlerMgDump(ctx, selector)
	noError(t, err)
	if d == nil {
		t.Fatal("nil mg dump stream")
	}
	return d
}

func errText(t testing.TB, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("nil error")
	}
	return err.Error()
}

func requirePtr[T any](t testing.TB, v *T) *T {
	t.Helper()
	if v == nil {
		t.Fatal("nil pointer")
	}
	return v
}

func requireSlice[T any](t testing.TB, s []T) []T {
	t.Helper()
	if s == nil {
		t.Fatal("nil slice")
	}
	return s
}

func mustBuildReq(t testing.TB, req *request, err error) *request {
	t.Helper()
	noError(t, err)
	return requirePtr(t, req)
}

func mustBuildPair(t testing.TB, req *request, resp *response, err error) (*request, *response) {
	t.Helper()
	noError(t, err)
	return requirePtr(t, req), requirePtr(t, resp)
}

func mustStore(ctx context.Context, t testing.TB, c *Client, key, value []byte) {
	t.Helper()
	noError(t, c.Store(ctx, Item{Mode: StoreSet, Key: key, Value: value}))
}

func mustGet(ctx context.Context, t *testing.T, c *Client, key []byte) GetResult {
	t.Helper()
	got, err := doGetOne(ctx, c, key, GetOpts{})
	noError(t, err)
	if !got.Found {
		t.Fatalf("get miss for %q", key)
	}
	return got
}

func doGetOne(ctx context.Context, c *Client, key []byte, opts GetOpts) (GetResult, error) {
	var dst GetResult
	err := c.GetOne(ctx, key, &dst, opts)
	return dst, err
}

func doVersion(ctx context.Context, c *Client) ([]byte, error) {
	var dst []byte
	err := c.Version(ctx, &dst)
	return dst, err
}

func doVersionAll(ctx context.Context, c *Client) ([]NodeVersion, error) {
	var dst []NodeVersion
	err := c.VersionAll(ctx, &dst)
	return dst, err
}

func doStats(ctx context.Context, c *Client, args ...string) ([]Stat, error) {
	var dst []Stat
	err := c.Stats(ctx, &dst, args...)
	return dst, err
}

func doStatsAll(ctx context.Context, c *Client, args ...string) ([]NodeStats, error) {
	var dst []NodeStats
	err := c.StatsAll(ctx, &dst, args...)
	return dst, err
}

func doAdminStatsReset(ctx context.Context, a *AdminNode) error {
	return a.StatsReset(ctx)
}

func doAdminStatsDetail(ctx context.Context, a *AdminNode, mode StatsDetailMode) ([]Stat, error) {
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

func doGet(ctx context.Context, c *Client, keys [][]byte, opts GetOpts) ([]GetResult, error) {
	dst := make([]GetResult, len(keys))
	err := c.Get(ctx, keys, dst, opts)
	return dst, err
}

func doMetaGet(ctx context.Context, c *Client, key []byte, f MetaGet) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Get(ctx, key, &dst, f)
	return dst, err
}

func doMetaSet(ctx context.Context, c *Client, key, value []byte, f MetaSet) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Set(ctx, key, value, &dst, f)
	return dst, err
}

func doMetaDelete(ctx context.Context, c *Client, key []byte, f MetaDelete) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Delete(ctx, key, &dst, f)
	return dst, err
}

func doMetaArith(ctx context.Context, c *Client, key []byte, delta uint64, f MetaArithmetic) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Arithmetic(ctx, key, delta, &dst, f)
	return dst, err
}

func doMetaNoop(ctx context.Context, c *Client, f MetaNoop) (MetaResult, error) {
	var dst MetaResult
	err := c.Meta().Noop(ctx, &dst, f)
	return dst, err
}

func doMetaDebug(ctx context.Context, c *Client, key []byte, f MetaDebug) (MetaItemDebug, error) {
	var dst MetaItemDebug
	err := c.Meta().Debug(ctx, key, &dst, f)
	return dst, err
}

func doMetaPipeline(ctx context.Context, c *Client, ops []MetaOp) ([]MetaResult, error) {
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
	if target == nil {
		if err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("got nil, want errors.Is(..., %v)", target)
	}
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

func lessOrEqual[T int | int64 | uint64](t *testing.T, got, wantMax T) {
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
