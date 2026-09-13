package livetest

import (
	"context"
	"testing"

	"github.com/EyJunge1/go-memcached/memcached"
)

func TestNamespaceRoundTrip(t *testing.T) {
	ns := setupLiveClient(t, func(cfg *memcached.Config) {
		cfg.Cluster.Namespace = "lt:"
	})
	raw := setupLiveClient(t)
	ctx := context.Background()
	key := liveKey(t, "ns")
	mustStore(t, ns, ctx, key, []byte("namespaced"))

	item := mustGet(t, ns, ctx, key)
	equal(t, key, item.Key)
	equal(t, []byte("namespaced"), item.Value)

	prefixed := append([]byte("lt:"), key...)
	equal(t, []byte("namespaced"), mustGet(t, raw, ctx, prefixed).Value)
}

func TestSetServersAndClose(t *testing.T) {
	c := setupLiveClient(t, func(cfg *memcached.Config) {
		cfg.AllowSetServers = true
	})
	ctx := context.Background()
	noError(t, c.SetServers([]memcached.Server{{Address: tcpAddr()}}))

	key := liveKey(t, "ss")
	mustStore(t, c, ctx, key, []byte("ok"))
	equal(t, []byte("ok"), mustGet(t, c, ctx, key).Value)

	denied := setupLiveClient(t)
	isErrorIs(t, denied.SetServers([]memcached.Server{{Address: tcpAddr()}}), memcached.ErrSetServersNotAllowed)

	closed := setupLiveClient(t)
	noError(t, closed.Close())
	err := closed.Store(ctx, memcached.Item{Mode: memcached.StoreSet, Key: key, Value: []byte("x")})
	isErrorIs(t, err, memcached.ErrClosed)
}
