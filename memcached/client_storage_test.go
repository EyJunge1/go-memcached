package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestFakeServerRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("foo"), Value: []byte("bar"), Flags: 7}))
	item := mustGet(ctx, t, client, []byte("foo"))
	equal(t, []byte("bar"), item.Value)
	equal(t, uint32(7), item.Flags)

	items, err := doGet(ctx, client, [][]byte{[]byte("foo")}, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("bar"), items[0].Value)
}

func TestAddNotStoredKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	noError(t, expectStore(ErrNotStored)(client.Store(ctx, Item{Mode: StoreAdd, Key: []byte("k"), Value: []byte("other")})))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("v2")}))
	equal(t, int32(1), dials.Load())
}

func TestCASExistsKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	noError(t, expectStore(ErrExists)(client.Store(ctx, Item{Mode: StoreCAS, Key: []byte("k"), Value: []byte("other"), CAS: 99})))
	noError(t, expectStore(ErrNotFound)(client.Store(ctx, Item{Mode: StoreCAS, Key: []byte("missing"), Value: []byte("x"), CAS: 1})))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("v2")}))
	equal(t, int32(1), dials.Load())
}

func TestDeleteNotFoundKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	ctx := context.Background()
	isErrorIs(t, client.Delete(ctx, []byte("missing"), MutateOpts{}), ErrNotFound)
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	equal(t, int32(1), dials.Load())
}

func TestStoreRejectsCRLFKey(t *testing.T) {
	srv := startFakeMemcached(t)
	var writes atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(_ context.Context, network, address string) (net.Conn, error) {
			writes.Add(1)
			return net.Dial(network, address)
		}
	})
	err := client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("a\r\nb"), Value: []byte("v")})
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, int32(0), writes.Load())
}
