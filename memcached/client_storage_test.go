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
	noError(t, storeOK(client.Set(ctx, []byte("foo"), []byte("bar"), 7, 0)))
	item, err := client.GetOne(ctx, []byte("foo"))
	noError(t, err)
	equal(t, []byte("bar"), item.Value)
	equal(t, uint32(7), item.Flags)

	items, err := client.Get(ctx, GetOpts{CAS: true}, []byte("foo"))
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
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	noError(t, expectStore(ErrNotStored)(client.Add(ctx, []byte("k"), []byte("other"), 0, 0)))
	noError(t, storeOK(client.Set(ctx, []byte("k2"), []byte("v2"), 0, 0)))
	equal(t, int32(1), dials.Load())
}

func TestCasExistsKeepsConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	noError(t, expectStore(ErrExists)(client.Cas(ctx, []byte("k"), []byte("other"), 0, 0, 99)))
	noError(t, expectStore(ErrNotFound)(client.Cas(ctx, []byte("missing"), []byte("x"), 0, 0, 1)))
	noError(t, storeOK(client.Set(ctx, []byte("k2"), []byte("v2"), 0, 0)))
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
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	equal(t, int32(1), dials.Load())
}

func TestSetRejectsCRLFKey(t *testing.T) {
	srv := startFakeMemcached(t)
	var writes atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(_ context.Context, network, address string) (net.Conn, error) {
			writes.Add(1)
			return net.Dial(network, address)
		}
	})
	err := client.Set(context.Background(), []byte("a\r\nb"), []byte("v"), 0, 0)
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, int32(0), writes.Load())
}
