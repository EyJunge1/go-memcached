package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetaQuietHitReturnsValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	ctx := context.Background()
	_, err := doMetaSet(ctx, client, []byte("k"), []byte("hello\nworld"), MetaSet{})
	noError(t, err)

	item, err := doMetaGet(ctx, client, []byte("k"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("hello\nworld"), item.Item.Value)
}

func TestMetaQuietMissWithValueIsNotFound(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())

	res, err := doMetaGet(context.Background(), client, []byte("missing"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)

	_, err = doMetaSet(context.Background(), client, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	item, err := doMetaGet(context.Background(), client, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), item.Item.Value)
}

func TestMetaGetCheckCASReusesValueCap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := doMetaSet(ctx, client, []byte("k"), []byte("v"), MetaSet{Return: MetaSetReturn{CAS: true}})
	noError(t, err)
	var dst MetaResult
	dst.Item.Value = make([]byte, 0, 32)
	mg := MetaGet{Return: MetaGetReturn{CAS: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	cas := dst.Item.CAS
	equal(t, []byte("v"), dst.Item.Value)
	equal(t, 32, cap(dst.Item.Value))

	mg.CompareCAS = &cas
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	noError(t, dst.Err())
	isTrue(t, dst.Item.CASMatched)
	equal(t, 0, len(dst.Item.Value))
	equal(t, 32, cap(dst.Item.Value))

	wrong := cas + 1
	mg.CompareCAS = &wrong
	noError(t, client.Meta().Get(ctx, []byte("k"), &dst, mg))
	equal(t, []byte("v"), dst.Item.Value)
	isFalse(t, dst.Item.CASMatched)
	equal(t, 32, cap(dst.Item.Value))
}

func TestMetaQuietClientErrorThenMN(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_, err := doMetaGet(context.Background(), client, []byte("cerr"), MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}})
	isErrorIs(t, err, ErrClientError)
	err = client.Version(context.Background(), new([]byte))
	noError(t, err)
	equal(t, int32(1), dials.Load())
}

func TestMetaGetReturnsWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("foo"), Value: []byte("v")}))
	got, err := doMetaGet(ctx, client, []byte("foo"), MetaGet{Return: MetaGetReturn{Key: true, Value: true}})
	noError(t, err)
	equal(t, []byte("foo"), got.Item.WireKey)
	equal(t, []byte("foo"), got.Item.Key)
	isFalse(t, got.Item.KeyBase64)

	var dst MetaResult
	dst.Item.WireKey = make([]byte, 0, 32)
	mg := MetaGet{Return: MetaGetReturn{Key: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	buf := dst.Item.WireKey
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	if cap(dst.Item.WireKey) != cap(buf) || (cap(buf) > 0 && &dst.Item.WireKey[:cap(dst.Item.WireKey)][0] != &buf[:cap(buf)][0]) {
		t.Fatal("WireKey cap was not reused")
	}
	equal(t, []byte("foo"), dst.Item.WireKey)
}

func TestMetaGetDestKeyDoesNotAliasWireKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("foo"), []byte("v"))
	var dst MetaResult
	dst.Item.Key = make([]byte, 0, 16)
	dst.Item.WireKey = make([]byte, 0, 16)
	mg := MetaGet{Return: MetaGetReturn{Key: true, Value: true}}
	noError(t, client.Meta().Get(ctx, []byte("foo"), &dst, mg))
	equal(t, []byte("foo"), dst.Item.Key)
	equal(t, []byte("ns:foo"), dst.Item.WireKey)
	if sameByteArray(dst.Item.Key, dst.Item.WireKey) {
		t.Fatal("Key aliases WireKey")
	}
	dst.Item.Key = append(dst.Item.Key, 'x')
	equal(t, []byte("ns:foo"), dst.Item.WireKey)
}

func TestMetaGetVivifyRoundTrip(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	newCAS := uint64(42)

	res, err := doMetaGet(context.Background(), client, []byte("missing"), MetaGet{
		Vivify: new(Rel(time.Second)),
		NewCAS: &newCAS,
	})
	noError(t, err)
	equal(t, MetaStatusHD, res.Status)
	isTrue(t, res.Item.Won)
	waitLastCmdHasToken(t, srv, "N1")
	waitLastCmdHasToken(t, srv, "E42")
}
