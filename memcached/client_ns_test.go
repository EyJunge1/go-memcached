package memcached

import (
	"context"
	"testing"
)

func TestNamespaceKeyWireAndStrip(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	ctx := context.Background()

	noError(t, c.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}))
	var got GetResult
	noError(t, c.GetOne(ctx, []byte("k"), &got, GetOpts{}))
	isTrue(t, got.Found)
	equal(t, []byte("v"), got.Value)

	wired, box := c.nsKey([]byte("k"))
	equal(t, []byte("ns:k"), wired)
	putBuilderBox(box)

	equal(t, []byte("k"), c.stripNS([]byte("ns:k")))
	equal(t, []byte("other"), c.stripNS([]byte("other")))
	if c.stripNS(nil) != nil {
		t.Fatal("stripNS(nil) want nil")
	}

	keys := [][]byte{[]byte("a"), []byte("b")}
	hdr, wbox, hdrp, err := c.wireGetKeys(keys)
	noError(t, err)
	equal(t, []byte("ns:a"), hdr[0])
	equal(t, []byte("ns:b"), hdr[1])
	releaseWiredGetKeys(wbox, hdrp)

	ps := c.PoolStats()
	if len(ps) == 0 {
		t.Fatal("expected pool stats after traffic")
	}
	if ps[0].MaxOpen <= 0 {
		t.Fatalf("unexpected pool stats: %+v", ps[0])
	}
}

func TestWireGetKeysWithoutNamespace(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	keys := [][]byte{[]byte("a"), []byte("b")}
	hdr, box, hdrp, err := c.wireGetKeys(keys)
	noError(t, err)
	if box != nil || hdrp != nil {
		t.Fatal("expected nil boxes without namespace")
	}
	equal(t, keys[0], hdr[0])
	equal(t, keys[1], hdr[1])

	_, _, _, err = c.wireGetKeys([][]byte{[]byte("bad key\n")})
	isErrorIs(t, err, ErrInvalidKey)
}

func TestNsKeyWithoutPrefix(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	key := []byte("plain")
	wired, box := c.nsKey(key)
	if box != nil {
		t.Fatal("expected nil box")
	}
	equal(t, key, wired)
	if (*Client)(nil).nsKey(key); false {
		t.Fatal("unreachable")
	}
	w, b := (*Client)(nil).nsKey(key)
	equal(t, key, w)
	if b != nil {
		t.Fatal("nil client box")
	}
}
