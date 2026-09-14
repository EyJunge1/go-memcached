package benchmark

import (
	"context"
	"testing"

	ours "github.com/EyJunge1/go-memcached/memcached"
	"github.com/bradfitz/gomemcache/memcache"
	rainycape "github.com/rainycape/memcache"
)

func TestOurs(t *testing.T) {
	c := newOursTCP(t)
	ctx := context.Background()
	key, val := []byte("b/ours/smoke"), smallValue
	if err := c.Store(ctx, ours.Item{Key: key, Value: val}); err != nil {
		t.Fatal(err)
	}
	var got ours.GetResult
	if err := c.GetOne(ctx, key, &got, ours.GetOpts{}); err != nil {
		t.Fatal(err)
	}
	if !got.Found || string(got.Value) != string(val) {
		t.Fatalf("got %#v", got)
	}
}

func TestYeqown(t *testing.T) {
	c := newYeqown(t, tcpAddr())
	ctx := context.Background()
	if err := c.Set(ctx, "b/yeqown/smoke", smallValue, 0, 0); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get(ctx, "b/yeqown/smoke")
	if err != nil {
		t.Fatal(err)
	}
	if string(item.Value) != string(smallValue) {
		t.Fatalf("got %q", item.Value)
	}
}

func TestBradfitz(t *testing.T) {
	c := newBradfitz(t)
	if err := c.Set(&memcache.Item{Key: "b/bradfitz/smoke", Value: smallValue}); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get("b/bradfitz/smoke")
	if err != nil {
		t.Fatal(err)
	}
	if string(item.Value) != string(smallValue) {
		t.Fatalf("got %q", item.Value)
	}
}

func TestRainycape(t *testing.T) {
	c := newRainycape(t)
	if err := c.Set(&rainycape.Item{Key: "b/rainycape/smoke", Value: smallValue}); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get("b/rainycape/smoke")
	if err != nil {
		t.Fatal(err)
	}
	if string(item.Value) != string(smallValue) {
		t.Fatalf("got %q", item.Value)
	}
}
