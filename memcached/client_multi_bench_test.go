package memcached

import (
	"context"
	"testing"
)

// Offline competitive-shape microbenches (fake server): meta quiet multi vs
// StoreMulti scaffolding. Compare allocs with make bench-micro / go test -bench.
func BenchmarkMetaGetMulti(b *testing.B) {
	srv := startFakeMemcached(b)
	c := testClient(b, srv.Addr())
	ctx := context.Background()
	keys := make([][]byte, 32)
	for i := range keys {
		keys[i] = []byte("k" + string(rune('A'+i)))
		noError(b, c.Store(ctx, Item{Key: keys[i], Value: []byte("v")}))
	}
	dst := make([]MetaResult, len(keys))
	for i := range dst {
		dst[i].Item.Value = make([]byte, 0, 8)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.Meta().GetMulti(ctx, keys, dst, MetaGet{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreMulti(b *testing.B) {
	srv := startFakeMemcached(b)
	c := testClient(b, srv.Addr())
	ctx := context.Background()
	items := make([]Item, 32)
	for i := range items {
		items[i] = Item{Key: []byte("s" + string(rune('A'+i))), Value: []byte("v")}
	}
	dst := make([]MetaResult, len(items))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.Meta().StoreMulti(ctx, items, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPreparedMetaGetMulti(b *testing.B) {
	srv := startFakeMemcached(b)
	c := testClient(b, srv.Addr())
	ctx := context.Background()
	keys := make([][]byte, 32)
	ops := make([]MetaOp, len(keys))
	dst := make([]MetaResult, len(keys))
	g := MetaGet{Quiet: true, Return: MetaGetReturn{Value: true}}
	for i := range keys {
		keys[i] = []byte("p" + string(rune('A'+i)))
		noError(b, c.Store(ctx, Item{Key: keys[i], Value: []byte("v")}))
		gf := g
		ops[i] = MetaOp{Key: keys[i], Get: &gf}
		dst[i].Item.Value = make([]byte, 0, 8)
	}
	batch, err := c.Meta().PreparePipeline(ops, dst)
	noError(b, err)
	b.Cleanup(func() { _ = batch.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
