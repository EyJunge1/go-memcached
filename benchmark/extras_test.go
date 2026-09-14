package benchmark

import (
	"context"
	"strconv"
	"testing"

	ours "github.com/EyJunge1/go-memcached/memcached"
	yeqown "github.com/yeqown/memcached"
)

func BenchmarkTCPMultiGet(b *testing.B) {
	requireTCP(b)
	for _, n := range []int{8, 32} {
		keys := seedKeys(b, smallValue, n)
		keyBytes := asKeyBytes(keys)
		b.Run("ours/"+strconv.Itoa(n), func(b *testing.B) {
			c := newOursTCP(b)
			ctx := context.Background()
			dst := make([]ours.GetResult, n)
			for i := range dst {
				dst[i].Value = make([]byte, 0, 64)
			}
			if err := c.Get(ctx, keyBytes, dst, ours.GetOpts{}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := c.Get(ctx, keyBytes, dst, ours.GetOpts{}); err != nil {
					b.Fatal(err)
				}
				for i := range dst {
					if !dst[i].Found {
						b.Fatalf("miss at %d", i)
					}
				}
			}
		})
		b.Run("yeqown/"+strconv.Itoa(n), func(b *testing.B) {
			c := newYeqown(b, tcpAddr())
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				items, err := c.Gets(ctx, keys...)
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != n {
					b.Fatalf("got %d items", len(items))
				}
			}
		})
		b.Run("bradfitz/"+strconv.Itoa(n), func(b *testing.B) {
			c := newBradfitz(b)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				items, err := c.GetMulti(keys)
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != n {
					b.Fatalf("got %d items", len(items))
				}
			}
		})
		b.Run("rainycape/"+strconv.Itoa(n), func(b *testing.B) {
			c := newRainycape(b)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				items, err := c.GetMulti(keys)
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != n {
					b.Fatalf("got %d items", len(items))
				}
			}
		})
	}
}

func BenchmarkTCPGetCAS(b *testing.B) {
	requireTCP(b)
	keys := seedKeys(b, smallValue, 256)
	keyBytes := asKeyBytes(keys)
	casOpts := ours.GetOpts{ReturnCAS: true}

	b.Run("ours", func(b *testing.B) {
		c := newOursTCP(b)
		ctx := context.Background()
		dst := ours.GetResult{Value: make([]byte, 0, 64)}
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			if err := c.GetOne(ctx, keyBytes[i%len(keyBytes)], &dst, casOpts); err != nil {
				b.Fatal(err)
			}
			if !dst.Found || dst.CAS == 0 {
				b.Fatal("missing CAS")
			}
			i++
		}
	})

	b.Run("yeqown", func(b *testing.B) {
		c := newYeqown(b, tcpAddr())
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			items, err := c.Gets(ctx, keys[i%len(keys)])
			if err != nil {
				b.Fatal(err)
			}
			if len(items) == 0 || items[0].CAS == 0 {
				b.Fatal("missing CAS")
			}
			i++
		}
	})

	b.Run("bradfitz", func(b *testing.B) {
		c := newBradfitz(b)
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			item, err := c.Get(keys[i%len(keys)])
			if err != nil {
				b.Fatal(err)
			}
			if item.CasID == 0 {
				b.Fatal("missing CAS")
			}
			i++
		}
	})

	b.Run("rainycape", func(b *testing.B) {
		c := newRainycape(b)
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			if _, err := c.Get(keys[i%len(keys)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkTCPMetaSetGet(b *testing.B) {
	requireTCP(b)
	for _, size := range payloadSizes() {
		b.Run("ours/"+size.name, func(b *testing.B) {
			c := newOursTCP(b)
			ctx := context.Background()
			mg := ours.MetaGet{Return: ours.MetaGetReturn{Value: true}}
			var res ours.MetaResult
			res.Item.Value = make([]byte, 0, len(size.val)+8)
			var setRes ours.MetaResult
			key := make([]byte, 0, 128)
			prefix := []byte(b.Name())
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key = append(key[:0], prefix...)
				key = append(key, '/')
				key = strconv.AppendUint(key, uint64(i), 10)
				if err := c.Meta().Set(ctx, key, size.val, &setRes, ours.MetaSet{}); err != nil {
					b.Fatal(err)
				}
				if err := c.Meta().Get(ctx, key, &res, mg); err != nil {
					b.Fatal(err)
				}
				if err := res.Err(); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
		b.Run("yeqown/"+size.name, func(b *testing.B) {
			c := newYeqown(b, tcpAddr())
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := []byte(benchKey(b.Name(), uint64(i)))
				if _, err := c.MetaSet(ctx, key, size.val); err != nil {
					b.Fatal(err)
				}
				if _, err := c.MetaGet(ctx, key, yeqown.MetaGetFlagReturnValue()); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

func BenchmarkTCPMetaPipeline(b *testing.B) {
	requireTCP(b)
	keys := seedKeys(b, smallValue, 8)
	ops := make([]ours.MetaOp, len(keys))
	mg := ours.MetaGet{Return: ours.MetaGetReturn{Value: true}}
	for i, k := range keys {
		ops[i] = ours.MetaOp{Key: []byte(k), Get: &mg}
	}

	c := newOursTCP(b)
	ctx := context.Background()
	got := make([]ours.MetaResult, len(ops))
	for i := range got {
		got[i].Item.Value = make([]byte, 0, 64)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.Meta().Pipeline(ctx, ops, got); err != nil {
			b.Fatal(err)
		}
		for i := range got {
			if err := got[i].Err(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTCPPipelineStore(b *testing.B) {
	requireTCP(b)
	c := newOursTCP(b)
	ctx := context.Background()
	const batch = 8
	keys := asKeyBytes(makeBenchKeys(b, batch))
	items := make([]ours.Item, batch)
	for i := range items {
		items[i] = ours.Item{Key: keys[i], Value: smallValue}
	}
	p := c.Pipeline()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range items {
			p.Store(items[i])
		}
		if err := p.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
