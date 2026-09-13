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
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				items, err := c.Get(ctx, keyBytes, ours.GetOpts{})
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != n {
					b.Fatalf("got %d items", len(items))
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
	casOpts := ours.GetOpts{CAS: true}

	b.Run("ours", func(b *testing.B) {
		c := newOursTCP(b)
		ctx := context.Background()
		var one [1][]byte
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			one[0] = keyBytes[i%len(keyBytes)]
			items, err := c.Get(ctx, one[:], casOpts)
			if err != nil {
				b.Fatal(err)
			}
			if len(items) == 0 || items[0].CAS == 0 {
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
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := []byte(benchKey(b.Name(), uint64(i)))
				if _, err := c.Meta().Set(ctx, key, size.val, ours.MetaSet{}); err != nil {
					b.Fatal(err)
				}
				res, err := c.Meta().Get(ctx, key, mg)
				if err != nil {
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
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		got, err := c.Meta().Pipeline(ctx, ops)
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(keys) {
			b.Fatalf("got %d results", len(got))
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
	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		p := c.Pipeline()
		for j := range batch {
			key := []byte(benchKey(b.Name(), uint64(i*batch+j)))
			p.Store(ours.Item{Key: key, Value: smallValue})
		}
		if err := p.Run(ctx); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
