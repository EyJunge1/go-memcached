package benchmark

import (
	"context"
	"testing"

	ours "github.com/EyJunge1/go-memcached/memcached"
	yeqown "github.com/yeqown/memcached"
)

func BenchmarkUDPGet(b *testing.B) {
	requireUDP(b)
	for _, size := range payloadSizes() {
		nseed := 256
		if size.name == "4k" {
			nseed = 32
		}
		keys := seedKeys(b, size.val, nseed)

		b.Run("ours/"+size.name, func(b *testing.B) {
			c := newOursUDP(b)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := []byte(keys[i%len(keys)])
				items, err := c.Get(ctx, [][]byte{key}, ours.GetOpts{})
				if err != nil {
					b.Fatal(err)
				}
				if len(items) == 0 {
					b.Fatal(errMiss)
				}
				i++
			}
		})

		b.Run("yeqown/"+size.name, func(b *testing.B) {
			c := newYeqown(b, udpAddr())
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				if _, err := c.Get(ctx, keys[i%len(keys)]); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})

		b.Run("bradfitz/"+size.name, func(b *testing.B) {
			b.Skip("bradfitz/gomemcache has no UDP API")
		})
		b.Run("rainycape/"+size.name, func(b *testing.B) {
			b.Skip("rainycape/memcache has no UDP API")
		})
	}
}

func BenchmarkUDPMetaGet(b *testing.B) {
	requireUDP(b)
	keys := seedKeys(b, smallValue, 256)
	keyBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keyBytes[i] = []byte(k)
	}

	b.Run("ours", func(b *testing.B) {
		c := newOursUDP(b)
		ctx := context.Background()
		mg := ours.MetaGet{Return: ours.MetaGetReturn{Value: true}}
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			res, err := c.Meta().Get(ctx, keyBytes[i%len(keyBytes)], mg)
			if err != nil {
				b.Fatal(err)
			}
			if err := res.Err(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	b.Run("yeqown", func(b *testing.B) {
		c := newYeqown(b, udpAddr())
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			if _, err := c.MetaGet(ctx, keyBytes[i%len(keyBytes)], yeqown.MetaGetFlagReturnValue()); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
