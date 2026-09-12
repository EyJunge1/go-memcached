package benchmark

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/EyJunge1/go-memcached/memcached"
)

func BenchmarkTCPSetGet(b *testing.B) {
	requireTCP(b)
	keys := makeBenchKeys(b, 256)
	for _, size := range payloadSizes() {
		for _, cl := range tcpFactories() {
			b.Run(cl.name+"/"+size.name, func(b *testing.B) {
				sg := cl.open(b)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				i := 0
				for b.Loop() {
					key := keys[i%len(keys)]
					if err := sg.set(ctx, key, size.val); err != nil {
						b.Fatal(err)
					}
					if err := sg.get(ctx, key); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		}
	}
}

func BenchmarkTCPPipelineSetGet(b *testing.B) {
	requireTCP(b)
	keys := asKeyBytes(makeBenchKeys(b, 256))
	for _, size := range payloadSizes() {
		b.Run("ours/"+size.name, func(b *testing.B) {
			c := newOursTCP(b)
			ctx := context.Background()
			p := c.Pipeline()
			dst := memcached.GetResult{Value: make([]byte, 0, len(size.val))}
			var stored, fetched memcached.PipelineResult
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := keys[i%len(keys)]
				err := p.
					Store(memcached.Item{Key: key, Value: size.val}, &stored).
					GetOne(key, &dst, memcached.GetOpts{}, &fetched).
					Run(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if stored.Err != nil || fetched.Err != nil || !dst.Found {
					b.Fatalf("store=%v get=%v found=%v", stored.Err, fetched.Err, dst.Found)
				}
				i++
			}
		})
	}
}

func BenchmarkTCPPreparedBatchSetGet(b *testing.B) {
	requireTCP(b)
	key := []byte("prepared-set-get")
	for _, size := range payloadSizes() {
		for _, session := range []bool{false, true} {
			name := "pooled"
			if session {
				name = "session"
			}
			b.Run(name+"/"+size.name, func(b *testing.B) {
				c := newOursTCP(b)
				ctx := context.Background()
				dst := memcached.GetResult{Value: make([]byte, 0, len(size.val))}
				var stored, fetched memcached.PipelineResult
				batch, err := c.Pipeline().
					Store(memcached.Item{Key: key, Value: size.val}, &stored).
					GetOne(key, &dst, memcached.GetOpts{}, &fetched).
					Prepare()
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = batch.Close() })
				var held *memcached.Session
				if session {
					held, err = c.Session(ctx, key)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = held.Close() })
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if held == nil {
						err = batch.Run(ctx)
					} else {
						err = held.RunPrepared(ctx, batch)
					}
					if err != nil {
						b.Fatal(err)
					}
					if stored.Err != nil || fetched.Err != nil || !dst.Found {
						b.Fatalf("store=%v get=%v found=%v", stored.Err, fetched.Err, dst.Found)
					}
				}
			})
		}
	}
}

func BenchmarkTCPGet(b *testing.B) {
	requireTCP(b)
	for _, size := range payloadSizes() {
		nseed := 256
		if size.name == "4k" {
			nseed = 32
		}
		keys := seedKeys(b, size.val, nseed)
		for _, cl := range tcpFactories() {
			b.Run(cl.name+"/"+size.name, func(b *testing.B) {
				sg := cl.open(b)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				i := 0
				for b.Loop() {
					if err := sg.get(ctx, keys[i%len(keys)]); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		}
	}
}

func BenchmarkTCPSetGetParallel(b *testing.B) {
	requireTCP(b)
	keys := makeBenchKeys(b, 4096)
	for _, size := range payloadSizes() {
		for _, cl := range tcpFactories() {
			b.Run(cl.name+"/"+size.name, func(b *testing.B) {
				sg := cl.open(b)
				var n atomic.Int64
				mod := int64(len(keys))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					ctx := context.Background()
					w := worker(sg)
					for pb.Next() {
						key := keys[(n.Add(1)-1)%mod]
						if err := w.set(ctx, key, size.val); err != nil {
							b.Fatal(err)
						}
						if err := w.get(ctx, key); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
