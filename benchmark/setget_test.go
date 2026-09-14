package benchmark

import (
	"context"
	"sync/atomic"
	"testing"

	ours "github.com/EyJunge1/go-memcached/memcached"
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
			dst := ours.GetResult{Value: make([]byte, 0, len(size.val))}
			var stored, fetched ours.PipelineResult
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				key := keys[i%len(keys)]
				err := p.
					Store(ours.Item{Key: key, Value: size.val}, &stored).
					GetOne(key, &dst, ours.GetOpts{}, &fetched).
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
				dst := ours.GetResult{Value: make([]byte, 0, len(size.val))}
				var stored, fetched ours.PipelineResult
				batch, err := c.Pipeline().
					Store(ours.Item{Key: key, Value: size.val}, &stored).
					GetOne(key, &dst, ours.GetOpts{}, &fetched).
					Prepare()
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = batch.Close() })
				var held *ours.Session
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
				var n atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					ctx := context.Background()
					w := worker(sg)
					for pb.Next() {
						key := keys[int(n.Add(1)-1)%len(keys)]
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
