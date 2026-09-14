package benchmark

import (
	"context"
	"sync/atomic"
	"testing"
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
