package benchmark

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	ours "github.com/EyJunge1/go-memcached/memcached"
)

func BenchmarkTCPPreparedBatchDynamicValues(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	var stored ours.PipelineResult
	batch, err := c.Pipeline().
		Store(ours.Item{Key: []byte("prepared-dynamic"), Value: smallValue}, &stored).
		Prepare()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	values := [][]byte{largeValue}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.RunValues(ctx, values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTCPPreparedMetaBatch(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	ops := []ours.MetaOp{
		{Key: []byte("prepared-meta"), Body: smallValue, Set: &ours.MetaSet{}},
		{Key: []byte("prepared-meta"), Get: &ours.MetaGet{Return: ours.MetaGetReturn{Value: true}}},
	}
	dst := make([]ours.MetaResult, len(ops))
	batch, err := c.Meta().PreparePipeline(ops, dst)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTCPPreparedMetaBatchDynamicValues(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	ops := []ours.MetaOp{{
		Key: []byte("prepared-meta-dynamic"), Body: smallValue, Set: &ours.MetaSet{},
	}}
	dst := make([]ours.MetaResult, len(ops))
	batch, err := c.Meta().PreparePipeline(ops, dst)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	values := [][]byte{largeValue}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.RunValues(ctx, values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTCPPreparedMetaBatchGet(b *testing.B) {
	for _, count := range []int{8, 256} {
		b.Run(fmt.Sprintf("%d", count), func(b *testing.B) {
			keys := seedKeys(b, smallValue, count)
			c := newOursTCP(b)
			ctx := context.Background()
			ops := make([]ours.MetaOp, count)
			dst := make([]ours.MetaResult, count)
			for i := range ops {
				ops[i] = ours.MetaOp{
					Key: []byte(keys[i]),
					Get: &ours.MetaGet{Return: ours.MetaGetReturn{Value: true}},
				}
				dst[i].Item.Value = make([]byte, 0, 64)
			}
			batch, err := c.Meta().PreparePipeline(ops, dst)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = batch.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := batch.Run(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTCPLargePreparedMetaBatch(b *testing.B) {
	for _, count := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprintf("ops-%d", count), func(b *testing.B) {
			c := newOursTCP(b)
			ctx := context.Background()
			ops := make([]ours.MetaOp, count)
			dst := make([]ours.MetaResult, count)
			for i := range ops {
				ops[i] = ours.MetaOp{
					Key: fmt.Appendf(nil, "large-meta/%d", i),
					Get: &ours.MetaGet{},
				}
			}
			batch, err := c.Meta().PreparePipeline(ops, dst)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = batch.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := batch.Run(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTCPSessionPoolParallel(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	pool, err := c.NewSessionPool(ctx, []byte("session-worker"), runtime.GOMAXPROCS(0))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = pool.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var dst ours.GetResult
		for pb.Next() {
			if err := pool.Do(ctx, func(session *ours.Session) error {
				if err := session.Store(ctx, ours.Item{Key: []byte("session-worker"), Value: smallValue}); err != nil {
					return err
				}
				return session.GetOne(ctx, []byte("session-worker"), &dst, ours.GetOpts{})
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTCPSessionSetGet(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	key := []byte("direct-session")
	session, err := c.Session(ctx, key)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = session.Close() })
	var dst ours.GetResult
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := session.Store(ctx, ours.Item{Key: key, Value: smallValue}); err != nil {
			b.Fatal(err)
		}
		if err := session.GetOne(ctx, key, &dst, ours.GetOpts{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnixPreparedBatchSetGet(b *testing.B) {
	c := newOursUnix(b)
	ctx := context.Background()
	var (
		stored ours.PipelineResult
		read   ours.PipelineResult
		dst    ours.GetResult
	)
	key := []byte("unix-prepared")
	batch, err := c.Pipeline().
		Store(ours.Item{Key: key, Value: smallValue}, &stored).
		GetOne(key, &dst, ours.GetOpts{}, &read).
		Prepare()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.Run(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
