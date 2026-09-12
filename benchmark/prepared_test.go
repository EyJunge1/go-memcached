package benchmark

import (
	"context"
	"runtime"
	"strconv"
	"testing"

	"github.com/EyJunge1/go-memcached/memcached"
)

func BenchmarkTCPPreparedBatchDynamicValues(b *testing.B) {
	c := newOursTCP(b)
	ctx := context.Background()
	var stored memcached.PipelineResult
	batch, err := c.Pipeline().
		Store(memcached.Item{Key: []byte("prepared-dynamic"), Value: _smallValue}, &stored).
		Prepare()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	values := [][]byte{_largeValue}
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
	ops := []memcached.MetaOp{
		{Key: []byte("prepared-meta"), Body: _smallValue, Set: &memcached.MetaSet{}},
		{Key: []byte("prepared-meta"), Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}}},
	}
	dst := make([]memcached.MetaResult, len(ops))
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
	ops := []memcached.MetaOp{{
		Key: []byte("prepared-meta-dynamic"), Body: _smallValue, Set: &memcached.MetaSet{},
	}}
	dst := make([]memcached.MetaResult, len(ops))
	batch, err := c.Meta().PreparePipeline(ops, dst)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = batch.Close() })
	values := [][]byte{_largeValue}
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
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			keys := seedKeys(b, _smallValue, count)
			c := newOursTCP(b)
			ctx := context.Background()
			ops := make([]memcached.MetaOp, count)
			dst := make([]memcached.MetaResult, count)
			for i := range ops {
				ops[i] = memcached.MetaOp{
					Key: []byte(keys[i]),
					Get: &memcached.MetaGet{Return: memcached.MetaGetReturn{Value: true}},
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
		b.Run("ops-"+strconv.Itoa(count), func(b *testing.B) {
			c := newOursTCP(b)
			ctx := context.Background()
			ops := make([]memcached.MetaOp, count)
			dst := make([]memcached.MetaResult, count)
			for i := range ops {
				ops[i] = memcached.MetaOp{
					Key: []byte("large-meta/" + strconv.Itoa(i)),
					Get: &memcached.MetaGet{},
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
	key := []byte("session-worker")
	pool, err := c.NewSessionPool(ctx, key, runtime.GOMAXPROCS(0))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = pool.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var dst memcached.GetResult
		for pb.Next() {
			if err := pool.Do(ctx, func(session *memcached.Session) error {
				if err := session.Store(ctx, memcached.Item{Key: key, Value: _smallValue}); err != nil {
					return err
				}
				return session.GetOne(ctx, key, &dst, memcached.GetOpts{})
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
	var dst memcached.GetResult
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := session.Store(ctx, memcached.Item{Key: key, Value: _smallValue}); err != nil {
			b.Fatal(err)
		}
		if err := session.GetOne(ctx, key, &dst, memcached.GetOpts{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnixPreparedBatchSetGet(b *testing.B) {
	c := newOursUnix(b)
	ctx := context.Background()
	var (
		stored memcached.PipelineResult
		read   memcached.PipelineResult
		dst    memcached.GetResult
	)
	key := []byte("unix-prepared")
	batch, err := c.Pipeline().
		Store(memcached.Item{Key: key, Value: _smallValue}, &stored).
		GetOne(key, &dst, memcached.GetOpts{}, &read).
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
