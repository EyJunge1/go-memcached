package memcached

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkRunFanout256(b *testing.B) {
	for _, workers := range []int{0, goruntime.GOMAXPROCS(0)} {
		b.Run(fanoutBenchmarkName(workers), func(b *testing.B) {
			var calls atomic.Uint64
			b.ReportAllocs()
			for b.Loop() {
				runFanout(256, workers, func(int) { calls.Add(1) })
			}
			if calls.Load() != uint64(b.N)*256 {
				b.Fatal("fanout dropped work")
			}
		})
	}
}

func TestFanoutPoolConcurrentCallsAndClose(t *testing.T) {
	pool := newFanoutPool(4)
	var calls atomic.Uint64
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			isTrue(t, pool.run(context.Background(), 256, func(context.Context, int) { calls.Add(1) }))
		})
	}
	wg.Wait()
	equal(t, uint64(16*256), calls.Load())
	pool.close()
	isFalse(t, pool.run(context.Background(), 1, func(context.Context, int) {}))
}

func TestFanoutPoolCloseCancelsActiveWork(t *testing.T) {
	pool := newFanoutPool(1)
	started := make(chan struct{})
	result := make(chan bool, 1)
	cause := make(chan error, 1)
	go func() {
		result <- pool.run(context.Background(), 1, func(ctx context.Context, _ int) {
			close(started)
			<-ctx.Done()
			cause <- context.Cause(ctx)
		})
	}()
	<-started
	pool.close()
	isTrue(t, <-result)
	isTrue(t, errors.Is(<-cause, ErrClosed))
}

func TestFanoutPoolCloseWithFullQueue(t *testing.T) {
	pool := newFanoutPool(1)
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = pool.run(context.Background(), 1, func(ctx context.Context, _ int) {
			close(started)
			<-ctx.Done()
		})
	})
	<-started

	const queued = 8
	var pending sync.WaitGroup
	pending.Add(queued)
	for range queued {
		go func() {
			defer pending.Done()
			_ = pool.run(context.Background(), 1, func(context.Context, int) {})
		}()
	}
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		pool.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on saturated fanout queue")
	}
	wg.Wait()
	pending.Wait()
}

func BenchmarkFanoutPool256(b *testing.B) {
	pool := newFanoutPool(goruntime.GOMAXPROCS(0))
	b.Cleanup(pool.close)
	var calls atomic.Uint64
	run := func(context.Context, int) { calls.Add(1) }
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !pool.run(context.Background(), 256, run) {
			b.Fatal("closed fanout pool")
		}
	}
	if calls.Load() != uint64(b.N)*256 {
		b.Fatal("fanout dropped work")
	}
}

func BenchmarkPipelineAddrIndex256(b *testing.B) {
	addrs := make([]*addr, 256)
	for i := range addrs {
		addrs[i] = newAddrWeighted("tcp", stringInt(i), 1)
		addrs[i].slot = i
	}
	want := addrs[len(addrs)-1]
	b.Run("slot", func(b *testing.B) {
		for b.Loop() {
			if pipelineAddrIndex(addrs, want) != len(addrs)-1 {
				b.Fatal("wrong slot")
			}
		}
	})
	want.slot = 0
	b.Run("linear-fallback", func(b *testing.B) {
		for b.Loop() {
			if pipelineAddrIndex(addrs, want) != len(addrs)-1 {
				b.Fatal("wrong fallback slot")
			}
		}
	})
}

func fanoutBenchmarkName(workers int) string {
	if workers == 0 {
		return "per-node"
	}
	return "workers-" + stringInt(workers)
}

func stringInt(n int) string {
	var buf [20]byte
	i := len(buf)
	for {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			return string(buf[i:])
		}
	}
}
