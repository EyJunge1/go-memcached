package memcached

import (
	"context"
	"sync"
	"sync/atomic"
)

type fanoutPool struct {
	jobs        chan *fanoutCall
	workerCount int

	mu      sync.RWMutex
	workers sync.WaitGroup
	closed  bool

	activeMu sync.Mutex
	active   map[*fanoutCall]context.CancelCauseFunc
	rootCtx  context.Context
	stopRoot context.CancelCauseFunc
}

type fanoutCall struct {
	ctx  context.Context
	run  func(context.Context, int)
	n    int64
	next atomic.Int64
	done sync.WaitGroup
}

var fanoutCallPool = sync.Pool{New: func() any { return new(fanoutCall) }}

func newFanoutPool(workers int) *fanoutPool {
	if workers <= 0 {
		return nil
	}
	rootCtx, stopRoot := context.WithCancelCause(context.Background())
	p := &fanoutPool{
		jobs:        make(chan *fanoutCall, workers*4),
		workerCount: workers,
		active:      make(map[*fanoutCall]context.CancelCauseFunc),
		rootCtx:     rootCtx,
		stopRoot:    stopRoot,
	}
	for range workers {
		p.workers.Go(func() {
			for call := range p.jobs {
				for {
					raw := call.next.Add(1) - 1
					if raw >= call.n {
						break
					}
					// #nosec G115 -- raw is non-negative and below int-sized n.
					call.run(call.ctx, int(raw))
				}
				call.done.Done()
			}
		})
	}
	return p
}

func (p *fanoutPool) run(ctx context.Context, n int, run func(context.Context, int)) bool {
	if p == nil || n <= 0 {
		return false
	}
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return false
	}
	call := fanoutCallPool.Get().(*fanoutCall)
	var cancel context.CancelCauseFunc
	if ctx == context.Background() || ctx == context.TODO() {
		call.ctx = p.rootCtx
	} else {
		call.ctx, cancel = context.WithCancelCause(ctx)
	}
	call.run = run
	call.n = int64(n)
	call.next.Store(0)
	p.activeMu.Lock()
	p.active[call] = cancel
	p.activeMu.Unlock()
	participants := min(n, p.workerCount)
	call.done.Add(participants)
	for range participants {
		p.jobs <- call
	}
	p.mu.RUnlock()
	call.done.Wait()
	p.activeMu.Lock()
	delete(p.active, call)
	p.activeMu.Unlock()
	if cancel != nil {
		cancel(nil)
	}
	call.ctx = nil
	call.run = nil
	call.n = 0
	fanoutCallPool.Put(call)
	return true
}

func (p *fanoutPool) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.jobs)
	p.mu.Unlock()
	p.stopRoot(ErrClosed)
	p.activeMu.Lock()
	for _, cancel := range p.active {
		if cancel != nil {
			cancel(ErrClosed)
		}
	}
	p.activeMu.Unlock()
	p.workers.Wait()
}

func (c *Client) runFanout(ctx context.Context, n int, run func(context.Context, int)) {
	if n <= 0 {
		return
	}
	if c != nil && c.fanout != nil {
		if c.fanout.run(ctx, n, run) {
			return
		}
	}
	workers := 0
	if c != nil && c.rt != nil {
		workers = c.rt.fanoutWorkers
	}
	runFanout(n, workers, func(i int) { run(ctx, i) })
}

// runFanout runs every index once. workers <= 0 preserves the minimum-latency
// goroutine-per-node path; a positive limit avoids O(nodes) live goroutines.
func runFanout(n, workers int, run func(int)) {
	if n <= 0 {
		return
	}
	if n == 1 {
		run(0)
		return
	}
	var wg sync.WaitGroup
	if workers <= 0 || workers >= n {
		for i := range n {
			wg.Go(func() { run(i) })
		}
		wg.Wait()
		return
	}

	var next atomic.Int64
	for range workers {
		wg.Go(func() {
			for {
				raw := next.Add(1) - 1
				if raw >= int64(n) {
					return
				}
				// #nosec G115 -- raw is non-negative and strictly less than int-sized n.
				i := int(raw)
				run(i)
			}
		})
	}
	wg.Wait()
}
