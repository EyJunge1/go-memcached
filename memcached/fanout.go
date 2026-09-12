package memcached

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

type fanoutPool struct {
	// mu guards closed and coordinates enqueue with close (named, not embedded).
	mu      sync.Mutex
	closed  bool
	enqueue sync.WaitGroup

	// activeMu guards in-flight call cancel funcs.
	activeMu sync.Mutex
	active   map[*fanoutCall]context.CancelCauseFunc

	jobs        chan *fanoutCall // unbuffered (Uber: channel size 0 or 1)
	workerCount int
	workers     sync.WaitGroup
	closing     chan struct{} // unbuffered stop signal
	rootCtx     context.Context
	stopRoot    context.CancelCauseFunc
}

type fanoutCall struct {
	ctx  context.Context
	run  func(context.Context, int)
	n    int64
	next atomic.Int64
	done sync.WaitGroup
}

var _fanoutCallPool = sync.Pool{New: func() any { return &fanoutCall{} }}

func newFanoutPool(workers int) *fanoutPool {
	if workers <= 0 {
		return nil
	}
	rootCtx, stopRoot := context.WithCancelCause(context.Background())
	// Unbuffered jobs channel: senders select on closing for cancel, so Close
	// cannot block behind a saturated buffer (Uber: channel size 0 or 1).
	p := &fanoutPool{
		jobs:        make(chan *fanoutCall),
		workerCount: workers,
		closing:     make(chan struct{}),
		active:      make(map[*fanoutCall]context.CancelCauseFunc, workers),
		rootCtx:     rootCtx,
		stopRoot:    stopRoot,
	}
	for range workers {
		p.workers.Go(func() {
			for call := range p.jobs {
				for {
					raw := call.next.Add(1) - 1
					if raw < 0 || raw >= call.n {
						break
					}
					idx, ok := int64ToInt(raw)
					if !ok {
						break
					}
					call.run(call.ctx, idx)
				}
				call.done.Done()
			}
		})
	}
	return p
}

// beginEnqueue reserves an enqueue slot under mu.
func (p *fanoutPool) beginEnqueue() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.enqueue.Add(1)
	return true
}

func (p *fanoutPool) run(ctx context.Context, n int, run func(context.Context, int)) bool {
	if p == nil || n <= 0 {
		return false
	}
	if !p.beginEnqueue() {
		return false
	}

	select {
	case <-p.closing:
		p.enqueue.Done()
		return false
	default:
	}

	call, ok := _fanoutCallPool.Get().(*fanoutCall)
	if !ok || call == nil {
		call = &fanoutCall{}
	}
	var cancel context.CancelCauseFunc
	if ctx == context.Background() || ctx == context.TODO() {
		call.ctx = p.rootCtx
	} else {
		call.ctx, cancel = context.WithCancelCause(ctx)
	}
	call.run = run
	call.n = int64(n)
	call.next.Store(0)
	func() {
		p.activeMu.Lock()
		defer p.activeMu.Unlock()
		p.active[call] = cancel
	}()
	participants := min(n, p.workerCount)
	call.done.Add(participants)
	sent := 0
	closing := false
	for range participants {
		select {
		case p.jobs <- call:
			sent++
		case <-p.closing:
			for i := sent; i < participants; i++ {
				call.done.Done()
			}
			p.enqueue.Done()
			if sent == 0 {
				func() {
					p.activeMu.Lock()
					defer p.activeMu.Unlock()
					delete(p.active, call)
				}()
				releaseFanoutCall(call, cancel)
				return false
			}
			closing = true
		}
		if closing {
			break
		}
	}
	if !closing {
		p.enqueue.Done()
	}
	call.done.Wait()
	func() {
		p.activeMu.Lock()
		defer p.activeMu.Unlock()
		delete(p.active, call)
	}()
	releaseFanoutCall(call, cancel)
	return true
}

func (p *fanoutPool) closeFanout() {
	if p == nil {
		return
	}
	if !p.markClosed() {
		return
	}

	p.stopRoot(ErrClosed)
	func() {
		p.activeMu.Lock()
		defer p.activeMu.Unlock()
		for _, cancel := range p.active {
			if cancel != nil {
				cancel(ErrClosed)
			}
		}
	}()

	p.enqueue.Wait()
	close(p.jobs)
	p.workers.Wait()
}

// markClosed flips closed under mu; false if already closed.
func (p *fanoutPool) markClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.closed = true
	close(p.closing)
	return true
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

// int64ToInt converts v when it fits in int (used after an explicit range check).
func int64ToInt(v int64) (int, bool) {
	if v < 0 || v > math.MaxInt {
		return 0, false
	}
	return int(v), true
}

func releaseFanoutCall(call *fanoutCall, cancel context.CancelCauseFunc) {
	if cancel != nil {
		cancel(nil)
	}
	call.ctx = nil
	call.run = nil
	call.n = 0
	_fanoutCallPool.Put(call)
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
				if raw < 0 || raw >= int64(n) {
					return
				}
				i, ok := int64ToInt(raw)
				if !ok {
					return
				}
				run(i)
			}
		})
	}
	wg.Wait()
}
