package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var errPoolClosed = errors.New("connection pool is closed")

// connPool is the idle set for one node: get takes or dials, put parks or closes at the idle cap.
// Idle reuse is LIFO (hot end) so recently used sockets stay warm.
type connPool struct {
	// mu guards idle, waitCh, cleanerCh, and the closed* / idle* counters below.
	// Named field (not embedded) per Uber: Zero-value Mutexes are Valid.
	mu sync.Mutex

	maxIdle, maxOpen int
	maxLifetime      time.Duration
	maxIdleTime      time.Duration

	idle       []wireConn // LIFO stack; last element is hottest
	createConn func(ctx context.Context) (wireConn, error)
	open       atomic.Int32
	closed     atomic.Bool
	// closedCh is closed once so get waiters unblock (unbuffered).
	closedCh chan struct{}
	// cleanerCh wakes the idle reaper; size 1 so close/put never block on send.
	cleanerCh chan struct{}
	cleanerWG sync.WaitGroup
	// waitCh wakes MaxOpen waiters after open-- or an idle push (unbuffered;
	// closed and replaced under mu). Non-nil only while at least one get waits.
	waitCh    chan struct{}
	hasWaiter atomic.Bool

	// lifeCtx is canceled by close so Cleaner/Warmup dials cannot outlive the pool.
	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64
	idleHits       int64
	idleMisses     int64

	minIdle int

	server    Server
	onAcquire func(PoolEvent) // MaxOpen slot-wait only
	now       clockFunc
}

// newConnPool constructs an empty idle pool that dials through createConn.
func newConnPool(
	maxIdle, maxOpen int,
	maxLifetime, maxIdleTime time.Duration,
	createConn func(ctx context.Context) (wireConn, error),
) *connPool {
	idleCap := max(maxIdle, 1)
	if maxOpen > 0 && idleCap > maxOpen {
		idleCap = maxOpen
	}
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	p := &connPool{
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		maxLifetime: maxLifetime,
		maxIdleTime: maxIdleTime,
		idle:        make([]wireConn, 0, idleCap),
		createConn:  createConn,
		closedCh:    make(chan struct{}),
		lifeCtx:     lifeCtx,
		lifeCancel:  lifeCancel,
		now:         time.Now,
	}

	return p
}

// mergeDialContext returns a context canceled when either the caller context or
// the pool lifecycle ends. The returned cancel must be called.
func (p *connPool) mergeDialContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	if p.lifeCtx == nil {
		return ctx, cancel
	}
	stop := context.AfterFunc(p.lifeCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// close shuts the pool, drains idle sockets, and wakes waiters.
func (p *connPool) closePool() error {
	if alreadyClosed := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed.Load() {
			return true
		}

		p.closed.Store(true)
		close(p.closedCh)
		if p.lifeCancel != nil {
			p.lifeCancel()
		}

		p.drainIdleLocked()
		p.signalWaitersLocked()

		if p.cleanerCh != nil {
			select {
			case p.cleanerCh <- struct{}{}:
			default:
			}
		}
		return false
	}(); alreadyClosed {
		return nil
	}
	p.cleanerWG.Wait()
	return nil
}

// drainIdleLocked closes every currently parked socket. Caller must hold p.mu.
func (p *connPool) drainIdleLocked() {
	for i := range p.idle {
		if cn := p.idle[i]; cn != nil {
			_ = cn.Close()
			p.open.Add(-1)
		}
		p.idle[i] = nil
	}
	p.idle = p.idle[:0]
}

func (p *connPool) popIdleLocked() (wireConn, bool) {
	for {
		n := len(p.idle)
		if n == 0 {
			return nil, false
		}
		cn := p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
		if cn != nil {
			return cn, true
		}
	}
}

func (p *connPool) pushIdleLocked(cn wireConn) bool {
	if p.maxIdle <= 0 || len(p.idle) >= p.maxIdle {
		return false
	}
	p.idle = append(p.idle, cn)
	p.signalWaitersLocked()
	return true
}

// connReusable reports whether cn is still within maxLifetime and maxIdleTime.
func (p *connPool) connReusable(cn wireConn) bool {
	if cn == nil {
		return false
	}
	if p.maxLifetime <= 0 && p.maxIdleTime <= 0 {
		return true
	}
	now := resolveClock(p.now)()
	if p.maxLifetime > 0 {
		if _, expired := cn.expired(now.Add(-p.maxLifetime)); expired {
			return false
		}
	}
	if p.maxIdleTime > 0 {
		if _, idle := cn.idle(now.Add(-p.maxIdleTime)); idle {
			return false
		}
	}
	return true
}

// closeIdle closes cn and decrements the open-connection count.
func (p *connPool) closeIdle(cn wireConn) {
	if cn == nil {
		return
	}
	_ = cn.closeSilent()
	p.releaseOpen()
}

// closeReaped silently closes sockets the cleaner dropped from the idle set.
func (p *connPool) closeReaped(closing []wireConn) {
	for _, cn := range closing {
		if cn == nil {
			continue
		}
		_ = cn.closeSilent()
		p.releaseOpen()
	}
}

// releaseOpen decrements open and wakes MaxOpen waiters.
func (p *connPool) releaseOpen() {
	p.open.Add(-1)
	if !p.hasWaiter.Load() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.signalWaitersLocked()
}

// signalWaitersLocked wakes every get waiting for a free MaxOpen slot or idle conn.
func (p *connPool) signalWaitersLocked() {
	if p.waitCh != nil {
		close(p.waitCh)
		p.waitCh = nil
	}
	p.hasWaiter.Store(false)
}

// reserveOpenSlotLocked increments open when under MaxOpen and the pool is open.
// Caller must hold p.mu. Returns false when closed or at capacity.
func (p *connPool) reserveOpenSlotLocked() bool {
	if p.closed.Load() || int(p.open.Load()) >= p.maxOpen {
		return false
	}
	p.open.Add(1)
	return true
}

// get returns an idle reusable conn or dials a new one, waiting if the pool is at maxOpen.
// OnAcquire fires only for MaxOpen slot waits (not idle hits or dials).
func (p *connPool) get(ctx context.Context) (wireConn, error) {
	return p.getLocked(ctx)
}

// getLocked checks out an idle conn or dials under MaxOpen.
func (p *connPool) getLocked(ctx context.Context) (wireConn, error) {
	for {
		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		for {
			cn, ok := p.popIdleLocked()
			if !ok {
				break
			}
			if p.connReusable(cn) {
				p.idleHits++
				p.mu.Unlock()
				return cn, nil
			}
			p.mu.Unlock()
			p.closeIdle(cn)
			p.mu.Lock()
			if p.closed.Load() {
				p.mu.Unlock()
				return nil, errPoolClosed
			}
		}

		if int(p.open.Load()) >= p.maxOpen {
			if p.waitCh == nil {
				p.waitCh = make(chan struct{})
			}
			p.hasWaiter.Store(true)
			waitCh := p.waitCh
			p.mu.Unlock()
			waitStart := resolveClock(p.now)()
			select {
			case <-waitCh:
				if p.onAcquire != nil {
					p.onAcquire(PoolEvent{Server: p.server, Duration: resolveClock(p.now)().Sub(waitStart)})
				}
				continue
			case <-p.closedCh:
				if p.onAcquire != nil {
					p.onAcquire(PoolEvent{Server: p.server, Err: errPoolClosed, Duration: resolveClock(p.now)().Sub(waitStart)})
				}
				return nil, errPoolClosed
			case <-ctx.Done():
				if p.onAcquire != nil {
					p.onAcquire(PoolEvent{Server: p.server, Err: ctx.Err(), Duration: resolveClock(p.now)().Sub(waitStart)})
				}
				return nil, ctx.Err()
			}
		}
		if !p.reserveOpenSlotLocked() {
			p.mu.Unlock()
			continue
		}
		p.idleMisses++
		p.mu.Unlock()

		dialCtx, cancel := p.mergeDialContext(ctx)
		cn, err := p.createConn(dialCtx)
		cancel()
		if err != nil {
			p.releaseOpen()
			if p.closed.Load() {
				return nil, errPoolClosed
			}
			return nil, err
		}
		cn.setConnPool(p)

		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			p.releaseOpen()
			return nil, errors.Join(errPoolClosed, cn.Close())
		}
		p.mu.Unlock()
		return cn, nil
	}
}

// discard closes cn and decrements the open-connection count without returning
// it to idle. Every checkout must reach exactly one of discard or put.
func (p *connPool) discard(cn wireConn) error {
	if cn == nil {
		return nil
	}
	err := cn.closeSilent()
	p.releaseOpen()
	return err
}

// put parks cn in the idle set (LIFO) or closes it when the pool is closed or at the
// idle cap. Every checkout must reach exactly one of put or discard.
func (p *connPool) put(cn wireConn) error {
	if cn == nil {
		return fmt.Errorf("pool put nil connection: %w", ErrInvalidArgument)
	}
	if p.closed.Load() {
		p.releaseOpen()
		return cn.Close()
	}
	if p.maxIdle <= 0 {
		p.releaseOpen()
		p.bumpClosedIdleCap()
		return cn.closeSilent()
	}
	p.mu.Lock()
	if p.closed.Load() {
		p.drainIdleLocked()
		p.mu.Unlock()
		p.releaseOpen()
		return cn.Close()
	}
	if !p.pushIdleLocked(cn) {
		p.closedIdleCap++
		p.mu.Unlock()
		p.releaseOpen()
		return cn.closeSilent()
	}
	if p.maxLifetime > 0 || p.maxIdleTime > 0 {
		p.startCleanerLocked()
	}
	p.mu.Unlock()
	return nil
}
