package memcached

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var errPoolClosed = errors.New("connection pool is closed")

// connPool is the idle set for one node: get takes or dials, put parks or closes at the idle cap.
type connPool struct {
	maxIdle, maxOpen int
	maxLifetime      time.Duration
	maxIdleTime      time.Duration

	mu         sync.Mutex
	conns      chan wireConn
	createConn func(ctx context.Context) (wireConn, error)
	open       atomic.Int32
	closed     atomic.Bool
	// closedCh is closed once so get waiters unblock.
	closedCh  chan struct{}
	cleanerCh chan struct{}

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64

	minIdle int
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
	p := &connPool{
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		maxLifetime: maxLifetime,
		maxIdleTime: maxIdleTime,
		conns:       make(chan wireConn, idleCap),
		createConn:  createConn,
		closedCh:    make(chan struct{}),
	}

	return p
}

// close shuts the pool, drains idle sockets, and wakes waiters.
func (p *connPool) close() error {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return nil
	}

	p.closed.Store(true)
	close(p.closedCh)

	// Drain idle sockets; closing the idle channel would race with lock-free get.
	for {
		select {
		case cn := <-p.conns:
			_ = cn.Close()
			p.open.Add(-1)
		default:
			goto drained
		}
	}
drained:

	if p.cleanerCh != nil {
		select {
		case p.cleanerCh <- struct{}{}:
		default:
		}
	}

	p.mu.Unlock()
	return nil
}

// connReusable reports whether cn is still within maxLifetime and maxIdleTime.
func (p *connPool) connReusable(cn wireConn) bool {
	if p.maxLifetime <= 0 && p.maxIdleTime <= 0 {
		return true
	}
	now := clock()
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
	_ = cn.closeSilent()
	p.open.Add(-1)
}

// closeReaped silently closes sockets the cleaner dropped from the idle set.
func (p *connPool) closeReaped(closing []wireConn) {
	for _, cn := range closing {
		_ = cn.closeSilent()
		p.open.Add(-1)
	}
}

// get returns an idle reusable conn or dials a new one, waiting if the pool is at maxOpen.
func (p *connPool) get(ctx context.Context) (wireConn, error) {
	for {
		select {
		case cn := <-p.conns:
			if p.closed.Load() {
				_ = cn.Close()
				p.open.Add(-1)
				return nil, errPoolClosed
			}
			if p.connReusable(cn) {
				return cn, nil
			}
			p.closeIdle(cn)
			continue
		default:
		}

		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if int(p.open.Load()) >= p.maxOpen {
			p.mu.Unlock()
			select {
			case cn := <-p.conns:
				if p.closed.Load() {
					_ = cn.Close()
					p.open.Add(-1)
					return nil, errPoolClosed
				}
				if p.connReusable(cn) {
					return cn, nil
				}
				p.closeIdle(cn)
				continue
			case <-p.closedCh:
				return nil, errPoolClosed
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		p.open.Add(1)
		p.mu.Unlock()

		cn, err := p.createConn(ctx)
		if err != nil {
			p.open.Add(-1)
			return nil, err
		}
		cn.setConnPool(p)

		p.mu.Lock()
		if p.closed.Load() {
			p.mu.Unlock()
			p.open.Add(-1)
			return nil, errors.Join(errPoolClosed, cn.Close())
		}
		p.mu.Unlock()
		return cn, nil
	}
}

// discard closes cn and decrements the open-connection count without returning it to idle.
func (p *connPool) discard(cn wireConn) error {
	if cn == nil {
		return nil
	}
	err := cn.closeSilent()
	p.open.Add(-1)
	return err
}

// put parks cn in the idle set or closes it when the pool is closed or at the idle cap.
func (p *connPool) put(cn wireConn) error {
	if cn == nil {
		panic("pool: put nil connection")
	}
	if p.closed.Load() {
		p.open.Add(-1)
		return cn.Close()
	}
	if p.maxIdle <= 0 {
		p.open.Add(-1)
		p.bumpClosedIdleCap()
		return cn.closeSilent()
	}
	select {
	case p.conns <- cn:
		if p.maxLifetime > 0 || p.maxIdleTime > 0 {
			p.mu.Lock()
			if !p.closed.Load() {
				p.startCleanerLocked()
			}
			p.mu.Unlock()
		}
		return nil
	default:
		p.open.Add(-1)
		p.bumpClosedIdleCap()
		return cn.closeSilent()
	}
}

func (p *connPool) bumpClosedIdleCap() {
	p.mu.Lock()
	p.closedIdleCap++
	p.mu.Unlock()
}

// startCleanerLocked starts one cleaner goroutine under mu to reap aged sockets.
func (p *connPool) startCleanerLocked() {
	if (p.maxLifetime > 0 || p.maxIdleTime > 0) && int(p.open.Load()) > 0 && p.cleanerCh == nil {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionsCleaner(p.shortestIdleTimeLocked())
	}
}

// connectionsCleaner periodically reaps aged idle sockets and restores MinIdle.
func (p *connPool) connectionsCleaner(d time.Duration) {
	const minInterval = 1 * time.Second
	if d < minInterval {
		d = minInterval
	}

	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh:
		}

		p.mu.Lock()
		d = p.shortestIdleTimeLocked()
		if p.closed.Load() || p.open.Load() == 0 || d <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}

		d, closing := p.connectionCleanerRunLocked(d)
		p.mu.Unlock()

		p.closeReaped(closing)
		p.ensureMinIdle(context.Background())

		if d < minInterval {
			d = minInterval
		}

		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}
}

// shortestIdleTimeLocked returns the smaller enabled of maxLifetime and maxIdleTime.
func (p *connPool) shortestIdleTimeLocked() time.Duration {
	if p.maxIdleTime <= 0 {
		return p.maxLifetime
	}
	if p.maxLifetime <= 0 {
		return p.maxIdleTime
	}

	return min(p.maxLifetime, p.maxIdleTime)
}

// connectionCleanerRunLocked drops idle-aged and lifetime-expired sockets and puts survivors back.
func (p *connPool) connectionCleanerRunLocked(d time.Duration) (time.Duration, []wireConn) {
	var idleClosing int64
	closing := make([]wireConn, 0, p.maxIdle/2)
	kept := make([]wireConn, 0, len(p.conns))

	var idleSince, expiredSince time.Time
	checkIdle := p.maxIdleTime > 0
	checkLife := p.maxLifetime > 0
	if checkIdle {
		idleSince = clock().Add(-p.maxIdleTime)
	}
	if checkLife {
		expiredSince = clock().Add(-p.maxLifetime)
	}

	for {
		var c wireConn
		select {
		case c = <-p.conns:
		default:
			goto drained
		}

		shouldClose := false
		if checkIdle {
			if d2, ok := c.idle(idleSince); ok {
				shouldClose = true
				idleClosing++
			} else if d2 < d {
				d = d2
			}
		}
		if !shouldClose && checkLife {
			if d2, ok := c.expired(expiredSince); ok {
				shouldClose = true
			} else if d2 < d {
				d = d2
			}
		}

		if shouldClose {
			closing = append(closing, c)
		} else {
			kept = append(kept, c)
		}
	}

drained:
	for _, c := range kept {
		p.conns <- c
	}

	p.closedIdleAge += idleClosing
	p.closedLifetime += int64(len(closing)) - idleClosing
	return d, closing
}

// connPoolStats is occupancy and eviction counters for one node pool.
type connPoolStats struct {
	TotalConns int
	IdleConns  int
	MaxOpen    int
	MaxIdle    int

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64
}

// stats returns a snapshot of open, idle, and eviction counters.
func (p *connPool) stats() *connPoolStats {
	p.mu.Lock()
	s := &connPoolStats{
		TotalConns:     int(p.open.Load()),
		IdleConns:      len(p.conns),
		MaxOpen:        p.maxOpen,
		MaxIdle:        p.maxIdle,
		closedIdleCap:  p.closedIdleCap,
		closedIdleAge:  p.closedIdleAge,
		closedLifetime: p.closedLifetime,
	}
	p.mu.Unlock()
	return s
}

// ensureMinIdle dials until the idle set reaches minIdle or the open cap is hit.
func (p *connPool) ensureMinIdle(ctx context.Context) {
	if p.minIdle <= 0 {
		return
	}
	for {
		p.mu.Lock()
		if p.closed.Load() || len(p.conns) >= p.minIdle || int(p.open.Load()) >= p.maxOpen {
			p.mu.Unlock()
			return
		}
		p.open.Add(1)
		p.mu.Unlock()
		cn, err := p.createConn(ctx)
		if err != nil {
			p.open.Add(-1)
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}

// warmup dials up to n connections and parks them in the idle set.
func (p *connPool) warmup(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	if n > p.maxIdle && p.maxIdle > 0 {
		n = p.maxIdle
	}
	for i := 0; i < n; i++ {
		p.open.Add(1)
		cn, err := p.createConn(ctx)
		if err != nil {
			p.open.Add(-1)
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}
