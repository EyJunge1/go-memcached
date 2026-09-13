package memcached

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var errPoolClosed = errors.New("connection pool is closed")

// The connPool holds a pool of connections to one memcached server instance
// and provides a way to get a connection from the pool.
//
// It will automatically create a new connection if the pool is empty or all
// connections are busy, of course, within the maximum connection limit. It also
// automatically removes idle connections if the idle connection count exceeds
// the maximum idle connection limit.
//
// Idle connections are connections that are not used for a certain period
// of time decided by the maxIdleTime.
//
// The pool is thread-safe.
type connPool struct {
	maxIdle, maxOpen int
	maxLifetime      time.Duration
	maxIdleTime      time.Duration

	mu         sync.Mutex // guards following
	conns      chan memcachedConn
	createConn func(ctx context.Context) (memcachedConn, error)
	// The number of connections numOpen by the pool.
	numOpen atomic.Int32
	// Indicate if the pool is closed, if true, no new connections will be created
	// and all existing connections will be closed.
	closed bool
	// closedCh is closed exactly once when the pool closes, so waiting get()
	// callers can abort without receiving from a closed idle channel.
	closedCh  chan struct{}
	cleanerCh chan struct{}

	maxIdleClosed     int64 // the number of connections closed due to maxIdle
	maxIdleTimeClosed int64 // the number of connections closed due to maxIdleTime
	maxLifetimeClosed int64 // the number of connections closed due to maxLifetime

	minIdle int
}

func newConnPool(
	maxIdle, maxOpen int,
	maxLifetime, maxIdleTime time.Duration,
	createConn func(ctx context.Context) (memcachedConn, error),
) *connPool {
	p := &connPool{
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		maxLifetime: maxLifetime,
		maxIdleTime: maxIdleTime,
		conns:       make(chan memcachedConn, maxOpen),
		createConn:  createConn,
		closedCh:    make(chan struct{}),
	}

	return p
}

func (p *connPool) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}

	p.closed = true
	close(p.closedCh)

	// Drain idle connections without closing the idle channel itself.
	// Closing the channel would race with lock-free get() receives.
	for {
		select {
		case cn := <-p.conns:
			_ = cn.Close()
			p.numOpen.Add(-1)
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

func (p *connPool) connReusable(cn memcachedConn) bool {
	now := nowFunc()
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

func (p *connPool) closeIdle(cn memcachedConn) {
	_ = cn.Close()
	p.numOpen.Add(-1)
}

func (p *connPool) get(ctx context.Context) (memcachedConn, error) {
	for {
		select {
		case cn := <-p.conns:
			if p.connReusable(cn) {
				return cn, nil
			}
			p.closeIdle(cn)
			continue
		default:
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if int(p.numOpen.Load()) >= p.maxOpen {
			p.mu.Unlock()
			select {
			case cn := <-p.conns:
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
		p.numOpen.Add(1)
		p.mu.Unlock()

		cn, err := p.createConn(ctx)
		if err != nil {
			p.numOpen.Add(-1)
			return nil, err
		}
		cn.setConnPool(p)

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			p.numOpen.Add(-1)
			return nil, errors.Join(errPoolClosed, cn.Close())
		}
		p.mu.Unlock()
		return cn, nil
	}
}

func (p *connPool) discard(cn memcachedConn) error {
	if cn == nil {
		return nil
	}
	err := cn.Close()
	p.numOpen.Add(-1)
	return err
}

func (p *connPool) put(cn memcachedConn) error {
	if cn == nil {
		panic("pool: put nil connection")
	}

	p.mu.Lock()
	maxIdleClose := p.maxIdle > 0 && len(p.conns) >= p.maxIdle
	if p.closed || (p.maxOpen > 0 && int(p.numOpen.Load()) > p.maxOpen) || maxIdleClose {
		_ = cn.Close()
		p.numOpen.Add(-1)
		if maxIdleClose {
			p.maxIdleClosed++
		}

		p.mu.Unlock()
		return nil
	}

	select {
	case p.conns <- cn:
		p.startCleanerLocked()
		p.mu.Unlock()
		return nil
	default:
		p.mu.Unlock()
		// rare case, the pool is full
		p.numOpen.Add(-1)
		return cn.Close()
	}
}

// startCleanerLocked starts a cleaner goroutine to clean up expired connections.
// NOTE: MUST run in the connPool.mu.Lock()
func (p *connPool) startCleanerLocked() {
	if (p.maxLifetime > 0 || p.maxIdleTime > 0) && int(p.numOpen.Load()) > 0 && p.cleanerCh == nil {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionsCleaner(p.shortestIdleTimeLocked())
	}
}

func (p *connPool) connectionsCleaner(d time.Duration) {
	const minInterval = 1 * time.Second
	if d < minInterval {
		d = minInterval
	}

	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh: // pool was closed
		}

		p.mu.Lock()
		// This step wants to adjust the cleaner interval to the shortest idle time
		// among maxIdleTime and maxLifetime. Avoid maxIdleTime or maxLifetime is
		// much longer than the cleaner interval.
		d = p.shortestIdleTimeLocked()
		if p.closed || p.numOpen.Load() == 0 || d <= 0 {
			// stop the cleaner if the pool is closed or empty, or there's no need to clean up.
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}

		// Snapshot idle connections that need to be closed.
		d, closing := p.connectionCleanerRunLocked(d)
		p.mu.Unlock()

		for _, cn := range closing {
			_ = cn.Close()
			p.numOpen.Add(-1)
		}
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

func (p *connPool) shortestIdleTimeLocked() time.Duration {
	if p.maxIdleTime <= 0 {
		return p.maxLifetime
	}
	if p.maxLifetime <= 0 {
		return p.maxIdleTime
	}

	return min(p.maxLifetime, p.maxIdleTime)
}

// connectionCleanerRunLocked will remove two class connections:
//
// 1. if the connection is expired (exceeds maxLifetime since created).
// 2. if the connection idle time exceeds the idle connection limit(maxIdleTime).
//
// The idle channel is kept intact: currently available connections are drained
// with non-blocking receives, filtered in one pass, and survivors are put back.
func (p *connPool) connectionCleanerRunLocked(d time.Duration) (time.Duration, []memcachedConn) {
	var idleClosing int64
	closing := make([]memcachedConn, 0, p.maxIdle/2)
	kept := make([]memcachedConn, 0, len(p.conns))

	var idleSince, expiredSince time.Time
	checkIdle := p.maxIdleTime > 0
	checkLife := p.maxLifetime > 0
	if checkIdle {
		idleSince = nowFunc().Add(-p.maxIdleTime)
	}
	if checkLife {
		expiredSince = nowFunc().Add(-p.maxLifetime)
	}

	for {
		var c memcachedConn
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
				// Ensure idle connections are cleaned up as soon as possible.
				d = d2
			}
		}
		if !shouldClose && checkLife {
			if d2, ok := c.expired(expiredSince); ok {
				shouldClose = true
			} else if d2 < d {
				// Prevents connections staying in the pool when they have expired.
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

	p.maxIdleTimeClosed += idleClosing
	p.maxLifetimeClosed += int64(len(closing)) - idleClosing
	return d, closing
}

type connPoolStats struct {
	TotalConns int
	IdleConns  int
	MaxOpen    int
	MaxIdle    int

	maxIdleClosed     int64
	maxIdleTimeClosed int64
	maxLifetimeClosed int64
}

func (p *connPool) stats() *connPoolStats {
	p.mu.Lock()
	s := &connPoolStats{
		TotalConns:        int(p.numOpen.Load()),
		IdleConns:         len(p.conns),
		MaxOpen:           p.maxOpen,
		MaxIdle:           p.maxIdle,
		maxIdleClosed:     p.maxIdleClosed,
		maxIdleTimeClosed: p.maxIdleTimeClosed,
		maxLifetimeClosed: p.maxLifetimeClosed,
	}
	p.mu.Unlock()
	return s
}

func (p *connPool) ensureMinIdle(ctx context.Context) {
	if p.minIdle <= 0 {
		return
	}
	for {
		p.mu.Lock()
		if p.closed || len(p.conns) >= p.minIdle || int(p.numOpen.Load()) >= p.maxOpen {
			p.mu.Unlock()
			return
		}
		p.numOpen.Add(1)
		p.mu.Unlock()
		cn, err := p.createConn(ctx)
		if err != nil {
			p.numOpen.Add(-1)
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}

func (p *connPool) warmup(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	if n > p.maxIdle && p.maxIdle > 0 {
		n = p.maxIdle
	}
	for i := 0; i < n; i++ {
		p.numOpen.Add(1)
		cn, err := p.createConn(ctx)
		if err != nil {
			p.numOpen.Add(-1)
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}
