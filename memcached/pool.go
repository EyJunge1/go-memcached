package memcached

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var errPoolClosed = errors.New("connection pool is closed")

// connPool is the per-node idle set. get() takes an idle conn or dials up to
// maxOpen. put() returns a conn or closes it when the idle cap is reached.
// A background cleaner drops conns past maxIdleTime or maxLifetime.
type connPool struct {
	maxIdle, maxOpen int
	maxLifetime      time.Duration
	maxIdleTime      time.Duration

	mu         sync.Mutex
	conns      chan wireConn
	createConn func(ctx context.Context) (wireConn, error)
	open       atomic.Int32
	closed     bool
	// closedCh is closed once so waiters in get() abort without a receive
	// on a closed idle channel.
	closedCh  chan struct{}
	cleanerCh chan struct{}

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64

	minIdle int
}

func newConnPool(
	maxIdle, maxOpen int,
	maxLifetime, maxIdleTime time.Duration,
	createConn func(ctx context.Context) (wireConn, error),
) *connPool {
	p := &connPool{
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		maxLifetime: maxLifetime,
		maxIdleTime: maxIdleTime,
		conns:       make(chan wireConn, maxOpen),
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

func (p *connPool) connReusable(cn wireConn) bool {
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

func (p *connPool) closeIdle(cn wireConn) {
	_ = cn.Close()
	p.open.Add(-1)
}

func (p *connPool) get(ctx context.Context) (wireConn, error) {
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
		if int(p.open.Load()) >= p.maxOpen {
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
		p.open.Add(1)
		p.mu.Unlock()

		cn, err := p.createConn(ctx)
		if err != nil {
			p.open.Add(-1)
			return nil, err
		}
		cn.setConnPool(p)

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			p.open.Add(-1)
			return nil, errors.Join(errPoolClosed, cn.Close())
		}
		p.mu.Unlock()
		return cn, nil
	}
}

func (p *connPool) discard(cn wireConn) error {
	if cn == nil {
		return nil
	}
	err := cn.Close()
	p.open.Add(-1)
	return err
}

func (p *connPool) put(cn wireConn) error {
	if cn == nil {
		panic("pool: put nil connection")
	}

	p.mu.Lock()
	maxIdleClose := p.maxIdle <= 0 || len(p.conns) >= p.maxIdle
	if p.closed || (p.maxOpen > 0 && int(p.open.Load()) > p.maxOpen) || maxIdleClose {
		_ = cn.Close()
		p.open.Add(-1)
		if maxIdleClose {
			p.closedIdleCap++
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
		p.open.Add(-1)
		return cn.Close()
	}
}

// startCleanerLocked runs under mu. One goroutine reaps idle/expired conns.
func (p *connPool) startCleanerLocked() {
	if (p.maxLifetime > 0 || p.maxIdleTime > 0) && int(p.open.Load()) > 0 && p.cleanerCh == nil {
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
		case <-p.cleanerCh:
		}

		p.mu.Lock()
		d = p.shortestIdleTimeLocked()
		if p.closed || p.open.Load() == 0 || d <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}

		d, closing := p.connectionCleanerRunLocked(d)
		p.mu.Unlock()

		for _, cn := range closing {
			_ = cn.Close()
			p.open.Add(-1)
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

// connectionCleanerRunLocked drops idle-aged and lifetime-expired conns.
// The idle channel is not closed: drain, filter, put survivors back.
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

type connPoolStats struct {
	TotalConns int
	IdleConns  int
	MaxOpen    int
	MaxIdle    int

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64
}

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

func (p *connPool) ensureMinIdle(ctx context.Context) {
	if p.minIdle <= 0 {
		return
	}
	for {
		p.mu.Lock()
		if p.closed || len(p.conns) >= p.minIdle || int(p.open.Load()) >= p.maxOpen {
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
