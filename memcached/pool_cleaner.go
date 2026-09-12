package memcached

import (
	"context"
	"time"
)

func (p *connPool) bumpClosedIdleCap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closedIdleCap++
}

// startCleanerLocked starts one cleaner goroutine under mu to reap aged sockets.
// The goroutine exits on closedCh or when the pool is empty / ages are disabled;
// close waits on cleanerWG (Uber: no fire-and-forget goroutines).
func (p *connPool) startCleanerLocked() {
	if (p.maxLifetime > 0 || p.maxIdleTime > 0) && int(p.open.Load()) > 0 && p.cleanerCh == nil {
		// Buffer 1: non-blocking wake from put/close (Uber: channel size 0 or 1).
		p.cleanerCh = make(chan struct{}, 1)
		p.cleanerWG.Add(1)
		go p.connectionsCleaner(p.shortestIdleTimeLocked())
	}
}

// connectionsCleaner periodically reaps aged idle sockets and restores MinIdle.
func (p *connPool) connectionsCleaner(d time.Duration) {
	defer p.cleanerWG.Done()
	const minInterval = 1 * time.Second
	if d < minInterval {
		d = minInterval
	}

	t := time.NewTimer(d)
	defer t.Stop()

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh:
		case <-p.closedCh:
			p.mu.Lock()
			p.cleanerCh = nil
			p.mu.Unlock()
			return
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
		p.ensureMinIdle(p.lifeCtx)

		if p.closed.Load() {
			p.mu.Lock()
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}

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

// connectionCleanerRunLocked drops idle-aged and lifetime-expired sockets and keeps survivors.
func (p *connPool) connectionCleanerRunLocked(d time.Duration) (time.Duration, []wireConn) {
	var idleClosing int64
	closing := make([]wireConn, 0, p.maxIdle/2)
	kept := make([]wireConn, 0, len(p.idle))

	var idleSince, expiredSince time.Time
	checkIdle := p.maxIdleTime > 0
	checkLife := p.maxLifetime > 0
	if checkIdle {
		idleSince = resolveClock(p.now)().Add(-p.maxIdleTime)
	}
	if checkLife {
		expiredSince = resolveClock(p.now)().Add(-p.maxLifetime)
	}

	for _, c := range p.idle {
		if c == nil {
			continue
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
	for i := range p.idle {
		p.idle[i] = nil
	}
	p.idle = p.idle[:0]
	for _, c := range kept {
		if !p.pushIdleLocked(c) {
			closing = append(closing, c)
			p.closedIdleCap++
		}
	}

	expiredClosing := len(closing)
	p.closedIdleAge += idleClosing
	p.closedLifetime += int64(expiredClosing) - idleClosing
	return d, closing
}

// connPoolStats is occupancy and eviction counters for one node pool.
type connPoolStats struct {
	TotalConns int
	IdleConns  int
	MaxOpen    int
	MaxIdle    int

	IdleHits   int64
	IdleMisses int64

	closedIdleCap  int64
	closedIdleAge  int64
	closedLifetime int64
}

// stats returns a snapshot of open, idle, and eviction counters.
func (p *connPool) stats() *connPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &connPoolStats{
		TotalConns:     int(p.open.Load()),
		IdleConns:      len(p.idle),
		MaxOpen:        p.maxOpen,
		MaxIdle:        p.maxIdle,
		IdleHits:       p.idleHits,
		IdleMisses:     p.idleMisses,
		closedIdleCap:  p.closedIdleCap,
		closedIdleAge:  p.closedIdleAge,
		closedLifetime: p.closedLifetime,
	}
}

// ensureMinIdle dials until the idle set reaches minIdle or the open cap is hit.
func (p *connPool) ensureMinIdle(ctx context.Context) {
	if p.minIdle <= 0 || p.maxIdle <= 0 {
		return
	}
	for {
		p.mu.Lock()
		if p.closed.Load() || len(p.idle) >= p.minIdle || !p.reserveOpenSlotLocked() {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		dialCtx, cancel := p.mergeDialContext(ctx)
		cn, err := p.createConn(dialCtx)
		cancel()
		if err != nil {
			p.releaseOpen()
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}

// warmup dials up to n connections and parks them in the idle set.
// Slots are reserved under the same MaxOpen rule as get/ensureMinIdle.
func (p *connPool) warmup(ctx context.Context, n int) {
	if n <= 0 || p.maxIdle <= 0 {
		return
	}
	if n > p.maxIdle && p.maxIdle > 0 {
		n = p.maxIdle
	}
	if n > p.maxOpen {
		n = p.maxOpen
	}
	for i := 0; i < n; i++ {
		p.mu.Lock()
		if !p.reserveOpenSlotLocked() {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		dialCtx, cancel := p.mergeDialContext(ctx)
		cn, err := p.createConn(dialCtx)
		cancel()
		if err != nil {
			p.releaseOpen()
			return
		}
		cn.setConnPool(p)
		if err := p.put(cn); err != nil {
			return
		}
	}
}
