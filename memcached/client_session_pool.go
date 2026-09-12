package memcached

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// SessionPool owns a fixed number of Sessions bound to the node selected by
// its key. Concurrent Do calls wait for and exclusively lease one Session.
type SessionPool struct {
	c    *Client
	key  []byte
	size int
	done chan struct{}

	closed  atomic.Bool
	mu      sync.Mutex
	idle    []*Session
	waiters []chan struct{}
}

// NewSessionPool opens size stream sessions for key's current node. It keeps
// those connections checked out until Close. The caller must close the pool.
// UDP does not support session pools. size must not exceed Pool.MaxOpen.
func (c *Client) NewSessionPool(ctx context.Context, key []byte, size int) (*SessionPool, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	if size <= 0 {
		return nil, fmt.Errorf("session pool size must be positive: %w", ErrInvalidArgument)
	}
	maxOpen := 100
	if c.rt != nil && c.rt.maxOpen > 0 {
		maxOpen = c.rt.maxOpen
	}
	if size > maxOpen {
		return nil, fmt.Errorf("session pool size %d exceeds Pool.MaxOpen %d: %w", size, maxOpen, ErrInvalidArgument)
	}
	wired, box := c.nsKey(key)
	err := c.checkKeyValue(wired, nil)
	putBuilderBox(box)
	if err != nil {
		return nil, err
	}
	p := &SessionPool{
		c:    c,
		key:  bytes.Clone(key),
		size: size,
		done: make(chan struct{}),
		idle: make([]*Session, 0, size),
	}
	for range size {
		session, err := c.Session(ctx, p.key)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		p.idle = append(p.idle, session)
	}
	return p, nil
}

// Do exclusively leases one Session until fn returns. Waiting for a Session
// honors ctx. A broken Session is replaced lazily before a later callback.
func (p *SessionPool) Do(ctx context.Context, fn func(*Session) error) error {
	if p == nil || p.c == nil || p.closed.Load() {
		return ErrClosed
	}
	if fn == nil {
		return fmt.Errorf("nil session callback: %w", ErrInvalidArgument)
	}
	session, err := p.take(ctx)
	if err != nil {
		return err
	}
	if p.closed.Load() {
		if session != nil {
			_ = session.Close()
		}
		return ErrClosed
	}
	if session == nil || session.closed.Load() ||
		session.version != p.c.clusterVersion.Load() {
		if session != nil {
			_ = session.Close()
		}
		var err error
		session, err = p.c.Session(ctx, p.key)
		if err != nil {
			p.returnSession(nil)
			return err
		}
	}
	defer func() { p.returnSession(session) }()
	return fn(session)
}

func (p *SessionPool) take(ctx context.Context) (*Session, error) {
	for {
		session, wait, err := func() (*Session, chan struct{}, error) {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.closed.Load() {
				return nil, nil, ErrClosed
			}
			if n := len(p.idle); n > 0 {
				session := p.idle[n-1]
				p.idle[n-1] = nil
				p.idle = p.idle[:n-1]
				return session, nil, nil
			}
			wait := make(chan struct{}, 1)
			p.waiters = append(p.waiters, wait)
			return nil, wait, nil
		}()
		if err != nil {
			return nil, err
		}
		if session != nil {
			return session, nil
		}

		select {
		case <-wait:
			continue
		case <-ctx.Done():
			p.dropWaiter(wait)
			return nil, ctx.Err()
		case <-p.done:
			p.dropWaiter(wait)
			return nil, ErrClosed
		}
	}
}

func (p *SessionPool) dropWaiter(wait chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.waiters {
		if w != wait {
			continue
		}
		p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
		return
	}
}

// Size returns the fixed number of Session leases.
func (p *SessionPool) Size() int {
	if p == nil {
		return 0
	}
	return p.size
}

// Close releases all idle Sessions and prevents new leases. Sessions active in
// Do are released when their callbacks return; Close does not wait for them.
func (p *SessionPool) Close() error {
	if p == nil || !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(p.done)
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for _, session := range p.idle {
		if session != nil {
			if err := session.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	p.idle = nil
	for _, w := range p.waiters {
		select {
		case w <- struct{}{}:
		default:
		}
	}
	p.waiters = nil
	return first
}

func (p *SessionPool) returnSession(session *Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		if session != nil {
			_ = session.Close()
		}
		return
	}
	if session != nil && session.closed.Load() {
		session = nil
	}
	p.idle = append(p.idle, session)
	if len(p.waiters) == 0 {
		return
	}
	w := p.waiters[0]
	p.waiters = p.waiters[1:]
	select {
	case w <- struct{}{}:
	default:
	}
}
