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
	idle chan *Session
	done chan struct{}

	closed atomic.Bool
	mu     sync.Mutex
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
		idle: make(chan *Session, size),
		done: make(chan struct{}),
	}
	for range size {
		session, err := c.Session(ctx, p.key)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		p.idle <- session
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
	var session *Session
	select {
	case session = <-p.idle:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrClosed
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
	for range p.size {
		select {
		case session := <-p.idle:
			if session != nil {
				if err := session.Close(); err != nil && first == nil {
					first = err
				}
			}
		default:
			return first
		}
	}
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
	p.idle <- session
}
