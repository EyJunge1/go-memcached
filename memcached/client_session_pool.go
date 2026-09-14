package memcached

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// SessionPool owns a fixed number of node-bound Sessions for concurrent
// workers. Each callback receives exclusive use of one session.
type SessionPool struct {
	c    *Client
	key  []byte
	size int
	idle chan *Session
	done chan struct{}

	closed atomic.Bool
	mu     sync.Mutex
}

// NewSessionPool opens size stream sessions for key's current node. The pool
// keeps those connections checked out until Close and rejects UDP.
func (c *Client) NewSessionPool(ctx context.Context, key []byte, size int) (*SessionPool, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	if size <= 0 {
		return nil, fmt.Errorf("session pool size must be positive: %w", ErrInvalidArgument)
	}
	if err := c.checkKeyValue(key, nil); err != nil {
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

// Do leases one session until fn returns. A session broken by fn is replaced
// lazily before the next callback. Waiting for a worker honors ctx.
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

// Size returns the fixed number of worker slots.
func (p *SessionPool) Size() int {
	if p == nil {
		return 0
	}
	return p.size
}

// Close releases all currently idle sessions. Sessions active in Do are
// released automatically when their callbacks return.
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
