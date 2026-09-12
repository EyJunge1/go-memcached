package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Session exclusively holds one pooled stream connection for sequential
// operations on a single routed node. It must not be used concurrently.
// Client.Close does not reclaim the checkout; the caller must still call
// Session.Close. A failover eject can make an operation return
// ErrInvalidAddress without closing the node-sticky Session.
type Session struct {
	c       *Client
	node    *addr
	cn      wireConn
	version uint64

	running atomic.Bool
	closed  atomic.Bool
}

// Session checks out a connection for key's current node. Every later key must
// route to that node. Close returns a healthy connection to its pool. UDP does
// not support stream sessions.
func (c *Client) Session(ctx context.Context, key []byte) (*Session, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	wired, box := c.nsKey(key)
	defer putBuilderBox(box)
	if err := validateKeyAndValue(wired, nil); err != nil {
		return nil, err
	}
	node, err := c.pickNode(wired)
	if err != nil {
		return nil, err
	}
	if isUDPNetwork(node.Network) {
		return nil, fmt.Errorf("udp session: %w", ErrNotSupported)
	}
	cn, err := c.getConn(ctx, node)
	if err != nil {
		c.noteFailure(node)
		return nil, wrapTransport(fmt.Errorf("alloc connection: %w", err))
	}
	return &Session{
		c: c, node: node, cn: cn, version: c.clusterVersion.Load(),
	}, nil
}

// Server returns the node held by the session.
func (s *Session) Server() Server {
	if s == nil || s.node == nil {
		return Server{}
	}
	return addrToServer(s.node)
}

// Run executes and releases a Pipeline on the held connection. Every queued
// key must route to this Session's node.
func (s *Session) Run(ctx context.Context, p *Pipeline) (err error) {
	if p == nil {
		return fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	nOps := len(p.ops)
	if s != nil && s.c != nil {
		if obs := s.c.observer(); obs != nil {
			start := time.Now()
			defer func() {
				obs.ObserveOp(ctx, OpEvent{
					Op:       OpPipeline,
					Server:   serverFromAddr(s.node),
					Duration: time.Since(start),
					Err:      err,
					Keys:     nOps,
				})
			}()
		}
	}
	defer p.release()
	if s == nil || s.c == nil {
		failNotExecuted(p.ops, ErrClosed)
		return ErrClosed
	}
	if p.c == nil || p.c != s.c || p.prepared {
		return fmt.Errorf("pipeline belongs to another execution mode: %w", ErrInvalidArgument)
	}
	if err = s.begin(); err != nil {
		failNotExecuted(p.ops, err)
		return err
	}
	defer s.running.Store(false)
	if err = ctx.Err(); err != nil {
		failNotExecuted(p.ops, err)
		return err
	}
	if err = s.validateOps(p.ops); err != nil {
		failNotExecuted(p.ops, err)
		return errors.Join(append(p.errs, err)...)
	}
	if len(p.ops) == 0 {
		return errors.Join(p.errs...)
	}
	runErr := p.runConn(ctx, s.node, s.cn, p.ops)
	s.afterRun(runErr)
	return errors.Join(append(p.errs, runErr)...)
}

// RunPrepared executes a single-node PreparedBatch on the held connection,
// avoiding connection checkout, validation, encoding, and routing in the
// steady state.
func (s *Session) RunPrepared(ctx context.Context, b *PreparedBatch) error {
	return s.runPrepared(ctx, b, nil)
}

// RunPreparedValues executes a PreparedBatch with borrowed replacement
// storage values while retaining this session's dedicated connection.
func (s *Session) RunPreparedValues(ctx context.Context, b *PreparedBatch, values [][]byte) error {
	if b == nil || b.p == nil || b.closed.Load() {
		return fmt.Errorf("invalid prepared batch: %w", ErrInvalidArgument)
	}
	if len(values) != len(b.valueOps) {
		return fmt.Errorf("got %d values for %d storage operations: %w",
			len(values), len(b.valueOps), ErrInvalidArgument)
	}
	for i, opIndex := range b.valueOps {
		if i >= len(values) || opIndex < 0 || opIndex >= len(b.p.ops) {
			return fmt.Errorf("prepared storage index out of range: %w", ErrInvalidArgument)
		}
		if err := b.c.checkKeyValue(b.p.ops[opIndex].key, values[i]); err != nil { // #nosec G602 -- i/opIndex bounded above
			return err
		}
	}
	return s.runPrepared(ctx, b, values)
}

func (s *Session) runPrepared(ctx context.Context, b *PreparedBatch, values [][]byte) error {
	if s == nil || s.c == nil {
		return ErrClosed
	}
	if b == nil || b.c != s.c || b.p == nil || b.closed.Load() {
		return fmt.Errorf("prepared batch belongs to another client: %w", ErrInvalidArgument)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.running.Store(false)
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared batch is already running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	if values != nil {
		for i, opIndex := range b.valueOps {
			b.p.ops[opIndex].req.body = values[i]
		}
		defer b.restoreDefaultValues()
	}
	if err := b.updateValueLengths(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	resetPreparedBatch(b.p.ops)
	if b.clusterVersion != b.c.clusterVersion.Load() ||
		b.healthVersion != b.c.healthVersion.Load() {
		if err := b.refreshRoutes(); err != nil {
			failNotExecuted(b.p.ops, err)
			return err
		}
	}
	if len(b.groups) != 1 || b.groups[0].node.id() != s.node.id() {
		err := fmt.Errorf("prepared batch does not belong to session node: %w", ErrInvalidArgument)
		failNotExecuted(b.p.ops, err)
		return err
	}
	group := &b.groups[0]
	var err error
	if group.frames == nil {
		err = b.p.runConn(ctx, s.node, s.cn, group.ops)
	} else {
		err = b.p.runFrames(ctx, s.node, s.cn, group.ops, group.frames.frames)
	}
	s.afterRun(err)
	return err
}
