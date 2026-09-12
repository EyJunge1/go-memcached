package memcached

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// metaPipeliner runs a quiet meta batch (MetaClient or NodeMetaClient).
type metaPipeliner interface {
	Pipeline(ctx context.Context, ops []MetaOp, dst []MetaResult) error
}

var (
	_ metaPipeliner = (*MetaClient)(nil)
	_ metaPipeliner = (*NodeMetaClient)(nil)
)

// DeferredMeta is an opt-in enqueue-then-wait helper on top of Meta().Pipeline.
// Enqueue ops, then call the returned getters (or Flush). The first Wait/Flush
// publishes the whole batch once. Calling a getter before more enqueues finish
// kills cross-op batching for later ops — prefer Flush after enqueueing all.
// Explicit Meta().Pipeline / PreparePipeline remain the primary APIs.
//
// Key, Body, Opaque, and Extra slices passed to Get/Set/Delete/Arithmetic are
// copied at enqueue so callers may reuse or mutate their buffers afterward.
type DeferredMeta struct {
	pipe metaPipeliner
	ctx  context.Context

	mu      sync.Mutex
	ops     []MetaOp
	dst     []MetaResult
	flushed bool
	err     error
}

// Deferred starts a deferred meta batch bound to ctx.
// A nil or unbound MetaClient yields a handle whose Flush returns ErrClosed.
func (m *MetaClient) Deferred(ctx context.Context) *DeferredMeta {
	if _, err := m.boundClient(); err != nil {
		return &DeferredMeta{ctx: ctx}
	}
	return &DeferredMeta{pipe: m, ctx: ctx}
}

// Get enqueues a quiet meta get and returns a waiter for its result.
// key, f.Opaque, and f.Extra are copied; the caller's buffers may be reused afterward.
func (d *DeferredMeta) Get(key []byte, f MetaGet) func() (MetaResult, error) {
	f.Quiet = true
	f.Opaque = slices.Clone(f.Opaque)
	f.Extra = slices.Clone(f.Extra)
	return d.enqueue(MetaOp{Key: slices.Clone(key), Get: &f})
}

// Set enqueues a quiet meta set and returns a waiter for its result.
// key, value, f.Opaque, and f.Extra are copied; the caller's buffers may be reused afterward.
func (d *DeferredMeta) Set(key, value []byte, f MetaSet) func() (MetaResult, error) {
	f.Quiet = true
	f.Opaque = slices.Clone(f.Opaque)
	f.Extra = slices.Clone(f.Extra)
	return d.enqueue(MetaOp{Key: slices.Clone(key), Body: slices.Clone(value), Set: &f})
}

// Delete enqueues a quiet meta delete and returns a waiter for its result.
// key, f.Opaque, and f.Extra are copied; the caller's buffers may be reused afterward.
func (d *DeferredMeta) Delete(key []byte, f MetaDelete) func() (MetaResult, error) {
	f.Quiet = true
	f.Opaque = slices.Clone(f.Opaque)
	f.Extra = slices.Clone(f.Extra)
	return d.enqueue(MetaOp{Key: slices.Clone(key), Delete: &f})
}

// Arithmetic enqueues a quiet meta arithmetic and returns a waiter for its result.
// key, f.Opaque, and f.Extra are copied; the caller's buffers may be reused afterward.
func (d *DeferredMeta) Arithmetic(key []byte, delta uint64, f MetaArithmetic) func() (MetaResult, error) {
	f.Quiet = true
	f.Opaque = slices.Clone(f.Opaque)
	f.Extra = slices.Clone(f.Extra)
	return d.enqueue(MetaOp{Key: slices.Clone(key), Delta: delta, Arithmetic: &f})
}

func (d *DeferredMeta) enqueue(op MetaOp) func() (MetaResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.flushed {
		return func() (MetaResult, error) {
			return MetaResult{}, fmt.Errorf("deferred meta already flushed: %w", ErrInvalidArgument)
		}
	}
	idx := len(d.ops)
	d.ops = append(d.ops, op)
	d.dst = append(d.dst, MetaResult{})
	return func() (MetaResult, error) {
		if err := d.Flush(); err != nil {
			return MetaResult{}, err
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if idx >= len(d.dst) {
			return MetaResult{}, fmt.Errorf("deferred meta index: %w", ErrInvalidArgument)
		}
		res := d.dst[idx]
		return res, res.Err()
	}
}

// Flush runs the queued Pipeline once. Safe to call repeatedly.
func (d *DeferredMeta) Flush() error {
	if d == nil || d.pipe == nil {
		return ErrClosed
	}
	var ops []MetaOp
	var dst []MetaResult
	var ctx context.Context
	if err, already := func() (error, bool) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.flushed {
			return d.err, true
		}
		ops = d.ops
		dst = d.dst
		ctx = d.ctx
		d.flushed = true
		return nil, false
	}(); already {
		return err
	}

	if len(ops) == 0 {
		return nil
	}
	err := d.pipe.Pipeline(ctx, ops, dst)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
	return err
}
