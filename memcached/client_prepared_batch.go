package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PreparedBatch is a reusable prepared Pipeline execution plan. It owns the
// queued keys, default storage values, and wire metadata; result destinations
// remain borrowed. RunValues may supply per-run values without changing the
// captured defaults. A PreparedBatch must not be run concurrently.
type PreparedBatch struct {
	c *Client
	p *Pipeline

	groups         []preparedBatchGroup
	valueOps       []int
	defaultValues  [][]byte
	clusterVersion uint64
	healthVersion  uint64

	running atomic.Bool
	closed  atomic.Bool
}

type preparedBatchGroup struct {
	node   *addr
	ops    []pipeOp
	frames *pipelineFrameScratch
}

// Prepare transfers this Pipeline's queued operations into a PreparedBatch.
// The Pipeline must not be used afterward. Storage values are copied as the
// defaults; destination pointers must remain valid until the batch is closed.
func (p *Pipeline) Prepare() (*PreparedBatch, error) {
	if p == nil || p.c == nil {
		return nil, fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	if p.prepared {
		return nil, fmt.Errorf("pipeline already prepared: %w", ErrInvalidArgument)
	}
	if err := errors.Join(p.errs...); err != nil {
		p.release()
		return nil, err
	}
	p.prepared = true
	for i := range p.ops {
		if isPreparedValueOp(p.ops[i].req) {
			p.ops[i].req.body = bytes.Clone(p.ops[i].req.body)
		}
	}
	b := &PreparedBatch{c: p.c, p: p}
	b.valueOps = make([]int, 0, len(p.ops))
	b.defaultValues = make([][]byte, 0, len(p.ops))
	for i := range p.ops {
		if isPreparedValueOp(p.ops[i].req) {
			b.valueOps = append(b.valueOps, i)
			b.defaultValues = append(b.defaultValues, p.ops[i].req.body)
		}
	}
	if err := b.refreshRoutes(); err != nil {
		b.release()
		return nil, err
	}
	return b, nil
}

// Run executes the prepared plan. Cluster and failover changes trigger a
// route refresh; the normal case reuses validation, encoding, and grouping.
func (b *PreparedBatch) Run(ctx context.Context) error {
	return b.run(ctx, nil)
}

// RunValues executes the prepared plan while borrowing replacement values for
// its storage operations, in enqueue order. Values are only borrowed until
// RunValues returns. The keys, flags, expiration, CAS, and result destinations
// remain those captured by Prepare.
func (b *PreparedBatch) RunValues(ctx context.Context, values [][]byte) error {
	if b == nil || b.p == nil || b.closed.Load() {
		return ErrClosed
	}
	if len(values) != len(b.valueOps) {
		return fmt.Errorf("got %d values for %d storage operations: %w",
			len(values), len(b.valueOps), ErrInvalidArgument)
	}
	for i, opIndex := range b.valueOps {
		if i >= len(values) || opIndex < 0 || opIndex >= len(b.p.ops) {
			return fmt.Errorf("prepared storage index out of range: %w", ErrInvalidArgument)
		}
		value := values[i] // #nosec G602 -- i bounded above
		op := b.p.ops[opIndex]
		if err := b.c.checkKeyValue(op.key, value); err != nil {
			return err
		}
	}
	return b.run(ctx, values)
}

func (b *PreparedBatch) run(ctx context.Context, values [][]byte) (err error) {
	if b == nil || b.c == nil || b.p == nil || b.closed.Load() {
		return ErrClosed
	}
	if b.c.closed.Load() {
		return ErrClosed
	}
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared batch is already running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	obs := b.c.observer()
	if obs != nil {
		start := time.Now()
		nOps := len(b.p.ops)
		defer func() {
			obs.ObserveOp(ctx, OpEvent{
				Op:       OpPipeline,
				Duration: time.Since(start),
				Err:      err,
				Keys:     nOps,
			})
		}()
	}
	if values != nil {
		for i, opIndex := range b.valueOps {
			b.p.ops[opIndex].req.body = values[i]
		}
		defer b.restoreDefaultValues()
	}
	if err = b.updateValueLengths(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	if err = ctx.Err(); err != nil {
		failNotExecuted(b.p.ops, err)
		return err
	}
	resetPreparedBatch(b.p.ops)
	if b.clusterVersion != b.c.clusterVersion.Load() ||
		b.healthVersion != b.c.healthVersion.Load() {
		if err = b.refreshRoutes(); err != nil {
			failNotExecuted(b.p.ops, err)
			return err
		}
	}
	if len(b.groups) == 0 {
		return nil
	}
	if len(b.groups) == 1 {
		return b.runGroup(ctx, &b.groups[0])
	}

	var (
		mu   sync.Mutex
		errs []error
	)
	b.c.runFanout(ctx, len(b.groups), func(runCtx context.Context, i int) {
		if err := b.runGroup(runCtx, &b.groups[i]); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	})
	return errors.Join(errs...)
}

// Close releases retained wire buffers. It returns ErrInvalidArgument when a
// Run is currently active.
func (b *PreparedBatch) Close() error {
	if b == nil || b.closed.Load() {
		return nil
	}
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared batch is running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.release()
	return nil
}

func (b *PreparedBatch) release() {
	if b == nil || b.p == nil {
		return
	}
	b.releaseGroups()
	b.p.release()
	b.p = nil
	b.c = nil
	b.valueOps = nil
	b.defaultValues = nil
}

func (b *PreparedBatch) releaseGroups() {
	for i := range b.groups {
		releasePipelineFrames(b.groups[i].frames)
		b.groups[i] = preparedBatchGroup{}
	}
	b.groups = b.groups[:0]
}

func (b *PreparedBatch) runGroup(ctx context.Context, group *preparedBatchGroup) error {
	if group.frames == nil {
		return b.p.runAddr(ctx, group.node, group.ops)
	}
	if isUDPNetwork(group.node.Network) {
		err := fmt.Errorf("udp pipeline: %w", ErrNotSupported)
		failNotExecuted(group.ops, err)
		return err
	}
	run := func(ctx context.Context, cn wireConn) error {
		return b.p.runFrames(ctx, group.node, cn, group.ops, group.frames.frames)
	}
	if b.c.streamCount() > 0 {
		var started bool
		err := b.c.dispatchWaveViaStream(ctx, group.node, func(ctx context.Context, cn wireConn) error {
			started = true
			return run(ctx, cn)
		})
		if err == nil || !errors.Is(err, errNoStream) {
			if err != nil && !started {
				failNotExecuted(group.ops, err)
			}
			return err
		}
	}
	cn, err := b.c.getConn(ctx, group.node)
	if err != nil {
		err = wrapTransport(fmt.Errorf("alloc connection: %w", err))
		failNotExecuted(group.ops, err)
		return err
	}
	var opErr error
	defer func() { finishPipelineConn(cn, opErr) }()
	opErr = run(ctx, cn)
	return opErr
}
