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

// PreparedMetaBatch is a reusable prepared Meta().Pipeline execution plan. It
// owns validation data, namespaced keys, default set bodies, routing metadata,
// per-node wire batches, and opaque indexes. Result destinations remain
// borrowed. RunValues may supply per-run set bodies without changing the
// captured defaults. A PreparedMetaBatch must not be run concurrently.
// Batches from On().Meta().PreparePipeline stay pinned to that node (hash skipped).
type PreparedMetaBatch struct {
	c   *Client
	ops []preparedMetaOp
	dst []MetaResult

	valueOps      []int
	valueSlot     []int
	defaultValues [][]byte
	defaultWire   bool

	routed  []int
	counts  []int
	offsets []int
	grouped []preparedMetaOp
	raws    [][]byte

	groups         []preparedMetaBatchGroup
	clusterVersion uint64
	healthVersion  uint64

	// pin/pinned force every op onto one membership node (NodeMetaClient path).
	pin    Server
	pinned bool

	running atomic.Bool
	closed  atomic.Bool
}

type preparedMetaBatchGroup struct {
	node  *addr
	ops   []preparedMetaOp
	raw   []byte
	index *metaPipelineIndex
}

// PreparePipeline prepares mixed meta operations for repeated execution.
// The returned batch owns keys, default set bodies, flags, routing metadata,
// and wire bytes. dst and its reusable buffers must remain valid until Close.
func (m *MetaClient) PreparePipeline(ops []MetaOp, dst []MetaResult) (*PreparedMetaBatch, error) {
	c, err := m.boundClient()
	if err != nil || c.closed.Load() {
		return nil, ErrClosed
	}
	return prepareMetaPipeline(c, ops, dst, nil)
}

func prepareMetaPipeline(c *Client, ops []MetaOp, dst []MetaResult, pin *Server) (*PreparedMetaBatch, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	if len(ops) != len(dst) {
		return nil, fmt.Errorf("meta pipeline dst length %d != ops %d: %w",
			len(dst), len(ops), ErrInvalidArgument)
	}
	b := &PreparedMetaBatch{
		c:             c,
		ops:           make([]preparedMetaOp, len(ops)),
		dst:           dst,
		valueSlot:     make([]int, len(ops)),
		valueOps:      make([]int, 0, len(ops)),
		defaultValues: make([][]byte, 0, len(ops)),
	}
	if pin != nil {
		b.pin = *pin
		b.pinned = true
	}
	for i := range b.valueSlot {
		b.valueSlot[i] = -1
	}
	for i := range dst {
		_ = takeMetaKeep(&dst[i])
	}
	for i, op := range ops {
		if op.Set != nil {
			op.Body = bytes.Clone(op.Body)
		}
		prepared, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			b.release()
			return nil, err
		}
		prepared.keepUser = dst[i].Item.Key
		prepared.keepOpaque = dst[i].Item.Opaque
		b.ops[i] = prepared
		if op.Set != nil {
			b.valueSlot[i] = len(b.valueOps)
			b.valueOps = append(b.valueOps, i)
			b.defaultValues = append(b.defaultValues, prepared.body)
		}
	}
	if err := b.refreshRoutes(); err != nil {
		b.release()
		return nil, err
	}
	return b, nil
}

// Run executes the prepared meta batch with its default set bodies. Cluster
// membership or failover changes refresh its routing. Transport and parse
// failures are returned; protocol statuses remain in the corresponding dst.
func (b *PreparedMetaBatch) Run(ctx context.Context) error {
	return b.run(ctx, nil)
}

// RunValues executes the prepared meta batch with replacement Meta Set bodies
// in enqueue order. Values are borrowed only until RunValues returns.
// Transport and parse failures are returned; protocol statuses remain in dst.
func (b *PreparedMetaBatch) RunValues(ctx context.Context, values [][]byte) error {
	if b == nil || b.c == nil || b.closed.Load() {
		return ErrClosed
	}
	if len(values) != len(b.valueOps) {
		return fmt.Errorf("got %d values for %d meta set operations: %w",
			len(values), len(b.valueOps), ErrInvalidArgument)
	}
	for i, opIndex := range b.valueOps {
		if i >= len(values) || opIndex < 0 || opIndex >= len(b.ops) {
			return fmt.Errorf("prepared meta index out of range: %w", ErrInvalidArgument)
		}
		value := values[i] // #nosec G602 -- i bounded above
		op := b.ops[opIndex]
		if err := b.c.checkKeyValueFlags(op.key, value, op.valueBinary); err != nil {
			return err
		}
	}
	return b.run(ctx, values)
}

func (b *PreparedMetaBatch) run(ctx context.Context, values [][]byte) (err error) {
	if b == nil || b.c == nil || b.closed.Load() {
		return ErrClosed
	}
	if b.c.closed.Load() {
		return ErrClosed
	}
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared meta batch is already running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	obs := b.c.observer()
	if obs != nil {
		start := time.Now()
		nOps := len(b.ops)
		defer func() {
			obs.ObserveOp(ctx, OpEvent{
				Op:       OpMetaPipeline,
				Duration: time.Since(start),
				Err:      err,
				Keys:     nOps,
			})
		}()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	for i := range b.dst {
		_ = takeMetaKeep(&b.dst[i])
	}
	if b.clusterVersion != b.c.clusterVersion.Load() ||
		b.healthVersion != b.c.healthVersion.Load() {
		if err := b.refreshRoutes(); err != nil {
			return err
		}
	}
	if values != nil || !b.defaultWire {
		b.rebuildWire(values)
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
		group := &b.groups[i]
		if err := b.runGroup(runCtx, group); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	})
	return errors.Join(errs...)
}

// Close releases retained keys and wire buffers. It returns ErrInvalidArgument
// when a Run is currently active.
func (b *PreparedMetaBatch) Close() error {
	if b == nil || b.closed.Load() {
		return nil
	}
	if !b.running.CompareAndSwap(false, true) {
		return fmt.Errorf("prepared meta batch is running: %w", ErrInvalidArgument)
	}
	defer b.running.Store(false)
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.release()
	return nil
}

func (b *PreparedMetaBatch) runGroup(ctx context.Context, group *preparedMetaBatchGroup) error {
	req := buildRequest(group.ops[0].cmd, group.ops[0].key, group.raw)
	resp := buildQuietBarrierResponse()
	resp.metaPipeC = b.c
	resp.metaPipeDst = b.dst
	resp.metaPipeGroup = group.ops
	resp.metaPipeIndex = group.index
	resetMetaPipeSeen(resp, len(group.ops))
	defer func() {
		// group.raw is retained by the prepared batch, not _requestPool.
		req.raw = nil
		// group.index is retained too; _responsePool must not return it.
		resp.metaPipeIndex = nil
		releaseReqAndResp(req, resp)
	}()
	return b.c.finishMetaPipelineDispatch(ctx, group.node, group.ops, b.dst, req, resp)
}
