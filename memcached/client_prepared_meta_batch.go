package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

// PreparedMetaBatch is a reusable prepared Meta().Pipeline execution plan. It
// owns validation data, namespaced keys, default set bodies, routing metadata,
// per-node wire batches, and opaque indexes. Result destinations remain
// borrowed. RunValues may supply per-run set bodies without changing the
// captured defaults. A PreparedMetaBatch must not be run concurrently.
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
	if m == nil || m.c == nil {
		return nil, ErrClosed
	}
	if len(ops) != len(dst) {
		return nil, fmt.Errorf("meta pipeline dst length %d != ops %d: %w",
			len(dst), len(ops), ErrInvalidArgument)
	}
	b := &PreparedMetaBatch{
		c:         m.c,
		ops:       make([]preparedMetaOp, len(ops)),
		dst:       dst,
		valueSlot: make([]int, len(ops)),
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
		prepared, err := encodeMetaPipelineOp(m.c, op, i)
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
		// #nosec G602 -- equal lengths are checked above and valueOps is internal.
		if err := b.c.checkKeyValueFlags(
			b.ops[opIndex].key, values[i], b.ops[opIndex].valueBinary,
		); err != nil {
			return err
		}
	}
	return b.run(ctx, values)
}

func (b *PreparedMetaBatch) run(ctx context.Context, values [][]byte) error {
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
	if err := ctx.Err(); err != nil {
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
			errs = append(errs, err)
			mu.Unlock()
		}
	})
	return errors.Join(errs...)
}

// Close releases retained keys and wire buffers.
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
	defer func() {
		// group.raw is retained by the prepared batch, not requestPool.
		req.raw = nil
		// group.index is retained too; responsePool must not return it.
		resp.metaPipeIndex = nil
		releaseReqAndResp(req, resp)
	}()
	return b.c.finishMetaPipelineDispatch(ctx, group.node, group.ops, b.dst, req, resp)
}

func (b *PreparedMetaBatch) refreshRoutes() error {
	for {
		clusterVersion := b.c.clusterVersion.Load()
		healthVersion := b.c.healthVersion.Load()
		addrs, picker := b.c.snapshotCluster()
		if picker == nil || len(addrs) == 0 {
			return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
		}
		addrs = b.c.liveAddrs(addrs)

		if len(b.ops) == 0 {
			releasePreparedMetaGroups(b)
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}
		if cap(b.routed) < len(b.ops) {
			b.routed = make([]int, len(b.ops))
		} else {
			b.routed = b.routed[:len(b.ops)]
		}
		if cap(b.counts) < len(addrs) {
			b.counts = make([]int, len(addrs))
			b.offsets = make([]int, len(addrs)+1)
		} else {
			b.counts = b.counts[:len(addrs)]
			b.offsets = b.offsets[:len(addrs)+1]
			clear(b.counts)
			clear(b.offsets)
		}
		for i := range b.ops {
			nodeIndex, _, err := pickAddrIndex(picker, addrs, b.ops[i].key)
			if err != nil {
				return fmt.Errorf("pick node failed: %w", err)
			}
			b.routed[i] = nodeIndex
			b.counts[nodeIndex]++
		}
		for i := range addrs {
			b.offsets[i+1] = b.offsets[i] + b.counts[i]
			b.counts[i] = b.offsets[i]
		}
		if cap(b.grouped) < len(b.ops) {
			b.grouped = make([]preparedMetaOp, len(b.ops))
		} else {
			b.grouped = b.grouped[:len(b.ops)]
		}
		for i, group := range b.routed {
			b.grouped[b.counts[group]] = b.ops[i]
			b.counts[group]++
		}
		if clusterVersion != b.c.clusterVersion.Load() ||
			healthVersion != b.c.healthVersion.Load() {
			continue
		}
		if b.groups == nil {
			b.groups = make([]preparedMetaBatchGroup, 0, len(addrs))
			b.raws = make([][]byte, 0, len(addrs))
		}
		releasePreparedMetaGroups(b)
		for i, node := range addrs {
			start, end := b.offsets[i], b.offsets[i+1]
			if start == end {
				continue
			}
			ops := b.grouped[start:end]
			index, err := buildMetaPipelineIndex(ops)
			if err != nil {
				releasePreparedMetaGroups(b)
				return err
			}
			raw := b.takeRaw()
			if raw == nil {
				raw = encodePreparedMetaBatch(ops)
			} else {
				raw = encodePreparedMetaBatchInto(raw[:0], ops)
			}
			b.groups = append(b.groups, preparedMetaBatchGroup{
				node: node, ops: ops, raw: raw, index: index,
			})
		}
		b.defaultWire = true
		b.clusterVersion = clusterVersion
		b.healthVersion = healthVersion
		return nil
	}
}

func encodePreparedMetaBatch(ops []preparedMetaOp) []byte {
	var barrier MetaNoop
	need := 0
	for i := range ops {
		need += len(ops[i].line) + len(ops[i].body) + len(ops[i].tail)
		if ops[i].noop {
			barrier = ops[i].barrier
		}
	}
	need += metaQuietBarrierLen(barrier)
	return encodePreparedMetaBatchInto(make([]byte, 0, need), ops)
}

func encodePreparedMetaBatchInto(raw []byte, ops []preparedMetaOp) []byte {
	var barrier MetaNoop
	for i := range ops {
		raw = append(raw, ops[i].line...)
		raw = append(raw, ops[i].body...)
		raw = append(raw, ops[i].tail...)
		if ops[i].noop {
			barrier = ops[i].barrier
		}
	}
	return appendMetaQuietBarrier(raw, barrier)
}

func (b *PreparedMetaBatch) rebuildWire(values [][]byte) {
	for i := range b.groups {
		group := &b.groups[i]
		group.raw = encodePreparedMetaBatchValues(
			group.raw[:0], group.ops, b.valueSlot, b.defaultValues, values,
		)
	}
	b.defaultWire = values == nil
}

func encodePreparedMetaBatchValues(
	raw []byte,
	ops []preparedMetaOp,
	valueSlot []int,
	defaultValues, values [][]byte,
) []byte {
	var barrier MetaNoop
	for i := range ops {
		op := &ops[i]
		body := op.body
		if slot := valueSlot[op.idx]; slot >= 0 {
			body = defaultValues[slot]
			if values != nil {
				body = values[slot]
			}
			raw = appendMetaSetHeaderLength(raw, op.line, len(body))
		} else {
			raw = append(raw, op.line...)
		}
		raw = append(raw, body...)
		raw = append(raw, op.tail...)
		if op.noop {
			barrier = op.barrier
		}
	}
	return appendMetaQuietBarrier(raw, barrier)
}

func appendMetaSetHeaderLength(dst, line []byte, valueLen int) []byte {
	commandEnd := bytes.IndexByte(line, _SpaceByte)
	if commandEnd < 0 {
		return append(dst, line...)
	}
	keyEndRel := bytes.IndexByte(line[commandEnd+1:], _SpaceByte)
	if keyEndRel < 0 {
		return append(dst, line...)
	}
	tokenStart := commandEnd + 1 + keyEndRel + 1
	tokenEndRel := bytes.IndexByte(line[tokenStart:], _SpaceByte)
	tokenEnd := len(line) - len(_CRLFBytes)
	if tokenEndRel >= 0 {
		tokenEnd = tokenStart + tokenEndRel
	}
	dst = append(dst, line[:tokenStart]...)
	dst = strconv.AppendInt(dst, int64(valueLen), 10)
	return append(dst, line[tokenEnd:]...)
}

func releasePreparedMetaGroups(b *PreparedMetaBatch) {
	if b == nil {
		return
	}
	for i := range b.groups {
		putMetaPipelineIndex(b.groups[i].index)
		if b.groups[i].raw != nil {
			b.raws = append(b.raws, b.groups[i].raw[:0])
		}
		b.groups[i] = preparedMetaBatchGroup{}
	}
	b.groups = b.groups[:0]
}

func (b *PreparedMetaBatch) takeRaw() []byte {
	if len(b.raws) == 0 {
		return nil
	}
	last := len(b.raws) - 1
	raw := b.raws[last]
	b.raws[last] = nil
	b.raws = b.raws[:last]
	return raw
}

func (b *PreparedMetaBatch) release() {
	if b == nil {
		return
	}
	for i := range b.ops {
		putBuilderBox(b.ops[i].lineBox)
		putBuilderBox(b.ops[i].keyBox)
		b.ops[i] = preparedMetaOp{}
	}
	b.ops = nil
	b.dst = nil
	releasePreparedMetaGroups(b)
	b.groups = nil
	b.valueOps = nil
	b.valueSlot = nil
	b.defaultValues = nil
	b.routed = nil
	b.counts = nil
	b.offsets = nil
	b.grouped = nil
	b.raws = nil
	b.c = nil
}
