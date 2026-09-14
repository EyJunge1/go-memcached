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

// PreparedBatch is an immutable, reusable Pipeline plan. Preparation owns
// queued keys, values, and wire frames; result destinations remain borrowed.
// A PreparedBatch must not be run concurrently.
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

// Prepare transfers this Pipeline's queued operations into a reusable plan.
// The Pipeline must not be used afterward. Store values are copied once;
// destination pointers must remain valid until the PreparedBatch is closed.
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
		// #nosec G602 -- equal lengths are checked above and valueOps is internal.
		if err := b.c.checkKeyValue(b.p.ops[opIndex].key, values[i]); err != nil {
			return err
		}
	}
	return b.run(ctx, values)
}

func (b *PreparedBatch) run(ctx context.Context, values [][]byte) error {
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
			errs = append(errs, err)
			mu.Unlock()
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
	cn, err := b.c.getConn(ctx, group.node)
	if err != nil {
		err = wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
		failNotExecuted(group.ops, err)
		return err
	}
	var opErr error
	defer finishPipelineConn(cn, &opErr)
	opErr = b.p.runFrames(ctx, group.node, cn, group.ops, group.frames.frames)
	return opErr
}

func (b *PreparedBatch) refreshRoutes() error {
	for {
		clusterVersion := b.c.clusterVersion.Load()
		healthVersion := b.c.healthVersion.Load()
		addrs, picker := b.c.snapshotCluster()
		if picker == nil || len(addrs) == 0 {
			return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
		}
		addrs = b.c.liveAddrs(addrs)

		if len(b.p.ops) == 0 {
			b.releaseGroups()
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}
		if len(addrs) == 1 {
			if clusterVersion != b.c.clusterVersion.Load() ||
				healthVersion != b.c.healthVersion.Load() {
				continue
			}
			b.releaseGroups()
			b.groups = append(b.groups, preparedBatchGroup{node: addrs[0], ops: b.p.ops})
			_, b.groups[0].frames = pipelineFrames(b.groups[0].ops)
			b.clusterVersion = clusterVersion
			b.healthVersion = healthVersion
			return nil
		}

		p := b.p
		if cap(p.routed) < len(p.ops) {
			p.routed = make([]int, len(p.ops))
		} else {
			p.routed = p.routed[:len(p.ops)]
		}
		if cap(p.groupCounts) < len(addrs) {
			p.groupCounts = make([]int, len(addrs))
			p.groupOffsets = make([]int, len(addrs)+1)
		} else {
			p.groupCounts = p.groupCounts[:len(addrs)]
			p.groupOffsets = p.groupOffsets[:len(addrs)+1]
			clear(p.groupCounts)
			clear(p.groupOffsets)
		}
		for i := range p.ops {
			nodeIndex, _, err := pickAddrIndex(picker, addrs, p.ops[i].key)
			if err != nil {
				return fmt.Errorf("pick node failed: %w", err)
			}
			p.routed[i] = nodeIndex
			p.groupCounts[nodeIndex]++
		}
		for i := range addrs {
			p.groupOffsets[i+1] = p.groupOffsets[i] + p.groupCounts[i]
			p.groupCounts[i] = p.groupOffsets[i]
		}
		if cap(p.groupedOps) < len(p.ops) {
			p.groupedOps = make([]pipeOp, len(p.ops))
		} else {
			p.groupedOps = p.groupedOps[:len(p.ops)]
		}
		for i, group := range p.routed {
			p.groupedOps[p.groupCounts[group]] = p.ops[i]
			p.groupCounts[group]++
		}
		if clusterVersion != b.c.clusterVersion.Load() ||
			healthVersion != b.c.healthVersion.Load() {
			continue
		}
		b.releaseGroups()
		for i, node := range addrs {
			start, end := p.groupOffsets[i], p.groupOffsets[i+1]
			if start != end {
				b.groups = append(b.groups, preparedBatchGroup{
					node: node,
					ops:  p.groupedOps[start:end],
				})
			}
		}
		for i := range b.groups {
			_, b.groups[i].frames = pipelineFrames(b.groups[i].ops)
		}
		b.clusterVersion = clusterVersion
		b.healthVersion = healthVersion
		return nil
	}
}

func resetPreparedBatch(ops []pipeOp) {
	for i := range ops {
		op := &ops[i]
		if op.result != nil {
			*op.result = PipelineResult{}
		}
		if op.getDst != nil {
			resetGetResult(op.getDst)
		}
		if op.arithDst != nil {
			*op.arithDst = 0
		}
		if op.resp != nil {
			for j := range op.resp.rawLines {
				op.resp.rawLines[j] = nil
			}
			op.resp.rawLines = op.resp.rawLines[:0]
			op.resp.arithValue = 0
		}
	}
}

func (b *PreparedBatch) restoreDefaultValues() {
	for i, opIndex := range b.valueOps {
		b.p.ops[opIndex].req.body = b.defaultValues[i]
	}
}

func (b *PreparedBatch) updateValueLengths() error {
	for _, opIndex := range b.valueOps {
		req := b.p.ops[opIndex].req
		if err := patchStorageValueLength(req, len(req.body)); err != nil {
			return err
		}
	}
	return nil
}

func isPreparedValueOp(req *request) bool {
	if req == nil {
		return false
	}
	return bytes.Equal(req.cmd, _SetBytes) ||
		bytes.Equal(req.cmd, _AddBytes) ||
		bytes.Equal(req.cmd, _ReplaceBytes) ||
		bytes.Equal(req.cmd, _AppendBytes) ||
		bytes.Equal(req.cmd, _PrependBytes) ||
		bytes.Equal(req.cmd, _CasBytes)
}

// patchStorageValueLength updates the ASCII bytes field without rebuilding the
// command. Prepared request buffers normally have enough spare capacity even
// when the decimal field grows.
func patchStorageValueLength(req *request, valueLen int) error {
	if req == nil || len(req.raw) < 3 || !bytes.HasSuffix(req.raw, _CRLFBytes) {
		return fmt.Errorf("invalid prepared storage frame: %w", ErrInvalidArgument)
	}
	tokenEnd := len(req.raw) - len(_CRLFBytes)
	if bytes.Equal(req.cmd, _CasBytes) {
		casStart := bytes.LastIndexByte(req.raw[:tokenEnd], _SpaceByte)
		if casStart < 0 {
			return fmt.Errorf("invalid prepared cas frame: %w", ErrInvalidArgument)
		}
		tokenEnd = casStart
	}
	tokenStart := bytes.LastIndexByte(req.raw[:tokenEnd], _SpaceByte) + 1
	if tokenStart <= 0 || tokenStart >= tokenEnd {
		return fmt.Errorf("invalid prepared storage length: %w", ErrInvalidArgument)
	}
	var digits [20]byte
	encoded := strconv.AppendInt(digits[:0], int64(valueLen), 10)
	oldLen := tokenEnd - tokenStart
	newTotal := len(req.raw) + len(encoded) - oldLen
	if newTotal > cap(req.raw) {
		raw := make([]byte, newTotal)
		copy(raw, req.raw[:tokenStart])
		copy(raw[tokenStart:], encoded)
		copy(raw[tokenStart+len(encoded):], req.raw[tokenEnd:])
		req.raw = raw
	} else {
		oldTotal := len(req.raw)
		req.raw = req.raw[:newTotal]
		copy(req.raw[tokenStart+len(encoded):], req.raw[tokenEnd:oldTotal])
		copy(req.raw[tokenStart:], encoded)
	}
	if req.rawBox != nil {
		*req.rawBox = req.raw
	}
	return nil
}
