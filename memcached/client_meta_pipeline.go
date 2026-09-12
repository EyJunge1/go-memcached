package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Pipeline runs mixed meta ops, assigns O when missing, appends mn, and fans out by node.
// Replies are matched by opaque. With Pool.SharedStreams > 0, waves share the
// stream's opaque demux with concurrent meta single-ops (write-while-read);
// without SharedStreams, matching stays within each dispatch wave on a
// checked-out connection.
// len(dst) must equal len(ops). Each dst[i] is filled for ops[i]. Value buffers
// with enough cap are reused.
// Transport failures are the method error. Protocol misses and conflicts
// (EN, NF, NS, EX) live on dst[i].Status / dst[i].Err().
// CLIENT_ERROR/SERVER_ERROR after some opaque replies may leave matched
// siblings populated (per-opaque partial success). ErrMalformedResponse and
// transport failures clear that node wave's destinations before returning.
func (m *MetaClient) Pipeline(ctx context.Context, ops []MetaOp, dst []MetaResult) (err error) {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	obs := c.observer()
	if obs != nil {
		start := time.Now()
		nOps := len(ops)
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
	if len(ops) != len(dst) {
		return fmt.Errorf("meta pipeline dst length %d != ops %d: %w", len(dst), len(ops), ErrInvalidArgument)
	}
	if len(ops) == 0 {
		return nil
	}
	for i := range dst {
		_ = takeMetaKeep(&dst[i])
	}

	prepBox := getPreparedMeta(len(ops))
	defer putPreparedMeta(prepBox)
	out := dst
	for i, op := range ops {
		p, err := encodeMetaPipelineOp(c, op, i)
		if err != nil {
			return err
		}
		p.keepUser = out[i].Item.Key
		p.keepOpaque = out[i].Item.Opaque
		*prepBox = append(*prepBox, p)
	}
	prepared := *prepBox

	addrs, picker := c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		return fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
	}
	addrs = c.liveAddrs(addrs)
	if len(addrs) == 1 {
		return c.metaPipelineDispatch(ctx, addrs[0], prepared, out)
	}

	var first *addr
	sameNode := true
	for i := range prepared {
		nodeIndex, addr, err := pickAddrIndex(picker, addrs, prepared[i].key)
		if err != nil {
			return fmt.Errorf("pick node: %w", err)
		}
		prepared[i].nodeIndex = nodeIndex
		if first == nil {
			first = addr
		} else if addr != first {
			sameNode = false
		}
	}
	if sameNode {
		if first == nil {
			return fmt.Errorf("no available address: %w", ErrInvalidAddress)
		}
		return c.metaPipelineDispatch(ctx, first, prepared, out)
	}

	var countStack [32]int
	var offsetStack [33]int
	var counts, offsets []int
	if len(addrs) <= len(countStack) {
		counts = countStack[:len(addrs)]
		offsets = offsetStack[:len(addrs)+1]
	} else {
		counts = make([]int, len(addrs))
		offsets = make([]int, len(addrs)+1)
	}
	for i := range prepared {
		counts[prepared[i].nodeIndex]++
	}
	for i := range addrs {
		offsets[i+1] = offsets[i] + counts[i]
		counts[i] = offsets[i]
	}
	groupBox := getPreparedMeta(len(prepared))
	if cap(*groupBox) < len(prepared) {
		*groupBox = make([]preparedMetaOp, len(prepared))
	}
	*groupBox = (*groupBox)[:len(prepared)]
	defer putPreparedMeta(groupBox)
	grouped := *groupBox
	for i := range prepared {
		group := prepared[i].nodeIndex
		copyOp := prepared[i]
		copyOp.keyBox = nil
		grouped[counts[group]] = copyOp
		counts[group]++
		prepared[i].lineBox = nil
	}

	var (
		mu   sync.Mutex
		errs []error
	)
	active := 0
	for i := range addrs {
		if offsets[i] != offsets[i+1] {
			counts[active] = i
			active++
		}
	}
	c.runFanout(ctx, active, func(runCtx context.Context, groupIndex int) {
		i := counts[groupIndex]
		if err := c.metaPipelineDispatch(runCtx, addrs[i], grouped[offsets[i]:offsets[i+1]], out); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	})
	return errors.Join(errs...)
}
