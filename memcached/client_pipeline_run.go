package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// release returns pooled request and response objects for every queued op.
func (p *Pipeline) release() {
	for _, op := range p.ops {
		releaseReqAndResp(op.req, op.resp)
	}
	p.ops = p.ops[:0]
	p.errs = p.errs[:0]
	clear(p.resultDsts)
	clear(p.getDsts)
	clear(p.arithDsts)
	clear(p.routed)
	p.routed = p.routed[:0]
	clear(p.groupedOps)
	p.groupedOps = p.groupedOps[:0]
	clear(p.groupCounts)
	p.groupCounts = p.groupCounts[:0]
	clear(p.groupOffsets)
	p.groupOffsets = p.groupOffsets[:0]
	if cap(p.keyBuf) > _maxPooledBufferCap {
		p.keyBuf = make([]byte, 0, _defaultBufferSize)
	}
	p.keyBuf = p.keyBuf[:0]
}

func (p *Pipeline) routedBuf() []int {
	var routedStack [32]int
	if len(p.ops) <= len(routedStack) {
		return routedStack[:len(p.ops)]
	}
	if cap(p.routed) < len(p.ops) {
		p.routed = make([]int, len(p.ops))
	}
	p.routed = p.routed[:len(p.ops)]
	return p.routed
}

func (p *Pipeline) routeOps(addrs []*addr, picker picker, routed []int) (valid, first int, sameNode bool) {
	for i := range routed {
		routed[i] = -1
	}
	first = -1
	sameNode = true
	for i := range p.ops {
		nodeIndex, _, err := pickAddrIndex(picker, addrs, p.ops[i].key)
		if err != nil {
			err = fmt.Errorf("pick node: %w", err)
			if p.ops[i].result != nil {
				p.ops[i].result.State = PipelineNotExecuted
				p.ops[i].result.Err = err
			}
			p.errs = append(p.errs, err)
			continue
		}
		routed[i] = nodeIndex
		valid++
		if first < 0 {
			first = nodeIndex
		} else if nodeIndex != first {
			sameNode = false
		}
	}
	return valid, first, sameNode
}

func (p *Pipeline) buildOpGroups(addrs []*addr, routed []int, valid int) int {
	if cap(p.groupCounts) < len(addrs) {
		p.groupCounts = make([]int, len(addrs))
	}
	p.groupCounts = p.groupCounts[:len(addrs)]
	clear(p.groupCounts)
	if cap(p.groupOffsets) < len(addrs)+1 {
		p.groupOffsets = make([]int, len(addrs)+1)
	}
	p.groupOffsets = p.groupOffsets[:len(addrs)+1]
	clear(p.groupOffsets)
	if cap(p.groupedOps) < valid {
		p.groupedOps = make([]pipeOp, valid)
	}
	p.groupedOps = p.groupedOps[:valid]
	for _, nodeIndex := range routed {
		if nodeIndex >= 0 {
			p.groupCounts[nodeIndex]++
		}
	}
	activeGroups := 0
	for i := range addrs {
		p.groupOffsets[i+1] = p.groupOffsets[i] + p.groupCounts[i]
		if p.groupCounts[i] > 0 {
			activeGroups++
		}
		p.groupCounts[i] = p.groupOffsets[i]
	}
	for i, group := range routed {
		if group < 0 {
			continue
		}
		p.groupedOps[p.groupCounts[group]] = p.ops[i]
		p.groupCounts[group]++
	}
	return activeGroups
}

func (p *Pipeline) runGrouped(ctx context.Context, addrs []*addr, activeGroups int) []error {
	var mu sync.Mutex
	errs := make([]error, 0, activeGroups)
	runNode := func(node *addr, ops []pipeOp) {
		if err := p.runAddr(ctx, node, ops); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	}
	if activeGroups == 1 {
		for i, node := range addrs {
			if p.groupOffsets[i] != p.groupOffsets[i+1] {
				runNode(node, p.groupedOps[p.groupOffsets[i]:p.groupOffsets[i+1]])
				break
			}
		}
		return errs
	}
	n := 0
	for i := range addrs {
		if p.groupOffsets[i] != p.groupOffsets[i+1] {
			p.groupCounts[n] = i
			n++
		}
	}
	p.c.runFanout(ctx, n, func(runCtx context.Context, group int) {
		i := p.groupCounts[group]
		if err := p.runAddr(runCtx, addrs[i], p.groupedOps[p.groupOffsets[i]:p.groupOffsets[i+1]]); err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	})
	return errs
}

// Run writes every queued frame, then reads every reply. Distinct nodes run concurrently.
// After Run the queue is empty and p may be reused.
func (p *Pipeline) Run(ctx context.Context) (err error) {
	if p == nil || p.c == nil {
		return fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	if p.prepared {
		return fmt.Errorf("prepared pipeline: %w", ErrInvalidArgument)
	}
	nOps := len(p.ops)
	obs := p.c.observer()
	if obs != nil {
		start := time.Now()
		defer func() {
			obs.ObserveOp(ctx, OpEvent{
				Op:       OpPipeline,
				Duration: time.Since(start),
				Err:      err,
				Keys:     nOps,
			})
		}()
	}
	defer p.release()
	if err = ctx.Err(); err != nil {
		failNotExecuted(p.ops, err)
		return err
	}
	if len(p.ops) == 0 {
		return errors.Join(p.errs...)
	}

	addrs, picker := p.c.snapshotCluster()
	if picker == nil || len(addrs) == 0 {
		err := fmt.Errorf("no servers configured: %w", ErrInvalidAddress)
		failNotExecuted(p.ops, err)
		return err
	}
	addrs = p.c.liveAddrs(addrs)
	if len(addrs) == 1 {
		if err := p.runAddr(ctx, addrs[0], p.ops); err != nil {
			return errors.Join(append(p.errs, err)...)
		}
		return errors.Join(p.errs...)
	}

	routed := p.routedBuf()
	valid, first, sameNode := p.routeOps(addrs, picker, routed)
	if valid == len(p.ops) && sameNode {
		if err := p.runAddr(ctx, addrs[first], p.ops); err != nil {
			return errors.Join(append(p.errs, err)...)
		}
		return errors.Join(p.errs...)
	}

	activeGroups := p.buildOpGroups(addrs, routed, valid)
	errs := p.runGrouped(ctx, addrs, activeGroups)
	return errors.Join(append(p.errs, errs...)...)
}

// shouldDiscardPipelineConn reports whether a pipeline error must drop the socket.
func shouldDiscardPipelineConn(err error) bool {
	return shouldDiscardConn(err)
}

func finishPipelineConn(cn wireConn, err error) {
	if shouldDiscardPipelineConn(err) {
		if pool := cn.getConnPool(); pool != nil {
			_ = pool.discard(cn)
			return
		}
		_ = cn.closeSilent()
		return
	}
	_ = cn.release()
}

func pipelineAddrIndex(addrs []*addr, want *addr) (int, error) {
	if want != nil && want.slot >= 0 && want.slot < len(addrs) && addrs[want.slot] == want {
		return want.slot, nil
	}
	for i, node := range addrs {
		if node == want {
			return i, nil
		}
	}
	return -1, fmt.Errorf("picker returned an address outside its snapshot: %w", ErrInvalidAddress)
}
