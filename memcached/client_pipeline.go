package memcached

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Pipeline queues text commands, then writes every frame and reads every reply. Quiet mutations belong on Meta().Pipeline.
type Pipeline struct {
	c            *Client
	ops          []pipeOp
	errs         []error
	keyBuf       []byte
	nextGetGroup uint64
	resultDsts   map[*PipelineResult]struct{}
	getDsts      map[*GetResult]struct{}
	arithDsts    map[*uint64]struct{}
	routed       []int
	groupedOps   []pipeOp
	groupCounts  []int
	groupOffsets []int
	prepared     bool
}

// PipelineState describes how far one queued operation progressed.
type PipelineState uint8

const (
	// PipelineNotExecuted means no frame is known to have reached the server.
	PipelineNotExecuted PipelineState = iota
	// PipelineInvalid means local validation rejected the operation.
	PipelineInvalid
	// PipelineUnknown means the frame may have executed but no complete reply was read.
	PipelineUnknown
	// PipelineDone means the complete server reply was read. Err may still report
	// a nominal protocol outcome such as ErrNotStored or ErrExists.
	PipelineDone
)

// PipelineResult receives the outcome of one queued operation.
// Reuse it across runs to avoid allocations, but do not attach the same
// destination to more than one operation in a run.
type PipelineResult struct {
	State PipelineState
	Err   error
}

// pipeOp is one queued text Pipeline frame.
type pipeOp struct {
	req      *request
	resp     *response
	key      []byte
	getDst   *GetResult
	arithDst *uint64
	result   *PipelineResult

	getGroup   uint64
	getCommand string
	getExpiry  Exptime
	getWithCAS bool
}

// Pipeline begins a write-then-read text batch. Meta commands use Meta().Pipeline.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{c: c, keyBuf: make([]byte, 0, defaultBufferSize)}
}

// wireKey returns a key owned by the pipeline until Run completes.
// This keeps routing and the already-encoded wire frame consistent if the
// caller reuses its key buffer before Run.
func (p *Pipeline) wireKey(key []byte) (wired []byte, box *[]byte) {
	if p.c.hasNS() {
		return p.c.nsKey(key)
	}
	start := len(p.keyBuf)
	p.keyBuf = append(p.keyBuf, key...)
	return p.keyBuf[start:], nil
}

func (p *Pipeline) pipelineResult(dst []*PipelineResult) (*PipelineResult, bool) {
	if len(dst) == 0 || dst[0] == nil {
		return nil, true
	}
	out := dst[0]
	p.ensureAliasIndexes()
	if p.resultDsts != nil {
		if _, exists := p.resultDsts[out]; exists {
			p.errs = append(p.errs, fmt.Errorf(
				"duplicate pipeline result destination: %w", ErrInvalidArgument,
			))
			return nil, false
		}
		*out = PipelineResult{}
		return out, true
	}
	for i := range p.ops {
		if p.ops[i].result == out {
			p.errs = append(p.errs, fmt.Errorf(
				"duplicate pipeline result destination: %w", ErrInvalidArgument,
			))
			return nil, false
		}
	}
	*out = PipelineResult{}
	return out, true
}

func failPipelineResult(dst *PipelineResult, err error) {
	if dst != nil {
		dst.State = PipelineInvalid
		dst.Err = err
	}
}

func failNotExecuted(ops []pipeOp, err error) {
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.State = PipelineNotExecuted
			ops[i].result.Err = err
		}
	}
}

func failUnknown(ops []pipeOp, err error) {
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.State = PipelineUnknown
			ops[i].result.Err = err
		}
	}
}

func invalidatePipelineResults(dst []PipelineResult, err error) {
	for i := range dst {
		dst[i].State = PipelineInvalid
		dst[i].Err = err
	}
}

func (p *Pipeline) hasGetDst(dst *GetResult) bool {
	p.ensureAliasIndexes()
	if p.getDsts != nil {
		_, exists := p.getDsts[dst]
		return exists
	}
	for i := range p.ops {
		if p.ops[i].getDst == dst {
			return true
		}
	}
	return false
}

func (p *Pipeline) hasArithmeticDst(dst *uint64) bool {
	p.ensureAliasIndexes()
	if p.arithDsts != nil {
		_, exists := p.arithDsts[dst]
		return exists
	}
	for i := range p.ops {
		if p.ops[i].arithDst == dst {
			return true
		}
	}
	return false
}

const pipelineAliasIndexThreshold = 16

func (p *Pipeline) ensureAliasIndexes() {
	if p.resultDsts != nil || len(p.ops) < pipelineAliasIndexThreshold {
		return
	}
	p.resultDsts = make(map[*PipelineResult]struct{}, len(p.ops))
	p.getDsts = make(map[*GetResult]struct{}, len(p.ops))
	p.arithDsts = make(map[*uint64]struct{}, len(p.ops))
	for i := range p.ops {
		op := &p.ops[i]
		if op.result != nil {
			p.resultDsts[op.result] = struct{}{}
		}
		if op.getDst != nil {
			p.getDsts[op.getDst] = struct{}{}
		}
		if op.arithDst != nil {
			p.arithDsts[op.arithDst] = struct{}{}
		}
	}
}

func (p *Pipeline) appendOp(op pipeOp) {
	p.ops = append(p.ops, op)
	if p.resultDsts == nil {
		return
	}
	if op.result != nil {
		p.resultDsts[op.result] = struct{}{}
	}
	if op.getDst != nil {
		p.getDsts[op.getDst] = struct{}{}
	}
	if op.arithDst != nil {
		p.arithDsts[op.arithDst] = struct{}{}
	}
}

// Store enqueues one text storage command. Item.Quiet is ErrInvalidArgument.
// If result is supplied, Run stores this operation's outcome there.
// Item.Value is borrowed and must not be mutated until Run returns.
func (p *Pipeline) Store(item Item, result ...*PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	if item.Quiet {
		err := fmt.Errorf("pipeline Quiet: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	command, err := storeCommand(item.Mode)
	if err != nil {
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	key, keyBox := p.wireKey(item.Key)
	if err := p.c.checkKeyValue(key, item.Value); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	flags, exp := item.Flags, item.Exp
	if item.Mode == StoreAppend || item.Mode == StorePrepend {
		flags, exp = 0, 0
	}
	var request *request
	var resp *response
	if item.Mode == StoreCAS {
		request, resp = buildCasCommand(key, item.Value, flags, exp, item.CAS, false)
	} else {
		request, resp = buildStorageCommand(command, key, item.Value, flags, exp, false)
	}
	request.adoptKeyBox(keyBox)
	resp.afterRecv = mapStorageReply
	p.appendOp(pipeOp{req: request, resp: resp, key: key, result: out})
	return p
}

// Delete enqueues a text delete.
func (p *Pipeline) Delete(key []byte, result ...*PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	key, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req, resp := buildDeleteCommand(key, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapDeletedReply
	p.appendOp(pipeOp{req: req, resp: resp, key: key, result: out})
	return p
}

// Touch enqueues a text touch.
func (p *Pipeline) Touch(key []byte, expiry Exptime, result ...*PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	key, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req, resp := buildTouchCommand(key, expiry, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapTouchedReply
	p.appendOp(pipeOp{req: req, resp: resp, key: key, result: out})
	return p
}

// Incr enqueues a text increment and writes the updated counter into dst.
// Each operation in a run requires a distinct dst.
func (p *Pipeline) Incr(key []byte, delta uint64, dst *uint64, result ...*PipelineResult) *Pipeline {
	return p.arithmetic("incr", key, delta, dst, result...)
}

// Decr enqueues a text decrement and writes the updated counter into dst.
// Each operation in a run requires a distinct dst.
func (p *Pipeline) Decr(key []byte, delta uint64, dst *uint64, result ...*PipelineResult) *Pipeline {
	return p.arithmetic("decr", key, delta, dst, result...)
}

func (p *Pipeline) arithmetic(
	command string, key []byte, delta uint64, dst *uint64, result ...*PipelineResult,
) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	if dst == nil {
		err := fmt.Errorf("pipeline arithmetic dst: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	if p.hasArithmeticDst(dst) {
		err := fmt.Errorf("duplicate pipeline arithmetic destination: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	*dst = 0
	wired, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req, resp := buildArithmeticCommand(command, wired, delta, false)
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapArithmeticReply
	p.appendOp(pipeOp{
		req: req, resp: resp, key: wired, arithDst: dst, result: out,
	})
	return p
}

// GetOne enqueues one text get, gets, gat, or gats command.
// dst must remain valid and unique within the pipeline until Run returns.
// A miss leaves dst.Found false.
func (p *Pipeline) GetOne(key []byte, dst *GetResult, opts GetOpts, result ...*PipelineResult) *Pipeline {
	return p.getOne(key, dst, opts, 0, result...)
}

func (p *Pipeline) getOne(
	key []byte, dst *GetResult, opts GetOpts, group uint64, result ...*PipelineResult,
) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	if dst == nil {
		err := fmt.Errorf("pipeline get dst: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	if p.hasGetDst(dst) {
		err := fmt.Errorf("duplicate pipeline get destination: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	resetGetResult(dst)
	command, withCAS := getCommand(opts)
	var exp Exptime
	if opts.Touch != nil {
		exp = *opts.Touch
	}
	wired, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(wired, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req := encodeGetOneCommand(command, exp, wired)
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	p.appendOp(pipeOp{
		req: req, resp: resp, key: wired, getDst: dst, result: out,
		getGroup: group, getCommand: command, getExpiry: exp, getWithCAS: withCAS,
	})
	return p
}

// Get enqueues retrievals that are coalesced into one multi-get per node when
// ordering permits. All nodes share one write/read wave. len(dst) must equal len(keys).
func (p *Pipeline) Get(keys [][]byte, dst []GetResult, opts GetOpts, results ...[]PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	if len(keys) != len(dst) {
		err := fmt.Errorf(
			"pipeline get dst length %d != keys %d: %w",
			len(dst), len(keys), ErrInvalidArgument,
		)
		if len(results) > 0 {
			invalidatePipelineResults(results[0], err)
		}
		p.errs = append(p.errs, err)
		return p
	}
	var out []PipelineResult
	if len(results) > 0 {
		out = results[0]
		if len(out) != len(keys) {
			err := fmt.Errorf(
				"pipeline result length %d != keys %d: %w",
				len(out), len(keys), ErrInvalidArgument,
			)
			invalidatePipelineResults(out, err)
			p.errs = append(p.errs, err)
			return p
		}
	}
	p.nextGetGroup++
	if p.nextGetGroup == 0 {
		p.nextGetGroup++
	}
	group := p.nextGetGroup
	for i := range keys {
		if out == nil {
			p.getOne(keys[i], &dst[i], opts, group)
		} else {
			p.getOne(keys[i], &dst[i], opts, group, &out[i])
		}
	}
	return p
}

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
	if cap(p.keyBuf) > maxPooledBufferCap {
		p.keyBuf = make([]byte, 0, defaultBufferSize)
	} else {
		p.keyBuf = p.keyBuf[:0]
	}
}

// shouldDiscardPipelineConn reports whether a pipeline error must drop the socket.
func shouldDiscardPipelineConn(err error) bool {
	return shouldDiscardConn(err)
}

func finishPipelineConn(cn wireConn, errp *error) {
	var err error
	if errp != nil {
		err = *errp
	}
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

// Run writes every queued frame, then reads every reply. Distinct nodes run concurrently.
// After Run the queue is empty and p may be reused.
func (p *Pipeline) Run(ctx context.Context) error {
	if p == nil || p.c == nil {
		return fmt.Errorf("nil pipeline: %w", ErrInvalidArgument)
	}
	if p.prepared {
		return fmt.Errorf("prepared pipeline: %w", ErrInvalidArgument)
	}
	defer p.release()
	if err := ctx.Err(); err != nil {
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

	var routedStack [32]int
	var routed []int
	if len(p.ops) <= len(routedStack) {
		routed = routedStack[:len(p.ops)]
	} else {
		if cap(p.routed) < len(p.ops) {
			p.routed = make([]int, len(p.ops))
		} else {
			p.routed = p.routed[:len(p.ops)]
		}
		routed = p.routed
	}
	for i := range routed {
		routed[i] = -1
	}
	first := -1
	sameNode := true
	valid := 0
	for i := range p.ops {
		nodeIndex, _, err := pickAddrIndex(picker, addrs, p.ops[i].key)
		if err != nil {
			err = fmt.Errorf("pick node failed: %w", err)
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
	if valid == len(p.ops) && sameNode {
		if err := p.runAddr(ctx, addrs[first], p.ops); err != nil {
			return errors.Join(append(p.errs, err)...)
		}
		return errors.Join(p.errs...)
	}

	if cap(p.groupCounts) < len(addrs) {
		p.groupCounts = make([]int, len(addrs))
	} else {
		p.groupCounts = p.groupCounts[:len(addrs)]
		clear(p.groupCounts)
	}
	if cap(p.groupOffsets) < len(addrs)+1 {
		p.groupOffsets = make([]int, len(addrs)+1)
	} else {
		p.groupOffsets = p.groupOffsets[:len(addrs)+1]
		clear(p.groupOffsets)
	}
	if cap(p.groupedOps) < valid {
		p.groupedOps = make([]pipeOp, valid)
	} else {
		p.groupedOps = p.groupedOps[:valid]
	}
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

	var (
		mu   sync.Mutex
		errs []error
	)
	runNode := func(node *addr, ops []pipeOp) {
		if err := p.runAddr(ctx, node, ops); err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}
	if activeGroups == 1 {
		for i, node := range addrs {
			if p.groupOffsets[i] != p.groupOffsets[i+1] {
				runNode(node, p.groupedOps[p.groupOffsets[i]:p.groupOffsets[i+1]])
				break
			}
		}
	} else {
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
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	return errors.Join(append(p.errs, errs...)...)
}

func pipelineAddrIndex(addrs []*addr, want *addr) int {
	if want != nil && want.slot >= 0 && want.slot < len(addrs) && addrs[want.slot] == want {
		return want.slot
	}
	for i, node := range addrs {
		if node == want {
			return i
		}
	}
	panic("memcached: picker returned an address outside its snapshot")
}

type pipelineFrame struct {
	req       *request
	resp      *response
	start     int
	end       int
	tempDst   []GetResult
	temporary bool
}

type pipelineFrameScratch struct {
	frames []pipelineFrame
	keys   [][]byte
	dst    []GetResult
}

const maxPooledPipelineFrames = 256

var pipelineFramePool = sync.Pool{
	New: func() any {
		return &pipelineFrameScratch{
			frames: make([]pipelineFrame, 0, 16),
			keys:   make([][]byte, 0, 16),
			dst:    make([]GetResult, 0, 16),
		}
	},
}

// pipelineFrames combines keys from one Pipeline.Get call when they route to
// the same node and no intervening operation on that node separates them.
func pipelineFrames(ops []pipeOp) ([]pipelineFrame, *pipelineFrameScratch) {
	coalescedKeys := 0
	for i := 0; i < len(ops); {
		j := i + 1
		if ops[i].getGroup != 0 {
			for j < len(ops) && ops[j].getGroup == ops[i].getGroup {
				j++
			}
			if j-i > 1 {
				coalescedKeys += j - i
			}
		}
		i = j
	}
	if coalescedKeys == 0 {
		return nil, nil
	}

	scratch := pipelineFramePool.Get().(*pipelineFrameScratch)
	if cap(scratch.keys) < coalescedKeys {
		scratch.keys = make([][]byte, coalescedKeys)
	} else {
		scratch.keys = scratch.keys[:coalescedKeys]
	}
	if cap(scratch.dst) < coalescedKeys {
		scratch.dst = make([]GetResult, coalescedKeys)
	} else {
		scratch.dst = scratch.dst[:coalescedKeys]
	}
	if cap(scratch.frames) < len(ops) {
		scratch.frames = make([]pipelineFrame, 0, len(ops))
	} else {
		scratch.frames = scratch.frames[:0]
	}
	keys := scratch.keys
	dst := scratch.dst
	frames := scratch.frames
	tempAt := 0
	for i := 0; i < len(ops); {
		j := i + 1
		if ops[i].getGroup != 0 {
			for j < len(ops) && ops[j].getGroup == ops[i].getGroup {
				j++
			}
		}
		if j-i == 1 {
			frames = append(frames, pipelineFrame{
				req: ops[i].req, resp: ops[i].resp, start: i, end: j,
			})
			i = j
			continue
		}

		groupKeys := keys[tempAt : tempAt+j-i]
		groupDst := dst[tempAt : tempAt+j-i]
		for k := i; k < j; k++ {
			groupKeys[k-i] = ops[k].key
			groupDst[k-i].Value = ops[k].getDst.Value
		}
		var req *request
		if ops[i].getCommand == "gat" || ops[i].getCommand == "gats" {
			req = encodeGetAndTouchesCommand(ops[i].getCommand, ops[i].getExpiry, groupKeys...)
		} else {
			req = encodeGetsCommand(ops[i].getCommand, groupKeys...)
		}
		frames = append(frames, pipelineFrame{
			req:       req,
			resp:      buildRetrievalResponse(groupKeys, nil, groupDst, ops[i].getWithCAS),
			start:     i,
			end:       j,
			tempDst:   groupDst,
			temporary: true,
		})
		tempAt += j - i
		i = j
	}
	scratch.frames = frames
	return frames, scratch
}

func releasePipelineFrames(scratch *pipelineFrameScratch) {
	if scratch == nil {
		return
	}
	for i := range scratch.frames {
		if scratch.frames[i].temporary {
			releaseReqAndResp(scratch.frames[i].req, scratch.frames[i].resp)
		}
		scratch.frames[i] = pipelineFrame{}
	}
	for i := range scratch.keys {
		scratch.keys[i] = nil
		scratch.dst[i] = GetResult{}
	}
	if cap(scratch.frames) > maxPooledPipelineFrames ||
		cap(scratch.keys) > maxPooledPipelineFrames ||
		cap(scratch.dst) > maxPooledPipelineFrames {
		return
	}
	scratch.frames = scratch.frames[:0]
	scratch.keys = scratch.keys[:0]
	scratch.dst = scratch.dst[:0]
	pipelineFramePool.Put(scratch)
}

func setPipelineResultError(ops []pipeOp, err error) {
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.Err = err
		}
	}
}

// runAddr writes then reads one node's queued frames on a single connection.
func (p *Pipeline) runAddr(ctx context.Context, node *addr, ops []pipeOp) error {
	if isUDPNetwork(node.Network) {
		err := fmt.Errorf("udp pipeline: %w", ErrNotSupported)
		failNotExecuted(ops, err)
		return err
	}
	cn, err := p.c.getConn(ctx, node)
	if err != nil {
		err = wrapTransport(fmt.Errorf("alloc connection failed: %w", err))
		failNotExecuted(ops, err)
		return err
	}
	var opErr error
	defer finishPipelineConn(cn, &opErr)
	opErr = p.runConn(ctx, node, cn, ops)
	return opErr
}

// runConn executes ops on an already-exclusive connection without releasing it.
func (p *Pipeline) runConn(ctx context.Context, node *addr, cn wireConn, ops []pipeOp) error {
	frames, frameScratch := pipelineFrames(ops)
	defer releasePipelineFrames(frameScratch)
	var frameStack [32]pipelineFrame
	if frames == nil {
		if len(ops) <= len(frameStack) {
			frames = frameStack[:len(ops)]
		} else {
			frames = make([]pipelineFrame, len(ops))
		}
		for i := range ops {
			frames[i] = pipelineFrame{
				req: ops[i].req, resp: ops[i].resp, start: i, end: i + 1,
			}
		}
	}
	return p.runFrames(ctx, node, cn, ops, frames)
}

func (p *Pipeline) runFrames(
	ctx context.Context, node *addr, cn wireConn, ops []pipeOp, frames []pipelineFrame,
) error {
	var (
		err   error
		opErr error
	)
	for i := range frames {
		if !frames[i].temporary {
			continue
		}
		logical := ops[frames[i].start:frames[i].end]
		for j := range frames[i].tempDst {
			frames[i].tempDst[j] = GetResult{Value: logical[j].getDst.Value}
		}
	}
	stop := prepareOpIO(ctx, cn, p.c.rt.readTimeout, p.c.rt.writeTimeout)
	defer stop()

	failBatchWrite := func(n int64, writeErr error) error {
		opErr := wrapTransport(fmt.Errorf("send failed: %w", writeErr))
		unknownFrames := 0
		left := n
		for i := range frames {
			frameBytes := int64(requestWireSize(frames[i].req))
			if left < frameBytes {
				unknownFrames = i + 1
				break
			}
			left -= frameBytes
			unknownFrames = i + 1
		}
		if unknownFrames == 0 {
			unknownFrames = 1
		}
		logicalEnd := frames[unknownFrames-1].end
		failUnknown(ops[:logicalEnd], opErr)
		failNotExecuted(ops[logicalEnd:], opErr)
		p.c.noteFailure(node)
		return opErr
	}
	batchedWrite := false
	if wire, ok := cn.(*conn); ok && len(frames) > 1 &&
		wire.wr != nil && wire.wr.Buffered() == 0 && wire.isTLS() {
		var reqStack [32]*request
		var reqs []*request
		if len(frames) <= len(reqStack) {
			reqs = reqStack[:len(frames)]
		} else {
			reqs = make([]*request, len(frames))
		}
		total := 0
		for i := range frames {
			reqs[i] = frames[i].req
			total += requestWireSize(frames[i].req)
		}
		if total >= minTLSCoalesceBytes && total <= maxTLSCoalesceBytes {
			n, writeErr := wire.writeTLSBatch(reqs, total)
			if writeErr != nil {
				return failBatchWrite(n, writeErr)
			}
			batchedWrite = true
		}
	}
	if wire, ok := cn.(*conn); ok && len(frames) > 1 &&
		!batchedWrite && wire.wr != nil && wire.wr.Buffered() == 0 && wire.supportsPipelineWritev() {
		hasLargeBody := false
		var reqStack [32]*request
		var reqs []*request
		if len(frames) <= len(reqStack) {
			reqs = reqStack[:len(frames)]
		} else {
			reqs = make([]*request, len(frames))
		}
		for i := range frames {
			reqs[i] = frames[i].req
			hasLargeBody = hasLargeBody || len(frames[i].req.body) >= connWriteBufferSize
		}
		if hasLargeBody {
			n, writeErr := wire.writePipelineBatch(reqs)
			if writeErr != nil {
				return failBatchWrite(n, writeErr)
			}
			batchedWrite = true
		}
	}
	if !batchedWrite {
		for i := range frames {
			if err = frames[i].req.writePipeline(cn); err != nil {
				opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
				failUnknown(ops[:frames[i].end], opErr)
				failNotExecuted(ops[frames[i].end:], opErr)
				p.c.noteFailure(node)
				return opErr
			}
		}
		if err = cn.Flush(); err != nil {
			opErr = wrapTransport(fmt.Errorf("send failed: %w", err))
			failUnknown(ops, opErr)
			p.c.noteFailure(node)
			return opErr
		}
	}
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.State = PipelineUnknown
		}
	}

	var joined []error
	for i := range frames {
		frame := &frames[i]
		logical := ops[frame.start:frame.end]
		if frame.resp != nil {
			frame.resp.maxValueBytes = p.c.valueLimit()
			frame.resp.readTimeout = p.c.rt.readTimeout
			frame.resp.readCtx = ctx
			frame.resp.readConn = cn
		}
		err = frame.resp.recvReady(ctx, cn)
		if frame.temporary {
			for j := range frame.tempDst {
				*logical[j].getDst = frame.tempDst[j]
			}
		}
		if err != nil {
			opErr = wrapRecvError(fmt.Errorf("recv failed: %w", err))
			setPipelineResultError(logical, opErr)
			if shouldDiscardPipelineConn(opErr) {
				failUnknown(ops[frame.end:], opErr)
				p.c.noteFailure(node)
				return errors.Join(append(joined, opErr)...)
			}
			for j := range logical {
				if logical[j].result != nil {
					logical[j].result.State = PipelineDone
				}
			}
			joined = append(joined, opErr)
			continue
		}
		if frame.resp.afterRecv != nil {
			if err = wrapRecvError(frame.resp.afterRecv(frame.resp)); err != nil {
				setPipelineResultError(logical, err)
				for j := range logical {
					if logical[j].result != nil {
						logical[j].result.State = PipelineDone
					}
				}
				if shouldDiscardPipelineConn(err) {
					failUnknown(ops[frame.end:], err)
					p.c.noteFailure(node)
					return errors.Join(append(joined, err)...)
				}
				joined = append(joined, err)
				continue
			}
		}
		if logical[0].arithDst != nil {
			*logical[0].arithDst = frame.resp.arithValue
		}
		for j := range logical {
			if logical[j].result != nil {
				logical[j].result.State = PipelineDone
				logical[j].result.Err = nil
			}
		}
		if i == len(frames)-1 {
			if err = ensureNoTrailingBytes(cn); err != nil {
				opErr = wrapRecvError(err)
				setPipelineResultError(logical, opErr)
				failUnknown(ops[frame.end:], opErr)
				p.c.noteFailure(node)
				return errors.Join(append(joined, opErr)...)
			}
		}
	}
	p.c.noteSuccess(node)
	return errors.Join(joined...)
}
