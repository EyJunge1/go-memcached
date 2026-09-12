package memcached

import (
	"fmt"
)

// Pipeline queues text commands, then writes every frame and reads every reply.
// Quiet mutations belong on Meta().Pipeline.
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
	// Zero PipelineState is unset/invalid (Uber: enums start at one).
	PipelineNotExecuted PipelineState = iota + 1
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
	getExtra   []string
}

// Pipeline begins a write-then-read text batch. Meta commands use Meta().Pipeline.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{c: c, keyBuf: make([]byte, 0, _defaultBufferSize)}
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

const _pipelineAliasIndexThreshold = 16

func (p *Pipeline) ensureAliasIndexes() {
	if p.resultDsts != nil || len(p.ops) < _pipelineAliasIndexThreshold {
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
