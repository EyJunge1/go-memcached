package memcached

import (
	"fmt"
	"slices"
)

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
	var req *request
	var resp *response
	var buildErr error
	if item.Mode == StoreCAS {
		req, resp, buildErr = buildCASCommand(key, item.Value, flags, exp, item.CAS, item.Extra, false /* noReply */)
	} else {
		req, resp, buildErr = buildStorageCommand(command, key, item.Value, flags, exp, item.Extra, false /* noReply */)
	}
	if buildErr != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, buildErr)
		p.errs = append(p.errs, buildErr)
		return p
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapStorageReply
	p.appendOp(pipeOp{req: req, resp: resp, key: key, result: out})
	return p
}

// Delete enqueues a text delete. MutateOpts.Quiet is ErrInvalidArgument.
func (p *Pipeline) Delete(key []byte, opts MutateOpts, result ...*PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	if opts.Quiet {
		err := fmt.Errorf("pipeline Quiet: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	key, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req, resp, err := buildDeleteCommand(key, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapDeletedReply
	p.appendOp(pipeOp{req: req, resp: resp, key: key, result: out})
	return p
}

// Touch enqueues a text touch. MutateOpts.Quiet is ErrInvalidArgument.
func (p *Pipeline) Touch(key []byte, expiry Exptime, opts MutateOpts, result ...*PipelineResult) *Pipeline {
	if p == nil || p.c == nil || p.prepared {
		return p
	}
	out, ok := p.pipelineResult(result)
	if !ok {
		return p
	}
	if opts.Quiet {
		err := fmt.Errorf("pipeline Quiet: %w", ErrInvalidArgument)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	key, keyBox := p.wireKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req, resp, err := buildTouchCommand(key, expiry, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req.adoptKeyBox(keyBox)
	resp.afterRecv = mapTouchedReply
	p.appendOp(pipeOp{req: req, resp: resp, key: key, result: out})
	return p
}

// Incr enqueues a text increment and writes the updated counter into dst.
// Each operation in a run requires a distinct dst.
func (p *Pipeline) Incr(key []byte, delta uint64, dst *uint64, opts ArithmeticOpts, result ...*PipelineResult) *Pipeline {
	return p.arithmetic("incr", key, delta, dst, opts, result...)
}

// Decr enqueues a text decrement and writes the updated counter into dst.
// Each operation in a run requires a distinct dst.
func (p *Pipeline) Decr(key []byte, delta uint64, dst *uint64, opts ArithmeticOpts, result ...*PipelineResult) *Pipeline {
	return p.arithmetic("decr", key, delta, dst, opts, result...)
}

func (p *Pipeline) arithmetic(
	command string, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts, result ...*PipelineResult,
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
	req, resp, err := buildArithmeticCommand(command, wired, delta, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
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
	extra := slices.Clone(opts.Extra)
	req, err := encodeGetOneCommand(command, exp, wired, extra)
	if err != nil {
		putBuilderBox(keyBox)
		failPipelineResult(out, err)
		p.errs = append(p.errs, err)
		return p
	}
	req.adoptKeyBox(keyBox)
	resp := buildRetrievalResponseOne(wired, dst, withCAS)
	p.appendOp(pipeOp{
		req: req, resp: resp, key: wired, getDst: dst, result: out,
		getGroup: group, getCommand: command, getExpiry: exp, getWithCAS: withCAS, getExtra: extra,
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
