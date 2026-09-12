package memcached

import (
	"context"
	"errors"
	"fmt"
)

func (p *Pipeline) runAddr(ctx context.Context, node *addr, ops []pipeOp) error {
	if isUDPNetwork(node.Network) {
		err := fmt.Errorf("udp pipeline: %w", ErrNotSupported)
		failNotExecuted(ops, err)
		return err
	}
	limit := 0
	if p.c != nil && p.c.rt != nil {
		limit = p.c.rt.maxPipelineOps
	}
	if limit > 0 && len(ops) > limit {
		errs := make([]error, 0, (len(ops)+limit-1)/limit)
		for i := 0; i < len(ops); i += limit {
			end := i + limit
			if end > len(ops) {
				end = len(ops)
			}
			if err := ctx.Err(); err != nil {
				failNotExecuted(ops[i:], err)
				errs = append(errs, err)
				break
			}
			if err := p.runAddrWave(ctx, node, ops[i:end]); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	return p.runAddrWave(ctx, node, ops)
}

func (p *Pipeline) runAddrWave(ctx context.Context, node *addr, ops []pipeOp) error {
	if p.c.streamCount() > 0 {
		var started bool
		err := p.c.dispatchWaveViaStream(ctx, node, func(ctx context.Context, cn wireConn) error {
			started = true
			return p.runConn(ctx, node, cn, ops)
		})
		if err == nil || !errors.Is(err, errNoStream) {
			if err != nil && !started {
				failNotExecuted(ops, err)
			}
			return err
		}
	}
	cn, err := p.c.getConn(ctx, node)
	if err != nil {
		err = wrapTransport(fmt.Errorf("alloc connection: %w", err))
		failNotExecuted(ops, err)
		return err
	}
	var opErr error
	defer func() { finishPipelineConn(cn, opErr) }()
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

func (p *Pipeline) pipelineFailBatchWrite(
	node *addr, ops []pipeOp, frames []pipelineFrame, n int64, writeErr error,
) error {
	opErr := wrapTransport(fmt.Errorf("send: %w", writeErr))
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

func (p *Pipeline) pipelineTryTLSBatch(cn wireConn, frames []pipelineFrame) (batched bool, n int64, err error) {
	wire, ok := cn.(*conn)
	if !ok || len(frames) <= 1 || wire.wr == nil || wire.wr.Buffered() != 0 || !wire.isTLS() {
		return false, 0, nil
	}
	reqs := pipelineFrameReqs(frames)
	total := 0
	for i := range frames {
		reqs[i] = frames[i].req
		total += requestWireSize(frames[i].req)
	}
	if total < _minTLSCoalesceBytes || total > _maxTLSCoalesceBytes {
		return false, 0, nil
	}
	n, writeErr := wire.writeTLSBatch(reqs, total)
	if writeErr != nil {
		return false, n, writeErr
	}
	return true, n, nil
}

func (p *Pipeline) pipelineTryWritevBatch(cn wireConn, frames []pipelineFrame) (batched bool, n int64, err error) {
	wire, ok := cn.(*conn)
	if !ok || len(frames) <= 1 || wire.wr == nil || wire.wr.Buffered() != 0 || !wire.supportsPipelineWritev() {
		return false, 0, nil
	}
	reqs := pipelineFrameReqs(frames)
	hasLargeBody := false
	for i := range frames {
		reqs[i] = frames[i].req
		hasLargeBody = hasLargeBody || len(frames[i].req.body) >= _connWriteBufferSize
	}
	if !hasLargeBody {
		return false, 0, nil
	}
	n, writeErr := wire.writePipelineBatch(reqs)
	if writeErr != nil {
		return false, n, writeErr
	}
	return true, n, nil
}

func (p *Pipeline) pipelineWriteFrames(
	node *addr, cn wireConn, ops []pipeOp, frames []pipelineFrame,
) error {
	batched, n, err := p.pipelineTryTLSBatch(cn, frames)
	if err != nil {
		return p.pipelineFailBatchWrite(node, ops, frames, n, err)
	}
	if !batched {
		batched, n, err = p.pipelineTryWritevBatch(cn, frames)
		if err != nil {
			return p.pipelineFailBatchWrite(node, ops, frames, n, err)
		}
	}
	if batched {
		return nil
	}
	for i := range frames {
		if err := frames[i].req.writePipeline(cn); err != nil {
			opErr := wrapTransport(fmt.Errorf("send: %w", err))
			failUnknown(ops[:frames[i].end], opErr)
			failNotExecuted(ops[frames[i].end:], opErr)
			p.c.noteFailure(node)
			return opErr
		}
	}
	if err := cn.Flush(); err != nil {
		opErr := wrapTransport(fmt.Errorf("send: %w", err))
		failUnknown(ops, opErr)
		p.c.noteFailure(node)
		return opErr
	}
	return nil
}

func (p *Pipeline) pipelineReadOneFrame(
	ctx context.Context, node *addr, cn wireConn, ops []pipeOp,
	frame *pipelineFrame, logical []pipeOp, lastFrame bool, joined []error,
) ([]error, bool, error) {
	if frame.resp != nil {
		frame.resp.maxValueBytes = p.c.valueLimit()
		frame.resp.readTimeout = p.c.rt.readTimeout
		frame.resp.readCtx = ctx
		frame.resp.readConn = cn
	}
	err := frame.resp.recvReady(ctx, cn)
	if frame.temporary {
		for j := range frame.tempDst {
			*logical[j].getDst = frame.tempDst[j]
		}
	}
	if err != nil {
		return pipelineFrameRecvError(p, node, ops, frame, logical, err, joined)
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
				return joined, true, errors.Join(append(joined, err)...)
			}
			return append(joined, err), false, nil
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
	if lastFrame {
		if err = ensureNoTrailingBytes(cn); err != nil {
			opErr := wrapRecvError(err)
			setPipelineResultError(logical, opErr)
			failUnknown(ops[frame.end:], opErr)
			p.c.noteFailure(node)
			return joined, true, errors.Join(append(joined, opErr)...)
		}
	}
	return joined, false, nil
}

func (p *Pipeline) pipelineReadFrames(
	ctx context.Context, node *addr, cn wireConn, ops []pipeOp, frames []pipelineFrame,
) error {
	var joined []error
	for i := range frames {
		frame := &frames[i]
		logical := ops[frame.start:frame.end]
		var stop bool
		var retErr error
		joined, stop, retErr = p.pipelineReadOneFrame(
			ctx, node, cn, ops, frame, logical, i == len(frames)-1, joined,
		)
		if stop {
			return retErr
		}
	}
	p.c.noteSuccess(node)
	return errors.Join(joined...)
}

func (p *Pipeline) runFrames(
	ctx context.Context, node *addr, cn wireConn, ops []pipeOp, frames []pipelineFrame,
) error {
	resetCoalescedGetDst(ops, frames)
	stop := prepareOpIO(ctx, cn, p.c.rt.readTimeout, p.c.rt.writeTimeout)
	defer stop()
	if err := p.pipelineWriteFrames(node, cn, ops, frames); err != nil {
		return err
	}
	markPipelineOpsUnknown(ops)
	return p.pipelineReadFrames(ctx, node, cn, ops, frames)
}

func resetCoalescedGetDst(ops []pipeOp, frames []pipelineFrame) {
	for i := range frames {
		if !frames[i].temporary {
			continue
		}
		logical := ops[frames[i].start:frames[i].end]
		for j := range frames[i].tempDst {
			frames[i].tempDst[j] = GetResult{Value: logical[j].getDst.Value}
		}
	}
}

func pipelineFrameReqs(frames []pipelineFrame) []*request {
	var reqStack [32]*request
	if len(frames) <= len(reqStack) {
		return reqStack[:len(frames)]
	}
	return make([]*request, len(frames))
}

func markPipelineOpsUnknown(ops []pipeOp) {
	for i := range ops {
		if ops[i].result != nil {
			ops[i].result.State = PipelineUnknown
		}
	}
}

func pipelineFrameRecvError(
	p *Pipeline, node *addr, ops []pipeOp,
	frame *pipelineFrame, logical []pipeOp, err error, joined []error,
) ([]error, bool, error) {
	opErr := wrapRecvError(fmt.Errorf("recv: %w", err))
	setPipelineResultError(logical, opErr)
	if shouldDiscardPipelineConn(opErr) {
		failUnknown(ops[frame.end:], opErr)
		p.c.noteFailure(node)
		return joined, true, errors.Join(append(joined, opErr)...)
	}
	for j := range logical {
		if logical[j].result != nil {
			logical[j].result.State = PipelineDone
		}
	}
	return append(joined, opErr), false, nil
}
