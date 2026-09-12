package memcached

import (
	"bytes"
	"context"
	"fmt"
)

// streamPending is one in-flight meta reply keyed by opaque.
type streamPending struct {
	job   *streamJob
	prep  *preparedMetaOp // non-nil for Meta().Pipeline slots
	quiet bool
}

// streamBarrier is one trailing mn for a write window that contained quiet ops.
type streamBarrier struct {
	quiet   []*streamPending
	pipeJob *streamJob // Meta().Pipeline parent completed on MN (even if quiet empty)
}

type muxWrite struct {
	job *streamJob
	p   *streamPending
}

func (s *sharedStream) pendingCap() int {
	if s != nil && s.maxInFlight > 0 {
		return s.maxInFlight
	}
	return 256
}

func (s *sharedStream) initMux() {
	capHint := s.pendingCap()
	s.pendingNum = make(map[uint64]*streamPending, capHint)
	s.pendingTok = make(map[string]*streamPending, capHint)
	s.readWake = make(chan struct{}, 1)
	s.metaIdle = make(chan struct{}, 1)
	s.signalMetaIdle()
}

func (s *sharedStream) signalReadWake() {
	select {
	case s.readWake <- struct{}{}:
	default:
	}
}

func (s *sharedStream) signalMetaIdle() {
	select {
	case s.metaIdle <- struct{}{}:
	default:
	}
}

func (s *sharedStream) metaBusy() bool {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	return s.inFlight > 0 || len(s.barriers) > 0
}

func (s *sharedStream) waitMetaIdle() bool {
	for {
		if s.closed.Load() {
			return false
		}
		if !s.metaBusy() {
			return true
		}
		select {
		case <-s.stop:
			return false
		case <-s.metaIdle:
		}
	}
}

func (s *sharedStream) failAllPending(err error) {
	s.muxMu.Lock()
	pendNum := s.pendingNum
	pendTok := s.pendingTok
	barriers := s.barriers
	capHint := s.pendingCap()
	s.pendingNum = make(map[uint64]*streamPending, capHint)
	s.pendingTok = make(map[string]*streamPending, capHint)
	s.barriers = nil
	s.inFlight = 0
	s.muxMu.Unlock()

	seen := make(map[*streamJob]struct{}, len(pendNum)+len(pendTok)+len(barriers))
	fail := func(j *streamJob) {
		if j == nil {
			return
		}
		if _, ok := seen[j]; ok {
			return
		}
		seen[j] = struct{}{}
		select {
		case j.done <- err:
		default:
		}
	}
	for _, p := range pendNum {
		if p != nil {
			fail(p.job)
		}
	}
	for _, p := range pendTok {
		if p != nil {
			fail(p.job)
		}
	}
	for _, b := range barriers {
		if b.pipeJob != nil {
			fail(b.pipeJob)
		}
		for _, p := range b.quiet {
			if p != nil {
				fail(p.job)
			}
		}
	}
	s.signalMetaIdle()
}

func (s *sharedStream) registerPending(num uint64, hasNum bool, tok string, p *streamPending) error {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	if hasNum {
		if _, exists := s.pendingNum[num]; exists {
			return fmt.Errorf("duplicate opaque: %w", ErrInvalidArgument)
		}
		s.pendingNum[num] = p
	} else {
		if _, exists := s.pendingTok[tok]; exists {
			return fmt.Errorf("duplicate opaque %q: %w", tok, ErrInvalidArgument)
		}
		s.pendingTok[tok] = p
	}
	s.inFlight++
	return nil
}

func (s *sharedStream) lookupPending(num uint64, hasNum bool, tok []byte) (*streamPending, bool) {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	if hasNum {
		p, ok := s.pendingNum[num]
		return p, ok
	}
	if len(tok) == 0 {
		return nil, false
	}
	p, ok := s.pendingTok[string(tok)]
	return p, ok
}

func (s *sharedStream) takePending(num uint64, hasNum bool, tok []byte) (*streamPending, bool) {
	return s.takePendingKey(num, hasNum, "", tok)
}

// takePendingKey drops a pending entry. Prefer tokStr when already a string to
// avoid a []byte↔string round-trip; string(tok) map ops skip the temp alloc.
func (s *sharedStream) takePendingKey(num uint64, hasNum bool, tokStr string, tok []byte) (*streamPending, bool) {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	var p *streamPending
	var ok bool
	switch {
	case hasNum:
		p, ok = s.pendingNum[num]
		if ok {
			delete(s.pendingNum, num)
		}
	case tokStr != "":
		p, ok = s.pendingTok[tokStr]
		if ok {
			delete(s.pendingTok, tokStr)
		}
	case len(tok) > 0:
		p, ok = s.pendingTok[string(tok)]
		if ok {
			delete(s.pendingTok, string(tok))
		}
	}
	if ok && p != nil && s.inFlight > 0 {
		s.inFlight--
	}
	if s.inFlight == 0 && len(s.barriers) == 0 {
		s.signalMetaIdle()
	}
	return p, ok
}

func (s *sharedStream) pushBarrier(quiet []*streamPending) {
	s.pushBarrierFull(quiet, nil)
}

func (s *sharedStream) pushBarrierFull(quiet []*streamPending, pipeJob *streamJob) {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	s.barriers = append(s.barriers, streamBarrier{quiet: quiet, pipeJob: pipeJob})
}

func (s *sharedStream) popBarrier() (streamBarrier, bool) {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	if len(s.barriers) == 0 {
		return streamBarrier{}, false
	}
	b := s.barriers[0]
	s.barriers[0] = streamBarrier{}
	s.barriers = s.barriers[1:]
	if s.inFlight == 0 && len(s.barriers) == 0 {
		s.signalMetaIdle()
	}
	return b, true
}

func (s *sharedStream) creditLeft() int {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	limit := s.maxInFlight
	if limit <= 0 {
		limit = 256
	}
	left := limit - s.inFlight
	if left < 0 {
		return 0
	}
	return left
}

func (s *sharedStream) readerLoop() {
	defer s.wg.Done()
	for {
		if s.closed.Load() && !s.metaBusy() {
			return
		}
		if !s.metaBusy() {
			select {
			case <-s.stop:
				if !s.metaBusy() {
					return
				}
			case <-s.readWake:
			}
			continue
		}

		s.mu.Lock()
		cn := s.cn
		s.mu.Unlock()
		if cn == nil || cn.isClosed() {
			select {
			case <-s.stop:
				s.failAllPending(ErrClosed)
				return
			case <-s.readWake:
				continue
			}
		}

		err := s.readMuxFrame(cn)
		if err != nil {
			s.failMux(err)
			if s.closed.Load() {
				return
			}
		}
	}
}

func (s *sharedStream) readMuxFrame(cn wireConn) error {
	prepareMuxRead(cn, s.c.rt.readTimeout)

	c, ok := cn.(*conn)
	var rr protocolReader
	if ok {
		rr = c
	} else {
		rr = wireProtocolReader{c: cn}
	}
	sr, sliceOK := rr.(sliceReader)
	line, err := readProtocolLine(rr, sr, sliceOK)
	if err != nil {
		return wrapRecvError(fmt.Errorf("recv: %w", err))
	}
	if isMetaNoopLine(line) {
		return s.handleMuxMN()
	}
	if err = forecastCommonFaultLine(line); err != nil {
		if !isChargeMetaFault(err) {
			return wrapRecvError(err)
		}
		num, hasNum, tok := scanMetaOpaque(line)
		if !hasNum && len(tok) == 0 {
			return wrapRecvError(err)
		}
		return s.completeMuxFault(num, hasNum, tok, err)
	}
	num, hasNum, tok := scanMetaOpaque(line)
	// Look up without freeing credit first: header/body still need exclusive
	// use of the conn bufio. Releasing inFlight early lets the writer start
	// text reads and races the buffer.
	p, found := s.lookupPending(num, hasNum, tok)
	if !found || p == nil {
		if hasNum || len(tok) > 0 {
			return wrapRecvError(fmt.Errorf("unknown meta opaque response: %w", ErrMalformedResponse))
		}
		return wrapRecvError(fmt.Errorf("missing meta opaque response: %w", ErrMalformedResponse))
	}
	jobErr, streamErr := s.deliverMuxReply(p, rr, line)
	_, _ = s.takePending(num, hasNum, tok)
	if p.prep == nil {
		select {
		case p.job.done <- jobErr:
		default:
		}
	}
	return streamErr
}

func (s *sharedStream) completeMuxFault(num uint64, hasNum bool, tok []byte, fault error) error {
	p, ok := s.lookupPending(num, hasNum, tok)
	if !ok || p == nil {
		return wrapRecvError(fault)
	}
	if p.prep != nil {
		p.job.pipeFault = fault
		_, _ = s.takePending(num, hasNum, tok)
		return nil
	}
	_, _ = s.takePending(num, hasNum, tok)
	select {
	case p.job.done <- wrapRecvError(fault):
	default:
	}
	return nil
}

// deliverMuxReply consumes one matched meta reply. For single-ops, jobErr is
// completed by the caller after takePending (credit must drop first). Pipeline
// slots never signal job.done here (barrier / finishPipeJob does).
func (s *sharedStream) deliverMuxReply(p *streamPending, rr protocolReader, header []byte) (jobErr, streamErr error) {
	if p == nil {
		return nil, fmt.Errorf("missing mux pending: %w", ErrMalformedResponse)
	}
	j := p.job
	if j == nil {
		return nil, fmt.Errorf("missing mux job: %w", ErrMalformedResponse)
	}
	if p.prep != nil && j.resp != nil && j.resp.metaPipeDst != nil {
		j.resp.metaSawReply = true
		if err := j.resp.fillMetaPipe(rr, header); err != nil {
			return nil, wrapRecvError(err)
		}
		s.notePipeSlotDone(j)
		return nil, nil
	}
	resp := j.resp
	if resp == nil {
		return nil, nil
	}
	resp.maxValueBytes = s.c.valueLimit()
	if resp.metaItem != nil {
		if err := resp.fillMetaItem(rr, header); err != nil {
			return wrapRecvError(err), nil
		}
		resp.metaFilled = true
	} else {
		resp.rawLines = resp.rawLines[:0]
		if _, err := resp.appendDataBlock(rr, header); err != nil {
			return wrapRecvError(err), nil
		}
	}
	if resp.afterRecv != nil {
		if err := wrapRecvError(resp.afterRecv(resp)); err != nil {
			return err, nil
		}
	}
	return nil, nil
}

func (s *sharedStream) failMux(err error) {
	s.discardConn()
	s.failAllPending(err)
	s.failQueued(err)
}

func (s *sharedStream) failQueued(err error) {
	s.qMu.Lock()
	q := s.queue
	s.queue = nil
	s.qMu.Unlock()
	for _, j := range q {
		if j == nil {
			continue
		}
		select {
		case j.done <- err:
		default:
		}
	}
	select {
	case s.hasSpace <- struct{}{}:
	default:
	}
}

func (s *sharedStream) notePipeSlotDone(j *streamJob) {
	if j == nil {
		return
	}
	if j.pipeLeft.Add(-1) > 0 {
		return
	}
	if j.pipeNeedMN {
		j.pipeSlotsDone.Store(1)
		return
	}
	s.finishPipeJob(j, nil)
}

func (s *sharedStream) finishPipeJob(j *streamJob, err error) {
	if j == nil {
		return
	}
	if !j.pipeFinished.CompareAndSwap(0, 1) {
		return
	}
	s.clearJobPending(j)
	if err == nil && j.resp != nil && j.resp.metaPipeGroup != nil {
		s.applyPipeDefaults(j)
	}
	select {
	case j.done <- err:
	default:
	}
}

func (s *sharedStream) clearJobPending(j *streamJob) {
	if j == nil {
		return
	}
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	for k, p := range s.pendingNum {
		if p != nil && p.job == j {
			delete(s.pendingNum, k)
			if s.inFlight > 0 {
				s.inFlight--
			}
		}
	}
	for k, p := range s.pendingTok {
		if p != nil && p.job == j {
			delete(s.pendingTok, k)
			if s.inFlight > 0 {
				s.inFlight--
			}
		}
	}
	if s.inFlight == 0 && len(s.barriers) == 0 {
		s.signalMetaIdle()
	}
}

func (s *sharedStream) applyPipeDefaults(j *streamJob) {
	resp := j.resp
	if resp == nil {
		return
	}
	group := resp.metaPipeGroup
	out := resp.metaPipeDst
	c := resp.metaPipeC
	fault := j.pipeFault
	for _, p := range group {
		if p.noop {
			storeMetaPipelineResult(c, out, p.idx, &p, MetaResult{Status: MetaStatusMN})
		}
	}
	for i := range group {
		p := &group[i]
		if out[p.idx].Status != 0 {
			continue
		}
		if isChargeMetaFault(fault) {
			storeMetaPipelineResult(c, out, p.idx, p, metaResultWithErr(nil, MetaStatusUnknown, fault))
			continue
		}
		switch {
		case p.noop:
			storeMetaPipelineResult(c, out, p.idx, p, MetaResult{Status: MetaStatusMN})
		case bytes.Equal(p.cmd, _MgBytes):
			storeMetaPipelineResult(c, out, p.idx, p, MetaResult{Status: MetaStatusEN})
		default:
			storeMetaPipelineResult(c, out, p.idx, p, MetaResult{Status: MetaStatusHD})
		}
	}
}

func (s *sharedStream) handleMuxMN() error {
	b, ok := s.popBarrier()
	if !ok {
		return wrapRecvError(fmt.Errorf("unexpected MN: %w", ErrMalformedResponse))
	}
	seenPipe := make(map[*streamJob]struct{}, 1+len(b.quiet))
	if b.pipeJob != nil {
		seenPipe[b.pipeJob] = struct{}{}
		s.finishPipeJob(b.pipeJob, b.pipeJob.pipeFault)
	}
	for _, p := range b.quiet {
		if p == nil || p.job == nil {
			continue
		}
		j := p.job
		if p.prep != nil {
			if _, ok := seenPipe[j]; ok {
				continue
			}
			seenPipe[j] = struct{}{}
			s.finishPipeJob(j, j.pipeFault)
			continue
		}
		if s.removePendingRef(p) {
			s.completeQuietDefault(j)
		}
	}
	return nil
}

func (s *sharedStream) removePendingRef(target *streamPending) bool {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()
	removed := false
	for k, p := range s.pendingNum {
		if p == target {
			delete(s.pendingNum, k)
			removed = true
			if s.inFlight > 0 {
				s.inFlight--
			}
		}
	}
	for k, p := range s.pendingTok {
		if p == target {
			delete(s.pendingTok, k)
			removed = true
			if s.inFlight > 0 {
				s.inFlight--
			}
		}
	}
	if s.inFlight == 0 && len(s.barriers) == 0 {
		s.signalMetaIdle()
	}
	return removed
}

func (s *sharedStream) completeQuietDefault(j *streamJob) {
	resp := j.resp
	if resp != nil && resp.metaItem != nil && !resp.metaFilled {
		item := resp.metaItem
		item.status = MetaStatusHD
		item.gotHD = true
		resp.metaFilled = true
	}
	select {
	case j.done <- nil:
	default:
	}
}

func (s *sharedStream) failWriteBatch(written []muxWrite, failed *streamJob, rest []*streamJob, err error) {
	seen := make(map[*streamJob]struct{})
	fail := func(j *streamJob) {
		if j == nil {
			return
		}
		if _, ok := seen[j]; ok {
			return
		}
		seen[j] = struct{}{}
		select {
		case j.done <- err:
		default:
		}
	}
	for _, w := range written {
		if w.p != nil {
			s.removePendingRef(w.p)
		}
		fail(w.job)
	}
	fail(failed)
	start := false
	for _, j := range rest {
		if j == failed {
			start = true
			continue
		}
		if start {
			fail(j)
		}
	}
	s.failMux(err)
}

func metaJobCreditCost(j *streamJob) int {
	if j == nil {
		return 0
	}
	if j.resp != nil && j.resp.metaPipeDst != nil {
		n := 0
		for i := range j.resp.metaPipeGroup {
			if !j.resp.metaPipeGroup[i].noop {
				n++
			}
		}
		if n == 0 {
			return 1
		}
		return n
	}
	return 1
}

func (s *sharedStream) maxCredit() int {
	limit := s.maxInFlight
	if limit <= 0 {
		return 256
	}
	return limit
}

// writeMetaWindow coalesces meta mux jobs, registers opaques, flushes, and
// returns without waiting for reads (write-while-read).
func (s *sharedStream) writeMetaWindow(first *streamJob) {
	cost := metaJobCreditCost(first)
	if !s.waitMetaWriteCredit(first, cost) {
		return
	}
	batch := s.collectMetaWriteBatch(first, cost)
	ctx := batch[0].ctx
	cn, err := s.ensureConn(ctx)
	if err != nil {
		for _, j := range batch {
			select {
			case j.done <- err:
			default:
			}
		}
		return
	}

	stop := prepareMuxWrite(ctx, cn, s.c.rt.writeTimeout)
	defer stop()

	quietPend, needMNWrite, wroteAny, ok := s.writeMetaBatch(cn, batch)
	if !ok {
		return
	}
	if !wroteAny && !needMNWrite {
		return
	}
	if needMNWrite {
		if _, err = cn.Write(_MnCRLFBytes); err != nil {
			s.failMux(wrapTransport(fmt.Errorf("send: %w", err)))
			return
		}
		s.pushBarrier(quietPend)
	}
	if err = cn.Flush(); err != nil {
		s.failMux(wrapTransport(fmt.Errorf("send: %w", err)))
		return
	}
	s.mu.Lock()
	s.cn = cn
	s.mu.Unlock()
	s.signalReadWake()
}

// waitMetaWriteCredit blocks until first fits in the credit window, or stop.
// Keep first in hand while waiting — do not re-queue (avoids duplicate
// completion if stop fires mid-wait). Oversized pipeline waves run alone once
// the stream is fully idle.
func (s *sharedStream) waitMetaWriteCredit(first *streamJob, cost int) bool {
	creditCap := s.maxCredit()
	for {
		left := s.creditLeft()
		if cost <= left || (cost > creditCap && left >= creditCap) {
			return true
		}
		select {
		case <-s.stop:
			select {
			case first.done <- ErrClosed:
			default:
			}
			return false
		case <-s.readWake:
		case <-s.metaIdle:
		}
	}
}

func (s *sharedStream) collectMetaWriteBatch(first *streamJob, cost int) []*streamJob {
	const maxBatch = 32
	limit := s.creditLeft()
	if limit > maxBatch {
		limit = maxBatch
	}
	if cost > limit {
		limit = cost // Oversized wave: exclusive window.
	}
	batch := make([]*streamJob, 0, limit)
	batch = append(batch, first)
	used := metaJobCreditCost(first)
	for used < limit {
		j, ok := s.popJob()
		if !ok {
			break
		}
		if j == nil {
			continue
		}
		if !j.isMetaMux() || j.run != nil {
			s.pushFront(j)
			break
		}
		next := metaJobCreditCost(j)
		if used+next > limit {
			s.pushFront(j)
			break
		}
		batch = append(batch, j)
		used += next
	}
	return batch
}

func (s *sharedStream) writeMetaBatch(
	cn wireConn, batch []*streamJob,
) (quietPend []*streamPending, needMNWrite, wroteAny, ok bool) {
	written := make([]muxWrite, 0, len(batch))
	for _, j := range batch {
		if j.aborted.Load() {
			select {
			case j.done <- context.Canceled:
			default:
			}
			continue
		}
		if j.resp != nil && j.resp.metaPipeDst != nil {
			if !s.writeMetaPipeJob(cn, j, batch, &written) {
				return nil, false, false, false
			}
			continue
		}
		quiet, wrote, err := s.writeMetaSingleJob(cn, j)
		if err != nil {
			s.failWriteBatch(written, j, batch, err)
			return nil, false, false, false
		}
		written = append(written, wrote)
		if quiet {
			needMNWrite = true
			quietPend = append(quietPend, wrote.p)
		}
	}
	return quietPend, needMNWrite, len(written) > 0, true
}

func (s *sharedStream) writeMetaPipeJob(cn wireConn, j *streamJob, batch []*streamJob, written *[]muxWrite) bool {
	var pipeQuiet []*streamPending
	if err := s.prepareMetaPipeJob(j, &pipeQuiet); err != nil {
		s.failWriteBatch(*written, j, batch, err)
		return false
	}
	if j.resp != nil {
		j.resp.maxValueBytes = s.c.valueLimit()
		j.resp.readTimeout = s.c.rt.readTimeout
		j.resp.readCtx = j.ctx
		j.resp.readConn = cn
	}
	s.c.enableUDPFraming(j.node, j.req, j.resp)
	if j.req.udpEnabled {
		s.failWriteBatch(*written, j, batch, fmt.Errorf("shared stream udp: %w", ErrNotSupported))
		return false
	}
	if err := j.req.writePipeline(cn); err != nil {
		s.failWriteBatch(*written, j, batch, wrapTransport(fmt.Errorf("send: %w", err)))
		return false
	}
	// Pipeline payload already ends with mn; register barrier only.
	s.pushBarrierFull(pipeQuiet, j)
	*written = append(*written, muxWrite{job: j})
	return true
}

func (s *sharedStream) writeMetaSingleJob(cn wireConn, j *streamJob) (quiet bool, wrote muxWrite, err error) {
	stripJobQuietNoop(j)
	num, hasNum, tok, err := s.ensureJobOpaque(j)
	if err != nil {
		return false, muxWrite{}, err
	}
	quiet = j.isQuietMeta()
	p := &streamPending{job: j, quiet: quiet}
	if err = s.registerPending(num, hasNum, tok, p); err != nil {
		return false, muxWrite{}, err
	}
	s.c.enableUDPFraming(j.node, j.req, j.resp)
	if j.req.udpEnabled {
		_, _ = s.takePendingKey(num, hasNum, tok, nil)
		return false, muxWrite{}, fmt.Errorf("shared stream udp: %w", ErrNotSupported)
	}
	if j.resp != nil {
		j.resp.maxValueBytes = s.c.valueLimit()
		j.resp.readTimeout = s.c.rt.readTimeout
		j.resp.readCtx = j.ctx
		j.resp.readConn = cn
	}
	if err = j.req.writePipeline(cn); err != nil {
		_, _ = s.takePendingKey(num, hasNum, tok, nil)
		return false, muxWrite{}, wrapTransport(fmt.Errorf("send: %w", err))
	}
	return quiet, muxWrite{job: j, p: p}, nil
}

func (s *sharedStream) prepareMetaPipeJob(j *streamJob, quietPend *[]*streamPending) error {
	resp := j.resp
	group := resp.metaPipeGroup
	n := 0
	for i := range group {
		if !group[i].noop {
			n++
		}
	}
	j.pipeLeft.Store(int32(n))
	j.pipeNeedMN = true
	j.pipeSlotsDone.Store(0)
	j.pipeFinished.Store(0)

	*quietPend = make([]*streamPending, 0, n)
	for i := range group {
		p := &group[i]
		if p.noop {
			continue
		}
		num, hasNum, tok, ok := preparedMetaOpaque(p)
		if !ok {
			return fmt.Errorf("pipeline op missing opaque: %w", ErrInvalidArgument)
		}
		sp := &streamPending{job: j, prep: p, quiet: true}
		tokStr := ""
		if !hasNum {
			tokStr = tok
		}
		if err := s.registerPending(num, hasNum, tokStr, sp); err != nil {
			return err
		}
		*quietPend = append(*quietPend, sp)
	}
	return nil
}
