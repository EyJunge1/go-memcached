package memcached

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *sharedStream) loop() {
	defer s.wg.Done()
	for {
		job, ok := s.waitJob()
		if !ok {
			s.failMux(ErrClosed)
			s.signalReadWake()
			return
		}
		if job == nil {
			continue
		}
		if job.aborted.Load() {
			select {
			case job.done <- context.Canceled:
			default:
			}
			continue
		}

		// Meta opaque mux: write window, return immediately (reader demuxes).
		if job.run == nil && job.isMetaMux() {
			s.writeMetaWindow(job)
			continue
		}

		// Text / exclusive run waves wait until meta mux is idle.
		if !s.waitMetaIdle() {
			select {
			case job.done <- ErrClosed:
			default:
			}
			continue
		}
		if err := s.scrubIdleConn(); err != nil {
			select {
			case job.done <- err:
			default:
			}
			continue
		}
		if job.run != nil {
			s.runBatch([]*streamJob{job})
			continue
		}
		credit := s.maxInFlight
		if credit <= 0 {
			credit = 256
		}
		const maxBatch = 32
		limit := credit
		if limit > maxBatch {
			limit = maxBatch
		}
		batch := make([]*streamJob, 0, limit)
		batch = append(batch, job)
		ran := false
		for len(batch) < limit {
			j, ok := s.popJob()
			if !ok {
				break
			}
			if j == nil {
				continue
			}
			if j.aborted.Load() {
				select {
				case j.done <- context.Canceled:
				default:
				}
				continue
			}
			if j.run != nil {
				s.runBatch(batch)
				if !s.waitMetaIdle() {
					select {
					case j.done <- ErrClosed:
					default:
					}
					ran = true
					break
				}
				s.runBatch([]*streamJob{j})
				ran = true
				break
			}
			if j.isMetaMux() {
				s.pushFront(j)
				break
			}
			batch = append(batch, j)
		}
		if !ran {
			s.runBatch(batch)
		}
	}
}

func (s *sharedStream) discardConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cn != nil {
		_ = s.cn.Close()
		s.cn = nil
	}
}

func (s *sharedStream) failRest(batch []*streamJob, from int, err error) {
	for i := from; i < len(batch); i++ {
		select {
		case batch[i].done <- err:
		default:
		}
	}
}

func (s *sharedStream) runBatch(batch []*streamJob) {
	if len(batch) == 0 {
		return
	}
	ctx := batch[0].ctx
	cn, err := s.ensureConn(ctx)
	if err != nil {
		for _, j := range batch {
			j.done <- err
		}
		return
	}

	// Exclusive wave jobs (pipeline) run one at a time on the stream socket.
	if len(batch) == 1 && batch[0].run != nil {
		j := batch[0]
		stop := prepareOpIO(j.ctx, cn, s.c.rt.readTimeout, s.c.rt.writeTimeout)
		defer stop()
		err := j.run(j.ctx, cn)
		if err != nil {
			s.discardConn()
			j.done <- err
			return
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.cn = cn
		}()
		j.done <- nil
		return
	}

	stop := prepareOpIO(ctx, cn, s.c.rt.readTimeout, s.c.rt.writeTimeout)
	defer stop()

	// Write-coalesce: encode all frames, one Flush, then ordered reads.
	for i, j := range batch {
		if j.run != nil {
			err = fmt.Errorf("shared stream mixed wave job: %w", ErrInvalidArgument)
			s.failRest(batch, i, err)
			s.discardConn()
			return
		}
		s.c.enableUDPFraming(j.node, j.req, j.resp)
		if j.req.udpEnabled {
			err = fmt.Errorf("shared stream udp: %w", ErrNotSupported)
			j.done <- err
			s.failRest(batch, i+1, err)
			s.discardConn()
			return
		}
		if j.resp != nil {
			j.resp.maxValueBytes = s.c.valueLimit()
			j.resp.readTimeout = s.c.rt.readTimeout
			j.resp.readCtx = j.ctx
			j.resp.readConn = cn
		}
		if err = j.req.writePipeline(cn); err != nil {
			err = wrapTransport(fmt.Errorf("send: %w", err))
			j.done <- err
			s.failRest(batch, i+1, err)
			s.discardConn()
			return
		}
	}
	if err = cn.Flush(); err != nil {
		err = wrapTransport(fmt.Errorf("send: %w", err))
		for _, j := range batch {
			j.done <- err
		}
		s.discardConn()
		return
	}

	for i, j := range batch {
		if err = j.resp.recvReady(j.ctx, cn); err != nil {
			err = wrapRecvError(fmt.Errorf("recv: %w", err))
			j.done <- err
			s.failRest(batch, i+1, err)
			s.discardConn()
			return
		}
		if j.resp.afterRecv != nil {
			if err = wrapRecvError(j.resp.afterRecv(j.resp)); err != nil {
				j.done <- err
				s.failRest(batch, i+1, err)
				s.discardConn()
				return
			}
		}
	}
	// Signal completion only after the trailing-bytes check so a desync is
	// reported to callers instead of discarded.
	if err = ensureNoTrailingBytes(cn); err != nil {
		err = wrapRecvError(err)
		for _, j := range batch {
			j.done <- err
		}
		s.discardConn()
		return
	}
	for _, j := range batch {
		j.done <- nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cn = cn
}

// enqueueStreamJob publishes job onto a shared stream and waits for completion.
func (c *Client) enqueueStreamJob(ctx context.Context, node *addr, job *streamJob) error {
	s := c.pickStream(node)
	if s == nil {
		return errNoStream
	}
	job.ctx = ctx
	job.node = node
	if job.done == nil {
		job.done = make(chan error, 1)
	}
	if err := s.pushJob(ctx, job); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		job.aborted.Store(true)
		if s.tryRemoveJob(job) {
			return ctx.Err()
		}
		// Already writing or in-flight: wait for settle, still return cancel.
		select {
		case err := <-job.done:
			return errors.Join(ctx.Err(), err)
		default:
			return ctx.Err()
		}
	case err := <-job.done:
		return err
	}
}

// scrubIdleConn rejects trailing bytes before text/run reuse of the hot socket.
func (s *sharedStream) scrubIdleConn() error {
	s.mu.Lock()
	cn := s.cn
	s.mu.Unlock()
	if cn == nil || cn.isClosed() {
		return nil
	}
	if err := ensureNoTrailingBytes(cn); err != nil {
		s.discardConn()
		return wrapRecvError(err)
	}
	return nil
}

func prepareMuxWrite(ctx context.Context, rr wireConn, writeTimeout time.Duration) func() {
	_ = selectProximateDeadline(ctx, rr, writeTimeout, time.Now, false /* isRead */)
	if ctx != nil && ctx.Done() != nil {
		return watchContextCancel(ctx, rr)
	}
	return nopStop
}

func prepareMuxRead(rr wireConn, readTimeout time.Duration) {
	if readTimeout > 0 {
		_ = rr.setReadDeadline(time.Now().Add(readTimeout))
	}
}

// dispatchViaStream runs one req/resp on a shared stream when enabled.
func (c *Client) dispatchViaStream(ctx context.Context, node *addr, req *request, resp *response) error {
	return c.enqueueStreamJob(ctx, node, &streamJob{
		req:  req,
		resp: resp,
		done: make(chan error, 1),
	})
}

// dispatchWaveViaStream runs an exclusive multi-frame wave on a shared stream.
func (c *Client) dispatchWaveViaStream(
	ctx context.Context, node *addr, run func(context.Context, wireConn) error,
) error {
	return c.enqueueStreamJob(ctx, node, &streamJob{
		run:  run,
		done: make(chan error, 1),
	})
}

var errNoStream = fmt.Errorf("shared stream unavailable: %w", ErrNotSupported)
