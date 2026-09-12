package memcached

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// streamJob is one exclusive request/response wave on a shared stream.
// Single-op jobs set req/resp; pipeline waves set run instead.
type streamJob struct {
	ctx  context.Context
	node *addr
	req  *request
	resp *response
	// run executes an exclusive multi-frame wave on the stream connection.
	// When non-nil, req/resp are ignored and the job is not write-coalesced
	// with sibling single-ops (fairness: huge waves do not absorb small jobs).
	run  func(ctx context.Context, cn wireConn) error
	done chan error

	// aborted is set when the caller's context ends before the job finishes.
	// Queued jobs are dropped; in-flight jobs still settle to free credit.
	aborted atomic.Bool

	// Meta().Pipeline mux bookkeeping (SharedStreams opaque demux).
	pipeLeft      atomic.Int32
	pipeNeedMN    bool
	pipeSlotsDone atomic.Int32
	pipeFinished  atomic.Int32
	pipeFault     error
}

// sharedStream is one hot connection. Meta mg/ms/md/ma and Meta().Pipeline
// multiplex by opaque with write-while-read up to maxInFlight. Text single-ops
// write-coalesce with ordered reads once meta in-flight is idle.
type sharedStream struct {
	c           *Client
	node        *addr
	maxInFlight int

	mu     sync.Mutex
	cn     wireConn
	closed atomic.Bool
	stop   chan struct{}
	wg     sync.WaitGroup

	// Mutex queue replaces a buffered channel (Uber: channel size 0 or 1).
	// hasWork/hasSpace are size-1 wake signals for the consumer and producers.
	qMu      sync.Mutex
	queue    []*streamJob
	hasWork  chan struct{}
	hasSpace chan struct{}

	// Meta opaque mux state (writer + reader).
	muxMu      sync.Mutex
	pendingNum map[uint64]*streamPending
	pendingTok map[string]*streamPending
	barriers   []streamBarrier
	inFlight   int
	readWake   chan struct{}
	metaIdle   chan struct{}
}

func (c *Client) streamCount() int {
	if c == nil || c.rt == nil {
		return 0
	}
	return c.rt.sharedStreams
}

func (c *Client) ensureNodeStreams(node *addr) []*sharedStream {
	if c == nil || c.closed.Load() || c.streamCount() <= 0 || node == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-check under the client lock: Close may have won the race.
	if c.closed.Load() {
		return nil
	}
	if c.streams == nil {
		c.streams = make(map[string][]*sharedStream, len(c.addrs))
	}
	key := node.id()
	if existing := c.streams[key]; len(existing) > 0 {
		return existing
	}
	n := c.rt.sharedStreams
	out := make([]*sharedStream, 0, n)
	maxIn := c.rt.maxInFlightWrites
	if maxIn <= 0 {
		maxIn = 256
	}
	for i := 0; i < n; i++ {
		// Queue depth matches maxInFlight so producers block once the
		// stream's in-flight budget is full (intentional backpressure).
		s := &sharedStream{
			c:           c,
			node:        node,
			maxInFlight: maxIn,
			stop:        make(chan struct{}),
			queue:       make([]*streamJob, 0, maxIn),
			hasWork:     make(chan struct{}, 1),
			hasSpace:    make(chan struct{}, 1),
		}
		s.initMux()
		s.wg.Add(2)
		go s.loop()
		go s.readerLoop()
		out = append(out, s)
	}
	c.streams[key] = out
	return out
}

func (c *Client) pickStream(node *addr) *sharedStream {
	streams := c.ensureNodeStreams(node)
	if streams == nil {
		return nil
	}
	n := len(streams)
	if n == 0 {
		return nil
	}
	u := c.streamRR.Add(1) % uint64(n)
	if u > uint64(^uint(0)>>1) {
		return streams[0]
	}
	return streams[int(u)]
}

func (c *Client) closeStreams() {
	c.mu.Lock()
	all := c.streams
	c.streams = nil
	c.mu.Unlock()
	for _, group := range all {
		for _, s := range group {
			s.closeStream()
		}
	}
}

func (c *Client) dialStreamConn(ctx context.Context, node *addr) (wireConn, error) {
	// Dial through the normal pool create path without parking in idle.
	pool, err := c.poolForDial(ctx, node)
	if err != nil {
		return nil, err
	}
	dialCtx, cancel := pool.mergeDialContext(ctx)
	defer cancel()
	cn, err := pool.createConn(dialCtx)
	if err != nil {
		return nil, err
	}
	// Stream-owned: do not attach to pool checkout accounting.
	cn.setConnPool(nil)
	return cn, nil
}

func (c *Client) poolForDial(ctx context.Context, node *addr) (*connPool, error) {
	// Force pool creation via getConn then immediately... too heavy.
	// Duplicate minimal pool ensure from getConn.
	if node == nil {
		return nil, fmt.Errorf("nil node: %w", ErrInvalidAddress)
	}
	if p := node.pool.Load(); p != nil && !p.closed.Load() {
		return p, nil
	}
	// Trigger pool create by checking out and discarding is wasteful; call getConn path's ensure.
	cn, err := c.getConn(ctx, node)
	if err != nil {
		return nil, err
	}
	p := cn.getConnPool()
	_ = p.put(cn)
	return p, nil
}

func (s *sharedStream) closeStream() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}
	close(s.stop)
	// Drop the socket first so in-flight mux reads/writes abort, then fail
	// waiters. Discard before Wait so callers are not stuck on a live read.
	s.discardConn()
	select {
	case s.hasWork <- struct{}{}:
	default:
	}
	select {
	case s.hasSpace <- struct{}{}:
	default:
	}
	s.signalReadWake()
	s.signalMetaIdle()
	s.failAllPending(ErrClosed)
	s.failQueued(ErrClosed)
	s.wg.Wait()
}

func (s *sharedStream) ensureConn(ctx context.Context) (wireConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if s.cn != nil && !s.cn.isClosed() {
		return s.cn, nil
	}
	if s.cn != nil {
		_ = s.cn.Close()
		s.cn = nil
	}
	cn, err := s.c.dialStreamConn(ctx, s.node)
	if err != nil {
		return nil, err
	}
	s.cn = cn
	return cn, nil
}

// tryPush enqueues job when under maxIn. ok means enqueued; closed means shutting down.
func (s *sharedStream) tryPush(job *streamJob, maxIn int) (ok, closed bool) {
	s.qMu.Lock()
	defer s.qMu.Unlock()
	if s.closed.Load() {
		return false, true
	}
	if len(s.queue) >= maxIn {
		return false, false
	}
	s.queue = append(s.queue, job)
	select {
	case s.hasWork <- struct{}{}:
	default:
	}
	return true, false
}

func (s *sharedStream) pushJob(ctx context.Context, job *streamJob) error {
	maxIn := s.maxInFlight
	if maxIn <= 0 {
		maxIn = 256
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return ErrClosed
		default:
		}
		ok, closed := s.tryPush(job, maxIn)
		if closed {
			return ErrClosed
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return ErrClosed
		case <-s.hasSpace:
		}
	}
}

func (s *sharedStream) pushFront(job *streamJob) {
	if job == nil {
		return
	}
	s.qMu.Lock()
	defer s.qMu.Unlock()
	if s.closed.Load() {
		select {
		case job.done <- ErrClosed:
		default:
		}
		return
	}
	// Shift in place so existing queue capacity is reused.
	n := len(s.queue)
	s.queue = append(s.queue, job)
	copy(s.queue[1:], s.queue[:n])
	s.queue[0] = job
	select {
	case s.hasWork <- struct{}{}:
	default:
	}
}

// tryRemoveJob drops a still-queued job (caller cancel before write).
func (s *sharedStream) tryRemoveJob(job *streamJob) bool {
	if s == nil || job == nil {
		return false
	}
	s.qMu.Lock()
	defer s.qMu.Unlock()
	for i, j := range s.queue {
		if j != job {
			continue
		}
		copy(s.queue[i:], s.queue[i+1:])
		s.queue[len(s.queue)-1] = nil
		s.queue = s.queue[:len(s.queue)-1]
		select {
		case s.hasSpace <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

func (s *sharedStream) popJob() (*streamJob, bool) {
	s.qMu.Lock()
	defer s.qMu.Unlock()
	for len(s.queue) > 0 {
		job := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		select {
		case s.hasSpace <- struct{}{}:
		default:
		}
		if job != nil {
			return job, true
		}
	}
	return nil, false
}

func (s *sharedStream) waitJob() (*streamJob, bool) {
	for {
		if job, ok := s.popJob(); ok {
			return job, true
		}
		select {
		case <-s.stop:
			if job, ok := s.popJob(); ok {
				return job, true
			}
			return nil, false
		case <-s.hasWork:
		}
	}
}
