package memcached

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetaGetWithValueConvenience(t *testing.T) {
	f := MetaGet{}.WithValue()
	w := f.toFlags()
	if !w.v {
		t.Fatal("WithValue did not set v")
	}
}

func TestClientConfigIDWithoutDiscovery(t *testing.T) {
	equal(t, int64(0), (*Client)(nil).ConfigID())
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	equal(t, int64(0), c.ConfigID())
}

func TestOpEventRetries(t *testing.T) {
	equal(t, 0, (OpEvent{}).Retries())
	equal(t, 0, (OpEvent{Attempts: []Attempt{{}}}).Retries())
	equal(t, 2, (OpEvent{Attempts: []Attempt{{}, {}, {}}}).Retries())
}

func TestObserveDispatchEmitsAttempts(t *testing.T) {
	srv := startFakeMemcached(t)
	obs := &recordingObserver{}
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Observer = obs
	})
	req, resp := buildVersionCommand()
	defer releaseReqAndResp(req, resp)
	boom := errors.New("retry")
	c.observeDispatch(context.Background(), time.Now(), c.addrs[0], req, nil, []Attempt{
		{},
		{Err: boom},
	})
	ev := obs.snapshot()
	if len(ev) == 0 {
		t.Fatal("expected observer event")
	}
	greater(t, ev[len(ev)-1].Retries(), 0)
}

func TestParseShutdownReply(t *testing.T) {
	isErrorIs(t, parseShutdownReply(nil), io.EOF)
	isErrorIs(t, parseShutdownReply(&response{}), io.EOF)
	resp := &response{rawLines: [][]byte{[]byte("OK\r\n")}}
	noError(t, parseShutdownReply(resp))
	resp.rawLines = [][]byte{[]byte("SERVER_ERROR no\r\n")}
	isErrorIs(t, parseShutdownReply(resp), ErrServerError)
}

func TestDeferredMetaDelete(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, c.Meta().Set(ctx, []byte("dmd"), []byte("1"), &MetaResult{}, MetaSet{}))
	d := c.Meta().Deferred(ctx)
	wait := d.Delete([]byte("dmd"), MetaDelete{})
	noError(t, d.Flush())
	res, err := wait()
	noError(t, err)
	equal(t, MetaStatusHD, res.Status)
}

func TestPipelineFailBatchWriteDirect(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	var first, second PipelineResult
	p := c.Pipeline().
		Store(Item{Key: []byte("a"), Value: []byte("1")}, &first).
		Store(Item{Key: []byte("b"), Value: []byte("2")}, &second)
	req1 := &request{raw: []byte("set a 0 0 1\r\n"), body: []byte("1"), tail: []byte("\r\n")}
	req2 := &request{raw: []byte("set b 0 0 1\r\n"), body: []byte("2"), tail: []byte("\r\n")}
	frames := []pipelineFrame{
		{req: req1, end: 1},
		{req: req2, end: 2},
	}
	err := p.pipelineFailBatchWrite(c.addrs[0], p.ops, frames, 10, errors.New("boom"))
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	equal(t, PipelineNotExecuted, second.State)
}

func TestSharedStreamHelperCoverage(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	s := &sharedStream{
		c:        c,
		node:     c.addrs[0],
		queue:    make([]*streamJob, 0, 4),
		hasSpace: make(chan struct{}, 1),
		hasWork:  make(chan struct{}, 1),
		readWake: make(chan struct{}, 1),
		metaIdle: make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
	j1 := &streamJob{done: make(chan error, 1)}
	j2 := &streamJob{done: make(chan error, 1)}
	j3 := &streamJob{done: make(chan error, 1)}
	s.queue = []*streamJob{j1, j2}
	isTrue(t, s.tryRemoveJob(j2))
	isFalse(t, s.tryRemoveJob(j2))
	isFalse(t, (*sharedStream)(nil).tryRemoveJob(nil))

	restErr := errors.New("rest")
	s.failRest([]*streamJob{j1, j3}, 1, restErr)
	if got := <-j3.done; !errors.Is(got, restErr) && got != restErr {
		t.Fatalf("failRest err=%v", got)
	}

	item := &MetaItem{}
	resp := &response{metaItem: item}
	jq := &streamJob{done: make(chan error, 1), resp: resp}
	s.completeQuietDefault(jq)
	equal(t, MetaStatusHD, item.status)
	isTrue(t, item.gotHD)
	noError(t, <-jq.done)

	writeErr := errors.New("write")
	failed := &streamJob{done: make(chan error, 1)}
	rest := &streamJob{done: make(chan error, 1)}
	writtenJob := &streamJob{done: make(chan error, 1)}
	s.pendingNum = map[uint64]*streamPending{}
	s.pendingTok = map[string]*streamPending{}
	pend := &streamPending{job: writtenJob}
	s.pendingNum[1] = pend
	s.inFlight = 1
	s.failWriteBatch([]muxWrite{{job: writtenJob, p: pend}}, failed, []*streamJob{writtenJob, failed, rest}, writeErr)
	if got := <-writtenJob.done; got != writeErr {
		t.Fatalf("written=%v", got)
	}
	if got := <-failed.done; got != writeErr {
		t.Fatalf("failed=%v", got)
	}
	if got := <-rest.done; got != writeErr {
		t.Fatalf("rest=%v", got)
	}
}

func TestCountGetOutcomes(t *testing.T) {
	dst := []GetResult{
		{Found: true, Value: []byte("ab")},
		{Found: false},
		{Found: true, Value: []byte("c")},
	}
	hits, misses, respBytes := countGetOutcomes(dst)
	equal(t, 2, hits)
	equal(t, 1, misses)
	equal(t, int64(3), respBytes)
}

func TestWireProtocolReaderAndOpaqueCanonical(t *testing.T) {
	mc := &mockConn{}
	w := wireProtocolReader{c: mc}
	line, err := w.readLine('\n')
	noError(t, err)
	_ = line
	got, err := w.readExact(3)
	noError(t, err)
	hasLen(t, got, 3)
	isTrue(t, opaqueStringIsCanonical("7", 7))
	isFalse(t, opaqueStringIsCanonical("07", 7))
	isFalse(t, opaqueStringIsCanonical("", 0))
	nopStop()
}

func TestSessionEmptyPipelineRun(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	session, err := c.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })
	noError(t, session.Run(context.Background(), c.Pipeline()))
}

func TestPreparedBatchEmptyAndClose(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	batch, err := c.Pipeline().Prepare()
	noError(t, err)
	noError(t, batch.Run(context.Background()))
	noError(t, batch.Close())
	isErrorIs(t, batch.Run(context.Background()), ErrClosed)
}

func TestPipelineOpsCoverage(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()

	// Quiet / validation error paths on the builder.
	p := c.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("v"), Quiet: true}).
		Delete([]byte("k"), MutateOpts{Quiet: true}).
		Touch([]byte("k"), Rel(time.Second), MutateOpts{Quiet: true}).
		Incr([]byte("n"), 1, nil, ArithmeticOpts{}).
		GetOne([]byte("k"), nil, GetOpts{})
	err := p.Run(ctx)
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, c.Store(ctx, Item{Key: []byte("base"), Value: []byte("v")}))
	noError(t, c.Store(ctx, Item{Key: []byte("num"), Value: []byte("10")}))
	var (
		appendRes, prependRes, delRes, touchRes, incrRes, getRes PipelineResult
		got                                                      GetResult
		n                                                        uint64
	)
	noError(t, c.Pipeline().
		Store(Item{Mode: StoreAppend, Key: []byte("base"), Value: []byte("x")}, &appendRes).
		Store(Item{Mode: StorePrepend, Key: []byte("base"), Value: []byte("p")}, &prependRes).
		Incr([]byte("num"), 2, &n, ArithmeticOpts{}, &incrRes).
		Touch([]byte("base"), Rel(time.Minute), MutateOpts{}, &touchRes).
		GetOne([]byte("base"), &got, GetOpts{ReturnCAS: true}, &getRes).
		Delete([]byte("base"), MutateOpts{}, &delRes).
		Run(ctx))
	equal(t, PipelineDone, appendRes.State)
	equal(t, PipelineDone, prependRes.State)
	equal(t, PipelineDone, incrRes.State)
	equal(t, uint64(12), n)
	isTrue(t, got.Found)
}

func TestStoreModeToMetaAndMetaResultFrom(t *testing.T) {
	mode, err := storeModeToMeta(StoreSet)
	noError(t, err)
	equal(t, MetaSetModeSet, mode)
	mode, err = storeModeToMeta(StoreAdd)
	noError(t, err)
	equal(t, MetaSetModeAdd, mode)
	mode, err = storeModeToMeta(StoreReplace)
	noError(t, err)
	equal(t, MetaSetModeReplace, mode)
	mode, err = storeModeToMeta(StoreAppend)
	noError(t, err)
	equal(t, MetaSetModeAppend, mode)
	mode, err = storeModeToMeta(StorePrepend)
	noError(t, err)
	equal(t, MetaSetModePrepend, mode)
	mode, err = storeModeToMeta(StoreCAS)
	noError(t, err)
	equal(t, MetaSetModeSet, mode)
	_, err = storeModeToMeta(StoreMode(99))
	isErrorIs(t, err, ErrInvalidArgument)

	item := &MetaItem{Value: []byte("x")}
	res := metaResultFrom(item, MetaStatusVA)
	equal(t, MetaStatusVA, res.Status)
	equal(t, []byte("x"), res.Item.Value)
}

func TestConnReadHelpers(t *testing.T) {
	payload := []byte("LINE\r\nBODY\r\nhi\r\n")
	cn := &conn{rr: bufio.NewReader(bytes.NewReader(payload))}
	line, err := cn.readLine('\n')
	noError(t, err)
	equal(t, []byte("LINE\r\n"), line)
	slice, err := cn.readSlice('\n')
	noError(t, err)
	equal(t, []byte("BODY\r\n"), slice)
	empty, err := cn.readExact(0)
	noError(t, err)
	if empty != nil {
		t.Fatalf("empty=%q", empty)
	}
	_, err = cn.readExact(-1)
	isErrorIs(t, err, ErrMalformedResponse)
	dst := make([]byte, 2)
	noError(t, cn.readFramedValueInto(dst))
	equal(t, []byte("hi"), dst)

	closed := &conn{}
	closed.closed.Store(true)
	_, err = closed.readLine('\n')
	isTrue(t, err != nil)
	_, err = closed.readExact(1)
	isTrue(t, err != nil)
	isTrue(t, closed.readFramedValueInto(make([]byte, 1)) != nil)

	udp := &conn{}
	_, err = udp.readLine('\n')
	isErrorIs(t, err, ErrNotSupported)
}

func TestPreparedBatchCoalescedGetRunGroup(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, c.Store(ctx, Item{Key: []byte("g1"), Value: []byte("1")}))
	noError(t, c.Store(ctx, Item{Key: []byte("g2"), Value: []byte("2")}))
	dst := make([]GetResult, 2)
	batch, err := c.Pipeline().
		Get([][]byte{[]byte("g1"), []byte("g2")}, dst, GetOpts{}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	noError(t, batch.Run(ctx))
	isTrue(t, dst[0].Found)
	isTrue(t, dst[1].Found)
	equal(t, []byte("1"), dst[0].Value)
	equal(t, []byte("2"), dst[1].Value)

	c2 := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	dst2 := make([]GetResult, 2)
	batch2, err := c2.Pipeline().
		Get([][]byte{[]byte("g1"), []byte("g2")}, dst2, GetOpts{}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch2.Close()) })
	noError(t, batch2.Run(ctx))
	isTrue(t, dst2[0].Found)
	equal(t, []byte("1"), dst2[0].Value)
}

func TestReadLimitedLineContinue(t *testing.T) {
	// Force bufio.ErrBufferFull by using a tiny buffer reader.
	long := append(bytes.Repeat([]byte("a"), 100), '\n')
	r := bufio.NewReaderSize(bytes.NewReader(long), 16)
	line, err := readLimitedLine(r, '\n', 1000)
	noError(t, err)
	equal(t, long, line)

	r2 := bufio.NewReaderSize(bytes.NewReader(bytes.Repeat([]byte("b"), 80)), 8)
	_, err = readLimitedLine(r2, '\n', 32)
	isErrorIs(t, err, ErrMalformedResponse)

	first := []byte("partial")
	r3 := bufio.NewReader(bytes.NewReader([]byte("tail\n")))
	got, err := readLimitedLineContinue(r3, '\n', 100, first)
	noError(t, err)
	equal(t, []byte("partialtail\n"), got)
}

func TestSharedStreamRunBatchWaveAndFailures(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	s := &sharedStream{
		c:        c,
		node:     c.addrs[0],
		hasSpace: make(chan struct{}, 1),
		hasWork:  make(chan struct{}, 1),
		readWake: make(chan struct{}, 1),
		metaIdle: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		queue:    nil,
	}
	wave := &streamJob{
		ctx:  context.Background(),
		node: c.addrs[0],
		done: make(chan error, 1),
		run:  func(context.Context, wireConn) error { return nil },
	}
	s.runBatch([]*streamJob{wave})
	noError(t, <-wave.done)

	mixed := []*streamJob{
		{ctx: context.Background(), node: c.addrs[0], done: make(chan error, 1), req: &request{raw: []byte("version\r\n")}, resp: &response{}},
		{ctx: context.Background(), node: c.addrs[0], done: make(chan error, 1), run: func(context.Context, wireConn) error { return nil }},
	}
	s.runBatch(mixed)
	isTrue(t, <-mixed[0].done != nil)

	udpJob := &streamJob{
		ctx:  context.Background(),
		node: c.addrs[0],
		done: make(chan error, 1),
		req:  &request{raw: []byte("version\r\n"), udpEnabled: true},
		resp: &response{},
	}
	s.runBatch([]*streamJob{udpJob})
	isErrorIs(t, <-udpJob.done, ErrNotSupported)

	j1 := &streamJob{ctx: context.Background(), done: make(chan error, 1), node: c.addrs[0]}
	j2 := &streamJob{ctx: context.Background(), done: make(chan error, 1), node: c.addrs[0]}
	noError(t, c.Close())
	s.runBatch([]*streamJob{j1, j2})
	isTrue(t, <-j1.done != nil)
	isTrue(t, <-j2.done != nil)
}

func TestSessionOpsExtended(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	session, err := c.Session(ctx, []byte("s"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })

	noError(t, session.Store(ctx, Item{Key: []byte("s"), Value: []byte("v")}))
	var got GetResult
	noError(t, session.GetOne(ctx, []byte("s"), &got, GetOpts{Touch: exptimePtr(Rel(time.Minute)), ReturnCAS: true}))
	isTrue(t, got.Found)
	noError(t, session.Store(ctx, Item{Mode: StoreCAS, Key: []byte("s"), Value: []byte("v2"), CAS: got.CAS}))
	noError(t, session.Store(ctx, Item{Mode: StoreAppend, Key: []byte("s"), Value: []byte("x")}))
	noError(t, session.Store(ctx, Item{Mode: StorePrepend, Key: []byte("s"), Value: []byte("p")}))

	batch, err := c.Pipeline().Store(Item{Key: []byte("s"), Value: []byte("z")}).Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	noError(t, session.RunPreparedValues(ctx, batch, [][]byte{[]byte("zz")}))
	equal(t, []byte("zz"), mustGet(ctx, t, c, []byte("s")).Value)
}

func TestAdminDumpCancelMidStream(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpSendOK = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := admin.LRUCrawlerMetaDump(ctx, "all")
	noError(t, err)
	_ = <-stream.Entries
	cancel()
	stream.Close()
}

func TestCopyMetaUserKeyAndFillMetaItem(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.Namespace = "ns:"
	})
	keep := make([]byte, 0, 32)
	item := &MetaItem{WireKey: []byte("ns:hello"), Key: []byte("ignored")}
	copyMetaUserKey(item, keep, c, []byte("fb"), true)
	equal(t, []byte("hello"), item.Key)
	copyMetaUserKey(item, keep, c, []byte("fb"), false)
	if item.Key != nil {
		t.Fatalf("key=%q", item.Key)
	}
	copyMetaUserKey(nil, keep, c, nil, true)
	b64 := []byte("aGVsbG8=") // hello
	item = &MetaItem{WireKey: b64, KeyBase64: true}
	copyMetaUserKey(item, keep, c, []byte("fb"), true)
	equal(t, []byte("hello"), item.Key)
	item = &MetaItem{WireKey: []byte("!!!"), KeyBase64: true}
	copyMetaUserKey(item, keep, c, []byte("fb"), true)
	equal(t, []byte("fb"), item.Key)

	resp := &response{metaItem: &MetaItem{}, maxValueBytes: 64}
	rr := newPayloadReader([]byte("hi\r\n"))
	noError(t, resp.fillMetaItem(rr, []byte("VA 2\r\n")))
	equal(t, []byte("hi"), resp.metaItem.Value)
	isErrorIs(t, (&response{}).fillMetaItem(rr, []byte("VA 2\r\n")), ErrMalformedResponse)
	resp2 := &response{metaItem: &MetaItem{}, maxValueBytes: 64}
	noError(t, resp2.fillMetaItem(newPayloadReader(nil), []byte("HD\r\n"))) // non-VA ok
	resp3 := &response{metaItem: &MetaItem{}, maxValueBytes: 64}
	isErrorIs(t, resp3.fillMetaItem(newPayloadReader([]byte("x")), []byte("VA 2\r\n")), ErrMalformedResponse)
}

func TestDeliverMuxReplyAndReadFramedFallback(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	s := &sharedStream{c: c, node: c.addrs[0]}
	item := &MetaItem{}
	job := &streamJob{resp: &response{metaItem: item, maxValueBytes: 64}, done: make(chan error, 1)}
	p := &streamPending{job: job}
	rr := newPayloadReader([]byte("ab\r\n"))
	jobErr, streamErr := s.deliverMuxReply(p, rr, []byte("VA 2 O1\r\n"))
	noError(t, jobErr)
	noError(t, streamErr)
	equal(t, []byte("ab"), item.Value)

	_, streamErr = s.deliverMuxReply(nil, rr, nil)
	isErrorIs(t, streamErr, ErrMalformedResponse)
	_, streamErr = s.deliverMuxReply(&streamPending{}, rr, nil)
	isErrorIs(t, streamErr, ErrMalformedResponse)

	pr := &protocolReaderAdapter{payload: []byte("xy\r\n")}
	dst := make([]byte, 2)
	noError(t, readFramedValueInto(pr, dst))
	equal(t, []byte("xy"), dst)
}

type protocolReaderAdapter struct {
	payload []byte
	off     int
}

func (p *protocolReaderAdapter) readLine(byte) ([]byte, error) { return nil, io.EOF }

func (p *protocolReaderAdapter) readExact(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrMalformedResponse
	}
	if p.off+n > len(p.payload) {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, p.payload[p.off:p.off+n])
	p.off += n
	return out, nil
}

func TestSessionRunPreparedValuesValidation(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	session, err := c.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })
	batch, err := c.Pipeline().Store(Item{Key: []byte("k"), Value: []byte("v")}).Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	isErrorIs(t, session.RunPreparedValues(context.Background(), batch, nil), ErrInvalidArgument)
	isErrorIs(t, session.RunPreparedValues(context.Background(), nil, [][]byte{[]byte("x")}), ErrInvalidArgument)
}

func TestNilClosedErrorPaths(t *testing.T) {
	ctx := context.Background()
	var session *Session
	isErrorIs(t, session.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}), ErrClosed)
	isErrorIs(t, session.GetOne(ctx, []byte("k"), &GetResult{}, GetOpts{}), ErrClosed)
	isErrorIs(t, session.Delete(ctx, []byte("k"), MutateOpts{}), ErrClosed)
	isErrorIs(t, session.Touch(ctx, []byte("k"), Rel(time.Second), MutateOpts{}), ErrClosed)
	var n uint64
	isErrorIs(t, session.Incr(ctx, []byte("k"), 1, &n, ArithmeticOpts{}), ErrClosed)
	isErrorIs(t, session.Decr(ctx, []byte("k"), 1, &n, ArithmeticOpts{}), ErrClosed)

	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	s, err := c.Session(ctx, []byte("k"))
	noError(t, err)
	noError(t, s.Close())
	isErrorIs(t, s.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}), ErrClosed)
	isErrorIs(t, s.Run(ctx, c.Pipeline()), ErrClosed)

	nm := (*NodeMetaClient)(nil)
	isErrorIs(t, nm.Get(ctx, []byte("k"), &MetaResult{}, MetaGet{}), ErrClosed)
	isErrorIs(t, nm.Noop(ctx, &MetaResult{}, MetaNoop{}), ErrClosed)
	isErrorIs(t, nm.Arithmetic(ctx, []byte("k"), 1, &MetaResult{}, MetaArithmetic{}), ErrClosed)
	_, err = nm.PreparePipeline(nil, nil)
	isErrorIs(t, err, ErrClosed)
	isErrorIs(t, nm.GetMulti(ctx, [][]byte{[]byte("a")}, make([]MetaResult, 1), MetaGet{}), ErrClosed)
	isErrorIs(t, nm.StoreMulti(ctx, []Item{{Key: []byte("a"), Value: []byte("1")}}, make([]MetaResult, 1)), ErrClosed)
	isErrorIs(t, nm.DeleteMulti(ctx, [][]byte{[]byte("a")}, DeleteMissIgnore, make([]MetaResult, 1)), ErrClosed)
	// length mismatch
	meta := c.On(Server{Address: srv.Addr()}).Meta()
	isErrorIs(t, meta.GetMulti(ctx, [][]byte{[]byte("a")}, make([]MetaResult, 2), MetaGet{}), ErrInvalidArgument)
	isErrorIs(t, meta.StoreMulti(ctx, nil, make([]MetaResult, 1)), ErrInvalidArgument)
	isErrorIs(t, meta.DeleteMulti(ctx, [][]byte{[]byte("a")}, DeleteMissIgnore, nil), ErrInvalidArgument)
	noError(t, meta.GetMulti(ctx, nil, nil, MetaGet{}))
	noError(t, meta.StoreMulti(ctx, nil, nil))
	noError(t, meta.DeleteMulti(ctx, nil, DeleteMissIgnore, nil))

	adminClient := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, adminClient)
	_, err = admin.LRUCrawlerMetaDump(ctx, "bad selector!")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = admin.LRUCrawlerMgDump(ctx, "bad selector!")
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestSessionRunWithObserver(t *testing.T) {
	srv := startFakeMemcached(t)
	obs := &recordingObserver{}
	c := testClient(t, srv.Addr(), func(cfg *Config) { cfg.Observer = obs })
	session, err := c.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })
	noError(t, session.Run(context.Background(), c.Pipeline().Store(Item{Key: []byte("k"), Value: []byte("v")})))
	if len(obs.snapshot()) == 0 {
		t.Fatal("expected observer events")
	}
}

func TestSharedStreamTextTraffic(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 32
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte{byte('t'), byte('0' + i%10)}
			noError(t, c.Store(ctx, Item{Key: key, Value: []byte("v")}))
			var got GetResult
			noError(t, c.GetOne(ctx, key, &got, GetOpts{}))
			isTrue(t, got.Found)
		}(i)
	}
	wg.Wait()
}

func TestPipelineAliasIndexesAndFanoutHelpers(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	p := c.Pipeline()
	results := make([]PipelineResult, 20)
	for i := range 20 {
		key := []byte{byte('a' + i%26), byte('0' + i/26)}
		p.Store(Item{Key: key, Value: key}, &results[i])
	}
	// Crossing the alias threshold builds indexes; a duplicate result pointer must fail.
	p.Store(Item{Key: []byte("dup"), Value: []byte("x")}, &results[0])
	err := p.Run(ctx)
	isErrorIs(t, err, ErrInvalidArgument)

	seen := make([]bool, 5)
	runFanout(5, 2, func(i int) { seen[i] = true })
	for i := range seen {
		isTrue(t, seen[i])
	}
	runFanout(0, 2, func(int) { t.Fatal("should not run") })
	runFanout(1, 2, func(i int) { equal(t, 0, i) })
	runFanout(3, 0, func(i int) { seen[i] = true })
}

func TestMetaPipelineChunkedByMaxOps(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Cluster.MaxPipelineOps = 2
	})
	ctx := context.Background()
	ops := make([]MetaOp, 5)
	dst := make([]MetaResult, 5)
	for i := range ops {
		ops[i] = MetaOp{
			Key:  []byte{byte('m'), byte('0' + i)},
			Body: []byte("v"),
			Set:  &MetaSet{Quiet: true},
		}
	}
	noError(t, c.Meta().Pipeline(ctx, ops, dst))
	for i := range dst {
		equal(t, MetaStatusHD, dst[i].Status)
	}
}

func TestAdminEnumWireHelpers(t *testing.T) {
	for _, tc := range []struct {
		m    SlabsAutomoveMode
		want int
		err  bool
	}{
		{0, 0, false},
		{SlabsAutomoveStandby, 0, false},
		{SlabsAutomoveNormal, 1, false},
		{SlabsAutomoveAggressive, 2, false},
		{SlabsAutomoveMode(9), 0, true},
	} {
		got, err := tc.m.wire()
		if tc.err {
			isErrorIs(t, err, ErrInvalidArgument)
			continue
		}
		noError(t, err)
		equal(t, tc.want, got)
	}
	for _, tc := range []struct {
		m    StatsDetailMode
		want string
		err  bool
	}{
		{StatsDetailOn, "on", false},
		{StatsDetailOff, "off", false},
		{StatsDetailDump, "dump", false},
		{StatsDetailMode(9), "", true},
	} {
		got, err := tc.m.wire()
		if tc.err {
			isErrorIs(t, err, ErrInvalidArgument)
			continue
		}
		noError(t, err)
		equal(t, tc.want, got)
	}
}

func TestCompleteMuxFaultPrepAndEnqueueCancel(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
		cfg.Pool.MaxInFlightWrites = 1
	})
	s := &sharedStream{
		c:          c,
		node:       c.addrs[0],
		pendingNum: map[uint64]*streamPending{},
		pendingTok: map[string]*streamPending{},
		hasSpace:   make(chan struct{}, 1),
		hasWork:    make(chan struct{}, 1),
		readWake:   make(chan struct{}, 1),
		metaIdle:   make(chan struct{}, 1),
		stop:       make(chan struct{}),
		maxInFlight: 1,
	}
	j := &streamJob{done: make(chan error, 1)}
	prep := &preparedMetaOp{}
	p := &streamPending{job: j, prep: prep}
	s.pendingNum[3] = p
	s.inFlight = 1
	noError(t, s.completeMuxFault(3, true, nil, ErrClientError))
	equal(t, ErrClientError, j.pipeFault)

	// Cancel while queued: fill stream capacity then cancel waiting op.
	block := make(chan struct{})
	go func() {
		_ = c.Meta().Set(context.Background(), []byte("hold"), []byte("v"), &MetaResult{}, MetaSet{})
		<-block
	}()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Meta().Get(ctx, []byte("k"), &MetaResult{}, MetaGetValue())
	isTrue(t, err != nil)
	close(block)

	equal(t, 256, (&sharedStream{}).maxCredit())
	equal(t, 4, (&sharedStream{maxInFlight: 4}).maxCredit())
	equal(t, 0, metaJobCreditCost(nil))
	equal(t, 1, metaJobCreditCost(&streamJob{}))
}

func TestNodeMetaFinishDestErrors(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(strings.TrimSpace(line))
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "ms":
						nbytes := 0
						if len(fields) >= 3 {
							nbytes, _ = strconv.Atoi(fields[2])
						}
						if nbytes >= 0 {
							_, _ = io.ReadFull(r, make([]byte, nbytes+2))
						}
						_, _ = cn.Write([]byte("CLIENT_ERROR bad\r\n"))
					case "mg", "md", "ma":
						_, _ = cn.Write([]byte("CLIENT_ERROR bad\r\n"))
					default:
						_, _ = cn.Write([]byte("ERROR\r\n"))
					}
				}
			}(cn)
		}
	}()
	c := testClient(t, ln.Addr().String())
	meta := c.On(Server{Address: ln.Addr().String()}).Meta()
	ctx := context.Background()
	isErrorIs(t, meta.Get(ctx, []byte("k"), &MetaResult{}, MetaGetValue()), ErrClientError)
	isErrorIs(t, meta.Set(ctx, []byte("k"), []byte("v"), &MetaResult{}, MetaSet{}), ErrClientError)
	isErrorIs(t, meta.Delete(ctx, []byte("k"), &MetaResult{}, MetaDelete{}), ErrClientError)
}

func TestMultiNodeGetFanout(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	c := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
		cfg.Cluster.FanoutWorkers = 2
	})
	ctx := context.Background()
	noError(t, c.Store(ctx, Item{Key: []byte("a-1"), Value: []byte("1")}))
	noError(t, c.Store(ctx, Item{Key: []byte("b-1"), Value: []byte("2")}))
	dst := make([]GetResult, 2)
	noError(t, c.Get(ctx, [][]byte{[]byte("a-1"), []byte("b-1")}, dst, GetOpts{}))
	isTrue(t, dst[0].Found)
	isTrue(t, dst[1].Found)
}

func TestPipelineAndSessionMoreOps(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, c.Store(ctx, Item{Key: []byte("g"), Value: []byte("1")}))
	noError(t, c.Store(ctx, Item{Key: []byte("n"), Value: []byte("5")}))
	touch := Rel(time.Minute)
	var got GetResult
	var n, m uint64
	var r1, r2, r3 PipelineResult
	noError(t, c.Pipeline().
		GetOne([]byte("g"), &got, GetOpts{Touch: &touch, ReturnCAS: true}, &r1).
		Decr([]byte("n"), 1, &n, ArithmeticOpts{}, &r2).
		Incr([]byte("n"), 2, &m, ArithmeticOpts{}, &r3).
		Run(ctx))
	isTrue(t, got.Found)
	equal(t, uint64(4), n)
	equal(t, uint64(6), m)

	keys := [][]byte{[]byte("g"), []byte("missing")}
	dst := make([]GetResult, 2)
	results := make([]PipelineResult, 2)
	noError(t, c.Pipeline().Get(keys, dst, GetOpts{}, results).Run(ctx))
	isTrue(t, dst[0].Found)
	isFalse(t, dst[1].Found)

	session, err := c.Session(ctx, []byte("g"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })
	isErrorIs(t, session.GetOne(ctx, []byte("g"), nil, GetOpts{}), ErrInvalidArgument)
	isErrorIs(t, session.Incr(ctx, []byte("n"), 1, nil, ArithmeticOpts{}), ErrInvalidArgument)
}

func TestMultiNodePipelineRunGrouped(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	c := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	var ra, rb PipelineResult
	noError(t, c.Pipeline().
		Store(Item{Key: []byte("a-p"), Value: []byte("1")}, &ra).
		Store(Item{Key: []byte("b-p"), Value: []byte("2")}, &rb).
		Run(ctx))
	equal(t, PipelineDone, ra.State)
	equal(t, PipelineDone, rb.State)
	equal(t, []byte("1"), mustGet(ctx, t, c, []byte("a-p")).Value)
	equal(t, []byte("2"), mustGet(ctx, t, c, []byte("b-p")).Value)
}

func TestSharedStreamOpaqueHelpers(t *testing.T) {
	isFalse(t, (*streamJob)(nil).isMetaMux())
	isFalse(t, (*streamJob)(nil).isQuietMeta())
	isTrue(t, (&streamJob{req: &request{cmd: []byte("mg")}}).isMetaMux())
	isTrue(t, (&streamJob{req: &request{cmd: []byte("ms")}}).isMetaMux())
	isTrue(t, (&streamJob{req: &request{cmd: []byte("md")}}).isMetaMux())
	isTrue(t, (&streamJob{req: &request{cmd: []byte("ma")}}).isMetaMux())
	isFalse(t, (&streamJob{req: &request{cmd: []byte("get")}}).isMetaMux())
	isTrue(t, (&streamJob{resp: &response{metaPipeDst: []MetaResult{}}}).isMetaMux())
	isTrue(t, (&streamJob{resp: &response{metaPipeDst: []MetaResult{}}}).isQuietMeta())
	isTrue(t, (&streamJob{resp: &response{endIndicator: _endIndicatorQuietBarrier}}).isQuietMeta())

	stripJobQuietNoop(nil)
	j := &streamJob{
		req:  &request{tail: append([]byte(nil), _CRLFMnCRLFBytes...)},
		resp: &response{endIndicator: _endIndicatorQuietBarrier},
	}
	stripJobQuietNoop(j)
	equal(t, _CRLFBytes, j.req.tail)

	raw := []byte("mg k v\r\n")
	out, err := injectMetaRequestOpaque(raw, "7")
	noError(t, err)
	equal(t, []byte("mg k v O7\r\n"), out)
	_, err = injectMetaRequestOpaque(raw, "")
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = injectMetaRequestOpaque([]byte("mg k v"), "1")
	isErrorIs(t, err, ErrMalformedResponse)

	req := &request{raw: []byte("mg k v\r\n")}
	noError(t, injectMetaRequestOpaqueUint(req, 42))
	equal(t, []byte("mg k v O42\r\n"), req.raw)
	isErrorIs(t, injectMetaRequestOpaqueUint(nil, 1), ErrInvalidArgument)

	num, has, tok, found := scanMetaRequestOpaque([]byte("mg key O9\r\n"))
	isTrue(t, found && has)
	equal(t, uint64(9), num)
	_ = tok
	_, _, _, found = scanMetaRequestOpaque([]byte("mg"))
	isFalse(t, found)
}

func TestSharedStreamMuxBookkeeping(t *testing.T) {
	srv := startFakeMemcached(t)
	c := testClient(t, srv.Addr())
	s := &sharedStream{
		c:           c,
		node:        c.addrs[0],
		maxInFlight: 2,
		stop:        make(chan struct{}),
		hasSpace:    make(chan struct{}, 1),
		hasWork:     make(chan struct{}, 1),
	}
	s.initMux()
	equal(t, 2, s.pendingCap())
	equal(t, 256, (*sharedStream)(nil).pendingCap())
	isFalse(t, s.metaBusy())
	s.signalReadWake()
	s.signalMetaIdle()

	j1 := &streamJob{done: make(chan error, 1)}
	j2 := &streamJob{done: make(chan error, 1)}
	p1 := &streamPending{job: j1}
	p2 := &streamPending{job: j2, quiet: true}
	noError(t, s.registerPending(1, true, "", p1))
	isErrorIs(t, s.registerPending(1, true, "", p1), ErrInvalidArgument)
	noError(t, s.registerPending(0, false, "tok", p2))
	isErrorIs(t, s.registerPending(0, false, "tok", p2), ErrInvalidArgument)
	isTrue(t, s.metaBusy())

	got, ok := s.lookupPending(1, true, nil)
	isTrue(t, ok)
	equal(t, p1, got)
	got, ok = s.lookupPending(0, false, []byte("tok"))
	isTrue(t, ok)
	equal(t, p2, got)
	_, ok = s.lookupPending(0, false, nil)
	isFalse(t, ok)

	s.pushBarrier([]*streamPending{p2})
	s.pushBarrierFull(nil, j1)
	b, ok := s.popBarrier()
	isTrue(t, ok)
	hasLen(t, b.quiet, 1)
	_, ok = s.popBarrier()
	isTrue(t, ok)
	_, ok = s.popBarrier()
	isFalse(t, ok)

	taken, ok := s.takePending(1, true, nil)
	isTrue(t, ok)
	equal(t, p1, taken)
	taken, ok = s.takePendingKey(0, false, "tok", nil)
	isTrue(t, ok)
	equal(t, p2, taken)

	// Re-register and fail everything.
	noError(t, s.registerPending(9, true, "", &streamPending{job: j1}))
	noError(t, s.registerPending(0, false, "z", &streamPending{job: j2}))
	s.pushBarrierFull([]*streamPending{{job: j1}}, j2)
	s.failAllPending(errors.New("boom"))
	isFalse(t, s.metaBusy())

	isTrue(t, s.waitMetaIdle())
	s.closed.Store(true)
	isFalse(t, s.waitMetaIdle())
}
