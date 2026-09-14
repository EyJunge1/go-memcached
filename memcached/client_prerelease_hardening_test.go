package memcached

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProtocolLineLimit(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	go func() {
		huge := append(bytes.Repeat([]byte("A"), protocolLineLimit+1), '\n')
		_, _ = server.Write(huge)
		_ = server.Close()
	}()
	cn := &conn{raw: client, rr: bufio.NewReaderSize(client, 1024)}
	_, err := cn.readLine('\n')
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestStatsResponseBudget(t *testing.T) {
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 8)
	defer resp.release()
	var stats []Stat
	resp.statsDst = &stats
	line := []byte("STAT k " + strings.Repeat("v", 1024) + "\r\n")
	pr := newPayloadReader(bytes.Repeat(line, (statsResponseBudget/len(line))+2))
	err := resp.read2(pr, false)
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestGetMalformedResponseDestinationContract(t *testing.T) {
	pr := newPayloadReader([]byte("VALUE k 0 3\r\nabc\r\nVALUE\r\nEND\r\n"))
	dst := []GetResult{{}}
	resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
	defer resp.release()
	err := resp.readRetrieval(pr)
	isErrorIs(t, err, ErrMalformedResponse)
	isFalse(t, dst[0].Found)
	equal(t, 0, len(dst[0].Value))
}

func TestMetaPipelineRejectsDuplicateResponseOpaque(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		r := bufio.NewReader(cn)
		w := bufio.NewWriter(cn)
		_, _ = r.ReadBytes('\n') // first op line
		_, _ = r.ReadBytes('\n') // second op line
		_, _ = r.ReadBytes('\n') // mn
		_, _ = w.WriteString("HD O0\r\nHD O0\r\nMN\r\n")
		_ = w.Flush()
	}()

	client := testClient(t, ln.Addr().String())
	dst := make([]MetaResult, 2)
	err = client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Opaque: []byte("0")}},
		{Key: []byte("b"), Get: &MetaGet{Opaque: []byte("1")}},
	}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
	equal(t, MetaStatusUnknown, dst[1].Status)
}

func TestMetaPipelineRejectsUnknownResponseOpaque(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		r := bufio.NewReader(cn)
		w := bufio.NewWriter(cn)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = w.WriteString("HD O999\r\nMN\r\n")
		_ = w.Flush()
	}()

	client := testClient(t, ln.Addr().String())
	dst := make([]MetaResult, 1)
	err = client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Body: []byte("v"), Set: &MetaSet{Opaque: []byte("0")}},
	}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
}

func TestMetaPipelineRejectsMissingResponseOpaque(t *testing.T) {
	ops := []preparedMetaOp{{idx: 0, hasGet: true, tok: "0"}}
	index, err := buildMetaPipelineIndex(ops)
	noError(t, err)
	defer putMetaPipelineIndex(index)
	dst := make([]MetaResult, 1)
	resp := buildQuietBarrierResponse()
	defer resp.release()
	resp.metaPipeDst = dst
	resp.metaPipeGroup = ops
	resp.metaPipeIndex = index
	resetMetaPipeSeen(resp, len(ops))

	err = resp.read2(newPayloadReader([]byte("HD\r\nMN\r\n")), true)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
}

func TestMetaPipelineMalformedClearsSiblingSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		r := bufio.NewReader(cn)
		w := bufio.NewWriter(cn)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = w.WriteString("HD O0\r\nHD O0\r\nMN\r\n")
		_ = w.Flush()
	}()

	client := testClient(t, ln.Addr().String())
	dst := make([]MetaResult, 2)
	err = client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Delete: &MetaDelete{Opaque: []byte("0")}},
		{Key: []byte("b"), Delete: &MetaDelete{Opaque: []byte("1")}},
	}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
	equal(t, MetaStatusUnknown, dst[1].Status)
}

func TestMetaPipelineTransportFailureClearsSiblingSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		r := bufio.NewReader(cn)
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = r.ReadBytes('\n')
		_, _ = cn.Write([]byte("HD O0\r\n"))
		_ = cn.Close()
	}()

	client := testClient(t, ln.Addr().String())
	dst := []MetaResult{
		{Item: MetaItem{Value: make([]byte, 0, 16)}},
		{Item: MetaItem{Value: make([]byte, 0, 16)}},
	}
	err = client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Delete: &MetaDelete{Opaque: []byte("0")}},
		{Key: []byte("b"), Delete: &MetaDelete{Opaque: []byte("1")}},
	}, dst)
	isError(t, err)
	isErrorIs(t, err, ErrTransport)
	for i := range dst {
		equal(t, MetaStatusUnknown, dst[i].Status)
		equal(t, 16, cap(dst[i].Item.Value))
	}
}

func TestGetRejectsDuplicateVALUE(t *testing.T) {
	pr := newPayloadReader([]byte("VALUE k 0 1\r\na\r\nVALUE k 0 1\r\nb\r\nEND\r\n"))
	dst := []GetResult{{}}
	resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
	defer resp.release()
	err := resp.readRetrieval(pr)
	isErrorIs(t, err, ErrMalformedResponse)
	isFalse(t, dst[0].Found)
	equal(t, 0, len(dst[0].Value))
}

func TestRetrievalSupportsUnorderedAndRepeatedRequestedKeys(t *testing.T) {
	dst := make([]GetResult, 2)
	resp := buildRetrievalResponse([][]byte{[]byte("a"), []byte("b")}, nil, dst, false)
	err := resp.readRetrieval(newPayloadReader(
		[]byte("VALUE b 0 1\r\nb\r\nVALUE a 0 1\r\na\r\nEND\r\n"),
	))
	noError(t, err)
	equal(t, []byte("a"), dst[0].Value)
	equal(t, []byte("b"), dst[1].Value)
	resp.release()

	dst = make([]GetResult, 2)
	resp = buildRetrievalResponse([][]byte{[]byte("a"), []byte("a")}, nil, dst, false)
	err = resp.readRetrieval(newPayloadReader(
		[]byte("VALUE a 0 1\r\nx\r\nVALUE a 0 1\r\ny\r\nEND\r\n"),
	))
	noError(t, err)
	equal(t, []byte("x"), dst[0].Value)
	equal(t, []byte("y"), dst[1].Value)
	resp.release()
}

func TestSetServersRemoveBeforeFirstDial(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})

	addrs, _ := client.snapshotCluster()
	old := addrs[0]
	noError(t, client.SetServers([]Server{{Address: b.Addr()}}))

	_, err := client.getConn(context.Background(), old)
	isErrorIs(t, err, ErrInvalidAddress)
	client.mu.RLock()
	_, exists := client.connPools[old.id()]
	client.mu.RUnlock()
	isFalse(t, exists)
}

func TestSetServersIdenticalMembershipKeepsSessionValid(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	version := client.clusterVersion.Load()

	noError(t, client.SetServers(client.Servers()))
	equal(t, version, client.clusterVersion.Load())
	noError(t, session.Store(context.Background(), Item{
		Mode: StoreSet, Key: []byte("k"), Value: []byte("v"),
	}))
}

func TestSessionCloseAfterClientCloseReleasesCheckout(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	pool := session.cn.getConnPool()
	notNil(t, pool)
	equal(t, 1, int(pool.open.Load()))

	noError(t, client.Close())
	err = session.Store(context.Background(), Item{Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrClosed)
	noError(t, session.Close())
	equal(t, 0, int(pool.open.Load()))
	equal(t, 0, len(pool.conns))
}

func TestCheckoutPoolMapsErrPoolClosedToErrClosed(t *testing.T) {
	pool := newConnPool(1, 1, 0, 0, createConn)
	noError(t, pool.close())
	client := &Client{closed: atomic.Bool{}}
	client.closed.Store(true)
	_, err := client.checkoutPool(context.Background(), pool)
	isErrorIs(t, err, ErrClosed)
}

func TestCheckoutPoolKeepsErrPoolClosedWhenClientOpen(t *testing.T) {
	pool := newConnPool(1, 1, 0, 0, createConn)
	noError(t, pool.close())
	client := &Client{}
	_, err := client.checkoutPool(context.Background(), pool)
	isErrorIs(t, err, errPoolClosed)
}

func TestValidAuthSecretRejectsControls(t *testing.T) {
	for _, s := range []string{"a\x07b", "a\x7fb", "a b", "a\nb", "a\x00b"} {
		isErrorIs(t, validAuthSecret(s), ErrInvalidArgument)
	}
	noError(t, validAuthSecret("ok-user_1"))
}

func TestValidNamespaceRejectsControls(t *testing.T) {
	for _, ns := range []string{"a b", "a\nb", "a\x00b", "a\x7fb"} {
		isErrorIs(t, validNamespace(ns), ErrInvalidArgument)
	}
	noError(t, validNamespace("ns:"))
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Cluster.Namespace = "evil\r\n"
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
	noError(t, validNamespace(strings.Repeat("n", maxKeySize-1)))
	isErrorIs(t, validNamespace(strings.Repeat("n", maxKeySize)), ErrInvalidArgument)
}

func TestTextKeyAcceptsOpaqueHighBitBytes(t *testing.T) {
	noError(t, validateKeyAndValue([]byte{'k', 0x80, 0xff}, nil))
}

func TestAdversarialValueBodiesRemainLengthFramed(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	payload := []byte("\r\nquit\r\nEND\r\nVALUE forged 0 1\r\nx\r\nMN\r\n")

	noError(t, client.Store(ctx, Item{Key: []byte("plain"), Value: payload}))
	equal(t, payload, mustGet(t, client, ctx, []byte("plain")).Value)

	batch, err := client.Pipeline().
		Store(Item{Key: []byte("prepared")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { _ = batch.Close() })
	noError(t, batch.RunValues(ctx, [][]byte{payload}))
	equal(t, payload, mustGet(t, client, ctx, []byte("prepared")).Value)

	dst := make([]MetaResult, 1)
	metaBatch, err := client.Meta().PreparePipeline([]MetaOp{{
		Key: []byte("meta"), Set: &MetaSet{},
	}}, dst)
	noError(t, err)
	t.Cleanup(func() { _ = metaBatch.Close() })
	noError(t, metaBatch.RunValues(ctx, [][]byte{payload}))
	equal(t, payload, mustGet(t, client, ctx, []byte("meta")).Value)
}

func TestValidMetaHintRejectsOverlong(t *testing.T) {
	noError(t, validMetaHint(strings.Repeat("A", maxMetaHintBytes)))
	isErrorIs(t, validMetaHint(strings.Repeat("A", maxMetaHintBytes+1)), ErrInvalidArgument)
}

func TestNilMetaClientReturnsErrClosed(t *testing.T) {
	var m *MetaClient
	ctx := context.Background()
	checks := []func() error{
		func() error { return m.Get(ctx, nil, nil, MetaGet{}) },
		func() error { return m.Set(ctx, nil, nil, nil, MetaSet{}) },
		func() error { return m.Delete(ctx, nil, nil, MetaDelete{}) },
		func() error { return m.Arithmetic(ctx, nil, 0, nil, MetaArithmetic{}) },
		func() error { return m.Debug(ctx, nil, nil, MetaDebug{}) },
		func() error { return m.Noop(ctx, nil, MetaNoop{}) },
		func() error { return m.Pipeline(ctx, nil, nil) },
		func() error {
			_, err := m.PreparePipeline(nil, nil)
			return err
		},
	}
	for _, check := range checks {
		isErrorIs(t, check(), ErrClosed)
	}
}

func TestNewRejectsMinIdleAboveMaxOpen(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 5
	cfg.Pool.MaxOpen = 2
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestNewRejectsMinIdleWhenIdlePoolDisabled(t *testing.T) {
	cfg, err := testConfig("127.0.0.1:11211")
	noError(t, err)
	cfg.Pool.MinIdle = 1
	cfg.Pool.MaxIdle = -1
	_, err = New(cfg)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestUnixSocketSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "sock")
	link := filepath.Join(dir, "link")
	ln, err := net.Listen("unix", target)
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	noError(t, os.Chmod(target, 0o600))
	noError(t, os.Symlink(target, link))
	isErrorIs(t, checkUnixSocketMode(link, false, 0), ErrInvalidArgument)
	isErrorIs(t, checkUnixSocketMode(link, true, 0), ErrInvalidArgument)
}

func TestUDPConnReleaseRotatesOnEpoch(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = serverSide.Close() })
	pool := newConnPool(1, 1, 0, 0, nil)
	oldEpoch := udpRequestEpoch.Load()
	cn := &conn{
		raw:      clientSide,
		network:  "udp",
		udpEpoch: oldEpoch,
		pool:     pool,
	}
	pool.open.Store(1)
	udpRequestEpoch.Store(oldEpoch + 1)
	t.Cleanup(func() { udpRequestEpoch.Store(oldEpoch) })

	noError(t, cn.release())
	isTrue(t, cn.closed.Load())
	equal(t, 0, int(pool.open.Load()))
	equal(t, 0, pool.stats().IdleConns)
	noError(t, pool.close())
}

func TestUDPWrapRequestRotatesBeforeRecv(t *testing.T) {
	oldEpoch := udpRequestEpoch.Load()
	t.Cleanup(func() {
		udpRequestEpoch.Store(oldEpoch)
		udpRequestID.Store(1)
	})

	var dials atomic.Int32
	freshSide, _ := net.Pipe()
	t.Cleanup(func() { _ = freshSide.Close() })

	pool := newConnPool(1, 1, 0, 0, func(context.Context) (wireConn, error) {
		dials.Add(1)
		return &conn{
			raw:      freshSide,
			network:  "udp",
			udpEpoch: udpRequestEpoch.Load(),
			udpIn:    make([]byte, 64),
			udpOut:   make([]byte, 0, 64),
		}, nil
	})

	staleSide, _ := net.Pipe()
	t.Cleanup(func() { _ = staleSide.Close() })
	stale := &conn{
		raw:      staleSide,
		network:  "udp",
		udpEpoch: oldEpoch,
		pool:     pool,
		udpIn:    make([]byte, 64),
		udpOut:   make([]byte, 0, 64),
	}
	pool.open.Store(1)

	udpRequestEpoch.Store(oldEpoch + 1)
	client := &Client{}
	node := newAddrWeighted("udp", "127.0.0.1:11211", 0)
	node.pool.Store(pool)

	got, err := client.rotateUDPConnIfStale(context.Background(), node, stale)
	noError(t, err)
	isTrue(t, stale.closed.Load())
	equal(t, int32(1), dials.Load())
	fresh, ok := got.(*conn)
	isTrue(t, ok)
	equal(t, oldEpoch+1, fresh.udpEpoch)
	isFalse(t, udpConnEpochStale(fresh))
	noError(t, pool.discard(got))
	noError(t, pool.close())
}
