package memcached

import (
	"bufio"
	"bytes"
	"context"
	"net"
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
