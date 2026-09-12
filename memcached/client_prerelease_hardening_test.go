package memcached

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
)

func TestProtocolLineLimit(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	go func() {
		huge := append(bytes.Repeat([]byte("A"), _protocolLineLimit+1), '\n')
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
	pr := newPayloadReader(bytes.Repeat(line, (_statsResponseBudget/len(line))+2))
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
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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
	err := client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Opaque: []byte("0")}},
		{Key: []byte("b"), Get: &MetaGet{Opaque: []byte("1")}},
	}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
	equal(t, MetaStatusUnknown, dst[1].Status)
}

func TestMetaPipelineRejectsUnknownResponseOpaque(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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
	err := client.Meta().Pipeline(context.Background(), []MetaOp{
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
	equal(t, MetaStatus(0), dst[0].Status)
}

func TestMetaPipelineMalformedClearsSiblingSuccess(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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
	err := client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Delete: &MetaDelete{Opaque: []byte("0")}},
		{Key: []byte("b"), Delete: &MetaDelete{Opaque: []byte("1")}},
	}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
	equal(t, MetaStatusUnknown, dst[0].Status)
	equal(t, MetaStatusUnknown, dst[1].Status)
}

func TestMetaPipelineTransportFailureClearsSiblingSuccess(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
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
	err := client.Meta().Pipeline(context.Background(), []MetaOp{
		{Key: []byte("a"), Delete: &MetaDelete{Opaque: []byte("0")}},
		{Key: []byte("b"), Delete: &MetaDelete{Opaque: []byte("1")}},
	}, dst)
	isError(t, err)
	isErrorIs(t, err, ErrTransport)
	for i := range dst {
		equal(t, MetaStatus(0), dst[i].Status)
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
