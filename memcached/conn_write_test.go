package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
)

func TestRequestSendWrappedConnUsesSingleBufferedWrite(t *testing.T) {
	c1, c2 := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, c2)
	}()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
		<-done
	})
	wc := &writeCountConn{Conn: c1}
	cn := &conn{raw: wc, wr: bufio.NewWriterSize(wc, _connWriteBufferSize)}
	req := &request{
		raw:  []byte("set k 0 0 2048\r\n"),
		body: make([]byte, _writevBodyThreshold),
		tail: []byte("\r\n"),
	}
	noError(t, req.sendReady(context.Background(), cn))
	equal(t, 0, cn.wr.Buffered())
	equal(t, 1, int(wc.n.Load()))
}

func TestRequestSendSmallUsesBufio(t *testing.T) {
	c1, c2 := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, c2)
	}()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
		<-done
	})
	wc := &writeCountConn{Conn: c1}
	cn := &conn{raw: wc, wr: bufio.NewWriterSize(wc, _connWriteBufferSize)}
	req := &request{raw: []byte("get k\r\n")}
	noError(t, req.sendReady(context.Background(), cn))
	equal(t, 1, int(wc.n.Load()))
}

func TestRequestWriteKeepsPipelineBuffered(t *testing.T) {
	c1, c2 := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, c2)
	}()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
		<-done
	})
	wc := &writeCountConn{Conn: c1}
	cn := &conn{raw: wc, wr: bufio.NewWriterSize(wc, _connWriteBufferSize)}
	req := &request{
		raw:  []byte("set k 0 0 2048\r\n"),
		body: make([]byte, _writevBodyThreshold),
		tail: []byte("\r\n"),
	}
	noError(t, req.write(cn))
	equal(t, 0, int(wc.n.Load()))
	greater(t, cn.wr.Buffered(), 0)
	noError(t, cn.Flush())
	equal(t, 1, int(wc.n.Load()))
}

func TestRequestWritePipelineWrappedConnStaysBuffered(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	wc := &writeCountConn{Conn: c1}
	cn := &conn{raw: wc, wr: bufio.NewWriterSize(wc, _connWriteBufferSize)}
	small := &request{raw: []byte("get k\r\n")}
	large := &request{
		raw:  []byte("set k 0 0 16384\r\n"),
		body: make([]byte, _connWriteBufferSize),
		tail: []byte("\r\n"),
	}
	expected := make([]byte, 0, len(small.raw)+len(large.raw)+len(large.body)+len(large.tail))
	expected = append(expected, small.raw...)
	expected = append(expected, large.raw...)
	expected = append(expected, large.body...)
	expected = append(expected, large.tail...)
	received := make(chan []byte, 1)
	go func() {
		got := make([]byte, len(expected))
		_, _ = io.ReadFull(c2, got)
		received <- got
	}()

	noError(t, small.writePipeline(cn))
	greater(t, cn.wr.Buffered(), 0)
	noError(t, large.writePipeline(cn))
	greater(t, cn.wr.Buffered(), 0)
	noError(t, cn.Flush())
	greater(t, int(wc.n.Load()), 1)
	equal(t, expected, <-received)
}

func TestConnWritePipelineBatch(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	raw, err := net.Dial("tcp", ln.Addr().String())
	noError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	server, err := ln.Accept()
	noError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	cn := &conn{raw: raw, wr: bufio.NewWriterSize(raw, _connWriteBufferSize)}
	isTrue(t, cn.supportsPipelineWritev())
	reqs := []*request{
		{raw: []byte("set a 0 0 1\r\n"), body: []byte("1"), tail: []byte("\r\n")},
		{raw: []byte("set b 0 0 1\r\n"), body: []byte("2"), tail: []byte("\r\n")},
	}
	expected := []byte("set a 0 0 1\r\n1\r\nset b 0 0 1\r\n2\r\n")
	received := make(chan []byte, 1)
	go func() {
		got := make([]byte, len(expected))
		_, _ = io.ReadFull(server, got)
		received <- got
	}()

	n, err := cn.writePipelineBatch(reqs)
	noError(t, err)
	equal(t, int64(len(expected)), n)
	equal(t, expected, <-received)
	greater(t, cap(cn.batchIOV), 0)
}
