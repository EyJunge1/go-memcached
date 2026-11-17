package memcached

import (
	"context"
	"errors"
	"net"
	"testing"
)

type failPipelineWriteConn struct{ net.Conn }

func (c *failPipelineWriteConn) Write([]byte) (int, error) {
	return 0, errors.New("forced pipeline write failure")
}

type partialPipelineWriteConn struct {
	net.Conn
	n int
}

func (c *partialPipelineWriteConn) Write(p []byte) (int, error) {
	n := c.n
	if n > len(p) {
		n = len(p)
	}
	if n < 0 {
		n = 0
	}
	return n, errors.New("forced partial pipeline write failure")
}

func TestPipelineRunCancelled(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var result PipelineResult
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}, &result).
		Run(ctx)
	isErrorIs(t, err, context.Canceled)
	equal(t, PipelineNotExecuted, result.State)
	isErrorIs(t, result.Err, context.Canceled)
}

func TestPipelineUDPRejected(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr())
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Run(context.Background())
	isErrorIs(t, err, ErrNotSupported)
}

func TestPipelineRecvErrorDiscardsConn(t *testing.T) {
	addr, dials := startCloseAfterCommand(t, "set")
	client := testClient(t, addr)
	var first, second PipelineResult
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}, &first).
		Store(Item{Mode: StoreSet, Key: []byte("other"), Value: []byte("v")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrTransport)
	equal(t, PipelineUnknown, second.State)
	isErrorIs(t, second.Err, ErrTransport)
	ver, verErr := doVersion(context.Background(), client)
	noError(t, verErr)
	equal(t, []byte("after-close"), ver)
	greater(t, int(dials.Load()), 1)
}

func TestPipelineFatalReplyChargesRemainingResults(t *testing.T) {
	var replies int
	addr, _ := startCmdServer(t, func(fields []string) string {
		if fields[0] != "set" {
			return "ERROR\r\n"
		}
		replies++
		if replies == 1 {
			return "SERVER_ERROR oom\r\n"
		}
		return "STORED\r\n"
	})
	client := testClient(t, addr)
	var first, second PipelineResult

	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: []byte("1")}, &first).
		Store(Item{Key: []byte("b"), Value: []byte("2")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrServerError)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrServerError)
	equal(t, PipelineUnknown, second.State)
	isErrorIs(t, second.Err, ErrServerError)
}

func TestPipelineWriteFailureLeavesLaterOpsNotExecuted(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			cn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &failPipelineWriteConn{Conn: cn}, nil
		}
	})
	var first, second PipelineResult

	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: make([]byte, _connWriteBufferSize)}, &first).
		Store(Item{Key: []byte("b"), Value: []byte("2")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrTransport)
	equal(t, PipelineNotExecuted, second.State)
	isErrorIs(t, second.Err, ErrTransport)
}

func TestPipelinePartialWritevBatchFailure(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			cn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			// Fail after some bytes so pipelineFailBatchWrite marks the first frame Unknown.
			return &partialPipelineWriteConn{Conn: cn, n: 32}, nil
		}
	})
	var first, second PipelineResult
	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: make([]byte, _connWriteBufferSize)}, &first).
		Store(Item{Key: []byte("b"), Value: make([]byte, _connWriteBufferSize)}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	equal(t, PipelineNotExecuted, second.State)
}
