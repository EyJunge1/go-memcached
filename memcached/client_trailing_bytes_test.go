package memcached

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestSingleResponseTrailingBytesDiscardConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	var poisoned atomic.Bool
	go func() {
		for {
			cn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				w := bufio.NewWriter(cn)
				for {
					line, readErr := r.ReadBytes('\n')
					if readErr != nil {
						return
					}
					if !bytes.HasPrefix(line, []byte("set ")) {
						continue
					}
					_, _ = r.ReadBytes('\n')
					if poisoned.CompareAndSwap(false, true) {
						_, _ = w.WriteString("STORED\r\nVALUE leak 0 4\r\nleak\r\nEND\r\n")
					} else {
						_, _ = w.WriteString("STORED\r\n")
					}
					_ = w.Flush()
				}
			}(cn)
		}
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	err = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrMalformedResponse)
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("v2")}))
	equal(t, int32(2), dials.Load())
}

func TestRetrievalExtraEndDoesNotPoisonNextCall(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	var poisoned atomic.Bool
	go func() {
		for {
			cn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				w := bufio.NewWriter(cn)
				for {
					_, readErr := r.ReadBytes('\n')
					if readErr != nil {
						return
					}
					if poisoned.CompareAndSwap(false, true) {
						_, _ = w.WriteString("VALUE k 0 1\r\nx\r\nEND\r\nEND\r\n")
					} else {
						_, _ = w.WriteString("END\r\n")
					}
					_ = w.Flush()
				}
			}(cn)
		}
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})

	_, err = doGetOne(client, context.Background(), []byte("k"), GetOpts{})
	isErrorIs(t, err, ErrMalformedResponse)
	_, err = doGetOne(client, context.Background(), []byte("k"), GetOpts{})
	noError(t, err)
	equal(t, int32(2), dials.Load())
}

func TestEnsureNoTrailingBytes(t *testing.T) {
	pr := newPayloadReader([]byte("STORED\r\nGARBAGE"))
	resp := buildLimitedLineResponse(1)
	defer resp.release()
	noError(t, resp.recvFrom(pr))
	isErrorIs(t, ensureNoTrailingBytes(pr), ErrMalformedResponse)
}

func trailingReplyClient(t *testing.T, reply string, tweak ...func(*Config)) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		_, _ = bufio.NewReader(cn).ReadBytes('\n')
		_, _ = cn.Write([]byte(reply))
	}()
	return testClient(t, ln.Addr().String(), tweak...)
}

func TestPipelineTrailingBytesAtWaveEnd(t *testing.T) {
	client := trailingReplyClient(t, "STORED\r\nEXTRA\r\n")
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Run(context.Background())
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestSessionTrailingBytesAfterMappedResponse(t *testing.T) {
	client := trailingReplyClient(t, "STORED\r\nEXTRA\r\n")
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	err = session.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, session.closed.Load())
}

func TestMetaPipelineTrailingBytesAtBarrier(t *testing.T) {
	client := trailingReplyClient(t, "EN O0\r\nMN\r\nEXTRA\r\n")
	dst := make([]MetaResult, 1)
	err := client.Meta().Pipeline(context.Background(), []MetaOp{{
		Key: []byte("k"),
		Get: &MetaGet{},
	}}, dst)
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestFlushTrailingBytesDiscardConnection(t *testing.T) {
	client := trailingReplyClient(t, "OK\r\nEXTRA\r\n", func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	server := client.Servers()[0]
	admin, err := client.Admin()
	noError(t, err)
	err = admin.On(server).Flush(context.Background(), FlushOpts{})
	isErrorIs(t, err, ErrMalformedResponse)
}
