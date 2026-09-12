package memcached

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

type tlsWriteCountConn struct {
	net.Conn

	writes atomic.Uint64
}

func (c *tlsWriteCountConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func testTLSWriteConn(t testing.TB) (*conn, *tlsWriteCountConn) {
	t.Helper()
	cert, key := mustTestTLSCert(t)
	ln := mustTLSListen(t, "tcp", "127.0.0.1:0", &tls.Config{
		Certificates:                []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key}},
		MinVersion:                  tls.VersionTLS13,
		DynamicRecordSizingDisabled: true,
	})
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		raw, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		_, _ = io.Copy(io.Discard, raw)
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	noError(t, err)
	counted := &tlsWriteCountConn{Conn: raw}
	tlsConn := tls.Client(counted, &tls.Config{
		InsecureSkipVerify:          true, // #nosec G402 -- local loopback TLS server has no trusted certificate
		MinVersion:                  tls.VersionTLS13,
		DynamicRecordSizingDisabled: true,
	})
	noError(t, tlsConn.HandshakeContext(context.Background()))
	t.Cleanup(func() { _ = tlsConn.Close() })
	return &conn{
		raw: tlsConn,
		wr:  bufio.NewWriterSize(tlsConn, _connWriteBufferSize),
	}, counted
}

func TestTLSMediumStoreUsesBoundedCoalescing(t *testing.T) {
	cn, counted := testTLSWriteConn(t)
	crlf := []byte("\r\n")
	req := &request{
		raw:  []byte("set key 0 0 65536\r\n"),
		body: make([]byte, 64<<10),
		tail: crlf,
	}
	counted.writes.Store(0)

	noError(t, req.sendReady(context.Background(), cn))
	lessOrEqual(t, counted.writes.Load(), 5)
}

func BenchmarkTLSStoreWriteStrategies(b *testing.B) {
	crlf := []byte("\r\n")
	for _, size := range []int{2 << 10, 4 << 10, 16 << 10, 64 << 10, 1 << 20} {
		for _, strategy := range []string{"bufio", "coalesced"} {
			b.Run(strategy+"/"+strconv.Itoa(size), func(b *testing.B) {
				cn, counted := testTLSWriteConn(b)
				req := &request{
					raw:  []byte("set key 0 0 " + strconv.Itoa(size) + "\r\n"),
					body: make([]byte, size),
					tail: crlf,
				}
				frame := make([]byte, 0, len(req.raw)+len(req.body)+len(req.tail))
				counted.writes.Store(0)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for b.Loop() {
					var err error
					if strategy == "coalesced" {
						frame = frame[:0]
						frame = append(frame, req.raw...)
						frame = append(frame, req.body...)
						frame = append(frame, req.tail...)
						_, err = cn.raw.Write(frame)
					} else {
						err = req.write(cn)
						if err == nil {
							err = cn.Flush()
						}
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(counted.writes.Load())/float64(b.N), "writes/op")
			})
		}
	}
}

func BenchmarkTLSPipelineWriteStrategies(b *testing.B) {
	crlf := []byte("\r\n")
	for _, size := range []int{4 << 10, 16 << 10} {
		for _, strategy := range []string{"bufio", "coalesced"} {
			b.Run(strategy+"/"+strconv.Itoa(size), func(b *testing.B) {
				cn, counted := testTLSWriteConn(b)
				const batch = 8
				reqs := make([]request, batch)
				total := 0
				for i := range reqs {
					reqs[i] = request{
						raw:  []byte("set key-" + strconv.Itoa(i) + " 0 0 " + strconv.Itoa(size) + "\r\n"),
						body: make([]byte, size),
						tail: crlf,
					}
					total += len(reqs[i].raw) + len(reqs[i].body) + len(reqs[i].tail)
				}
				frame := make([]byte, 0, total)
				counted.writes.Store(0)
				b.ReportAllocs()
				b.SetBytes(int64(batch * size))
				b.ResetTimer()
				for b.Loop() {
					var err error
					if strategy == "coalesced" {
						frame = frame[:0]
						for i := range reqs {
							frame = append(frame, reqs[i].raw...)
							frame = append(frame, reqs[i].body...)
							frame = append(frame, reqs[i].tail...)
						}
						_, err = cn.raw.Write(frame)
					} else {
						for i := range reqs {
							if err = reqs[i].writePipeline(cn); err != nil {
								break
							}
						}
						if err == nil {
							err = cn.Flush()
						}
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(counted.writes.Load())/float64(b.N), "writes/op")
			})
		}
	}
}
