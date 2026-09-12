package memcached

import (
	"crypto/tls"
	"io"
	"net"
	"time"
)

func (c *conn) Write(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errConnClosed
	}
	if c.wr == nil {
		return c.raw.Write(p)
	}

	return c.wr.Write(p)
}

// Flush implements flushing the write buffer to the socket.
func (c *conn) Flush() error {
	if c.isClosed() {
		return errConnClosed
	}
	if c.wr == nil {
		return nil
	}
	return c.wr.Flush()
}

// writeLargeBody writes header, body, and tail with writev so the payload
// is not copied through bufio. Caller must have an empty writer.
// iovBuf is the stable backing store: Buffers.WriteTo consume() advances
// the slice start and would otherwise drop cap to 0 (alloc per call).
func (c *conn) writeLargeBody(raw, body, tail []byte) error {
	if c.isClosed() {
		return errConnClosed
	}
	n := 2
	c.iovBuf[0] = raw
	c.iovBuf[1] = body
	if len(tail) > 0 {
		c.iovBuf[2] = tail
		n = 3
	}
	c.iov = c.iovBuf[:n]
	_, err := c.iov.WriteTo(c.raw)
	c.iovBuf = [3][]byte{}
	c.iov = c.iovBuf[:0]
	return err
}

// supportsPipelineWritev reports whether net.Buffers can use the concrete
// socket's vectored-write implementation. Wrapped sockets such as tls.Conn
// deliberately stay on the buffered path.
func (c *conn) supportsPipelineWritev() bool {
	switch c.raw.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	default:
		return false
	}
}

func (c *conn) isTLS() bool {
	_, ok := c.raw.(*tls.Conn)
	return ok
}

// writeTLSBatch coalesces medium frames into one tls.Conn.Write. The bounded
// copy avoids tiny records without copying very large values.
func (c *conn) writeTLSBatch(reqs []*request, total int) (int64, error) {
	if c.isClosed() {
		return 0, errConnClosed
	}
	box := takeTLSWriteBuffer(total)
	defer putTLSWriteBuffer(box)
	for _, req := range reqs {
		*box = append(*box, req.raw...)
		*box = append(*box, req.body...)
		*box = append(*box, req.tail...)
	}
	written := 0
	for written < len(*box) {
		n, err := c.raw.Write((*box)[written:])
		written += n
		if err != nil {
			return int64(written), err
		}
		if n == 0 {
			return int64(written), io.ErrShortWrite
		}
	}
	return int64(written), nil
}

// writePipelineBatch emits several frames with one vectored socket write
// sequence. The caller must have an empty buffered writer.
func (c *conn) writePipelineBatch(reqs []*request) (int64, error) {
	if c.isClosed() {
		return 0, errConnClosed
	}
	parts := 0
	for _, req := range reqs {
		parts++
		if len(req.body) > 0 {
			parts++
		}
		if len(req.tail) > 0 {
			parts++
		}
	}
	if cap(c.batchIOV) < parts {
		c.batchIOV = make(net.Buffers, 0, parts)
	}
	bufs := c.batchIOV[:0]
	for _, req := range reqs {
		bufs = append(bufs, req.raw)
		if len(req.body) > 0 {
			bufs = append(bufs, req.body)
		}
		if len(req.tail) > 0 {
			bufs = append(bufs, req.tail)
		}
	}
	c.iov = bufs
	n, err := c.iov.WriteTo(c.raw)
	for i := range bufs {
		bufs[i] = nil
	}
	c.batchIOV = bufs[:0]
	if cap(bufs) > 1024 {
		c.batchIOV = nil
	}
	c.iov = c.iovBuf[:0]
	return n, err
}

// Close implements io.Closer on the buffered socket.
func (c *conn) Close() error {
	return c.shutdown(true /* quit */)
}

// closeSilent closes the socket without sending quit. Pool overflow and
// transport discard use this; Client.Close still sends quit.
func (c *conn) closeSilent() error {
	return c.shutdown(false /* quit */)
}

func (c *conn) shutdown(quit bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	if quit && !isUDPNetwork(c.network) {
		_ = c.raw.SetWriteDeadline(resolveClock(c.now)().Add(100 * time.Millisecond)) // best-effort: quit probe
		_, _ = c.raw.Write(_QuitCRLFBytes)                                            // best-effort: quit before close
	}

	return c.raw.Close()
}

// expired reports the conn age and whether createdAt is before since.
func (c *conn) expired(since time.Time) (time.Duration, bool) {
	now := resolveClock(c.now)()
	past := now.Sub(c.createdAt)
	if since.IsZero() {
		return past, false
	}

	return past, c.createdAt.Before(since)
}

// idle reports remaining idle time and whether returnedAt is before since.
func (c *conn) idle(since time.Time) (time.Duration, bool) {
	if since.IsZero() {
		return c.returnedAt.Sub(since), false
	}

	ok := c.returnedAt.Before(since)
	if ok {
		return 0, true
	}

	return c.returnedAt.Sub(since), false
}

// release parks the conn. The next checkout overwrites the deadline; idle
// tracking is stamped only when the pool enforces lifetime or idle time.
// UDP sockets from a prior request-id epoch are discarded so a wrapped 16-bit
// id cannot pick up a stale datagram on the reused socket.
func (c *conn) release() error {
	if isUDPNetwork(c.network) && c.udpEpoch != _udpRequestEpoch.Load() {
		if p := c.pool; p != nil {
			return p.discard(c)
		}
		return c.closeSilent()
	}
	p := c.pool
	if p == nil {
		return c.closeSilent()
	}
	if p.maxIdleTime > 0 || p.maxLifetime > 0 {
		c.returnedAt = resolveClock(c.now)()
	}
	return p.put(c)
}

func takeTLSWriteBuffer(n int) *[]byte {
	box, ok := _tlsWriteBufferPool.Get().(*[]byte)
	if !ok || box == nil {
		buf := make([]byte, 0, 64<<10)
		box = &buf
	}
	*box = (*box)[:0]
	if cap(*box) < n {
		*box = make([]byte, 0, n)
	}
	return box
}

func putTLSWriteBuffer(box *[]byte) {
	if box == nil || cap(*box) > _maxTLSCoalesceBytes {
		return
	}
	*box = (*box)[:0]
	_tlsWriteBufferPool.Put(box)
}

func requestWireSize(req *request) int {
	return len(req.raw) + len(req.body) + len(req.tail)
}
