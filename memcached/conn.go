package memcached

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minTLSCoalesceBytes = 32 << 10
	maxTLSCoalesceBytes = 256 << 10
)

var tlsWriteBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 64<<10)
		return &buf
	},
}

var errConnClosed = errors.New("connection is closed")

// clockFunc is the time source; tests replace clock.
type clockFunc func() time.Time

var clock clockFunc = time.Now

const connWriteBufferSize = 16 << 10

// applyTCPNoDelay sets TCP_NODELAY on c when it unwraps to a *net.TCPConn.
func applyTCPNoDelay(c net.Conn, on bool) {
	type netConner interface{ NetConn() net.Conn }
	for i := 0; i < 3 && c != nil; i++ {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(on)
			return
		}
		if nc, ok := c.(netConner); ok {
			c = nc.NetConn()
			continue
		}
		return
	}
}

// checkUnixSocketMode rejects a Unix socket that is a symlink, world-writable, or exceeds maxMode.
func checkUnixSocketMode(path string, anyMode bool, maxMode os.FileMode) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("unix socket: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unix socket symlink not allowed: %w", ErrInvalidArgument)
	}
	if anyMode && maxMode == 0 {
		return nil
	}
	perm := fi.Mode().Perm()
	if !anyMode && perm&0o002 != 0 {
		return fmt.Errorf("unix socket is world-writable: %w", ErrInvalidArgument)
	}
	if maxMode != 0 && perm&^maxMode.Perm() != 0 {
		return fmt.Errorf("unix socket mode %#o exceeds max %#o: %w", perm, maxMode.Perm(), ErrInvalidArgument)
	}
	return nil
}

// wireConn is one pooled socket: line or framed reads, deadlines, and pool return.
type wireConn interface {
	io.ReadWriteCloser

	readLine(delim byte) ([]byte, error)
	readExact(n int) ([]byte, error)
	readDatagram() ([]byte, error)
	// expired reports age and whether createdAt is before since.
	expired(since time.Time) (time.Duration, bool)
	// idle reports remaining idle time and whether returnedAt is before since.
	idle(since time.Time) (time.Duration, bool)

	release() error
	setConnPool(p *connPool)
	getConnPool() *connPool

	setReadDeadline(d time.Time) error
	setWriteDeadline(d time.Time) error
	setDeadline(d time.Time) error
	closeSilent() error
	Flush() error
}

var _ wireConn = (*conn)(nil)

// conn is a buffered TCP, Unix, or UDP socket that implements wireConn.
type conn struct {
	createdAt  time.Time
	addr       net.Addr
	network    string
	returnedAt time.Time

	sync.Mutex // guards Close
	raw        net.Conn
	closed     atomic.Bool
	pool       *connPool

	rr       *bufio.Reader
	wr       *bufio.Writer
	iov      net.Buffers
	iovBuf   [3][]byte
	batchIOV net.Buffers
	udpIn    []byte
	udpOut   []byte
	udpEpoch uint64
}

// newConnContext dials node, applies TCP_NODELAY, and returns a buffered wireConn.
func newConnContext(ctx context.Context, node *addr, o *runtime) (*conn, error) {
	if o == nil {
		o = &runtime{tcpNoDelay: true}
	}
	if node != nil && strings.HasPrefix(node.Network, "unix") {
		if err := checkUnixSocketMode(node.Address, o.unixSocketAnyMode, o.unixSocketMaxMode); err != nil {
			return nil, err
		}
	}
	rawConn, err := node.dial(ctx, o.dialTimeout, o.keepAlive, o.dialContext)
	if err != nil {
		return nil, fmt.Errorf("dialContext: %w", err)
	}
	applyTCPNoDelay(rawConn, o.tcpNoDelay)

	network := ""
	if node != nil {
		network = node.Network
	}
	cn := &conn{
		createdAt:  clock(),
		returnedAt: clock(),
		raw:        rawConn,
		addr:       rawConn.RemoteAddr(),
		network:    network,
	}
	if isUDPNetwork(network) {
		// One raw Write is one datagram. bufio would copy and can split >16KiB.
		cn.udpIn = make([]byte, 65535)
		cn.udpOut = make([]byte, 0, defaultBufferSize)
		cn.udpEpoch = udpRequestEpoch.Load()
	} else {
		cn.rr = bufio.NewReaderSize(rawConn, connWriteBufferSize)
		cn.wr = bufio.NewWriterSize(rawConn, connWriteBufferSize)
	}

	return cn, nil
}

// getConnPool returns the pool that currently owns this conn, if any.
func (c *conn) getConnPool() *connPool {
	return c.pool
}

// setConnPool records the pool that owns this conn.
func (c *conn) setConnPool(p *connPool) {
	c.pool = p
}

var zeroTime = time.Time{}

// setReadDeadline applies a read deadline on the raw socket; a zero time clears it.
func (c *conn) setReadDeadline(d time.Time) error {
	return c.raw.SetReadDeadline(d)
}

// setWriteDeadline applies a write deadline on the raw socket; a zero time clears it.
func (c *conn) setWriteDeadline(d time.Time) error {
	return c.raw.SetWriteDeadline(d)
}

// setDeadline applies one combined read/write deadline; a zero time clears both.
func (c *conn) setDeadline(d time.Time) error {
	return c.raw.SetDeadline(d)
}

// isClosed reports whether Close has already been called.
func (c *conn) isClosed() bool {
	return c.closed.Load()
}

// buffered returns the number of unread bytes in the read buffer.
func (c *conn) buffered() int {
	if c.rr == nil {
		return 0
	}
	return c.rr.Buffered()
}

const protocolLineLimit = 64 * 1024

// readLine reads from the buffered socket through the next delim byte.
func (c *conn) readLine(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	return readLimitedLine(c.rr, delim, protocolLineLimit)
}

// readSlice returns a line that is only valid until the next read.
func (c *conn) readSlice(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	line, err := c.rr.ReadSlice(delim)
	if err == nil {
		if len(line) > protocolLineLimit {
			return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
		}
		return line, nil
	}
	if errors.Is(err, bufio.ErrBufferFull) {
		return readLimitedLineContinue(c.rr, delim, protocolLineLimit, line)
	}
	return line, err
}

// readLimitedLine reads through delim with a hard byte cap.
func readLimitedLine(r *bufio.Reader, delim byte, limit int) ([]byte, error) {
	line, err := r.ReadSlice(delim)
	if err == nil {
		if len(line) > limit {
			return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
		}
		return bytes.Clone(line), nil
	}
	if errors.Is(err, bufio.ErrBufferFull) {
		return readLimitedLineContinue(r, delim, limit, line)
	}
	if len(line) == 0 {
		return nil, err
	}
	if len(line) > limit {
		return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
	}
	return bytes.Clone(line), err
}

func readLimitedLineContinue(r *bufio.Reader, delim byte, limit int, first []byte) ([]byte, error) {
	if len(first) >= limit {
		return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
	}
	buf := make([]byte, len(first), min(limit, max(len(first)*2, 64)))
	copy(buf, first)
	for {
		line, err := r.ReadSlice(delim)
		if len(line) > 0 {
			if len(buf)+len(line) > limit {
				return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
			}
			buf = append(buf, line...)
		}
		if err == nil {
			return buf, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(buf) >= limit {
				return nil, fmt.Errorf("protocol line too long: %w", ErrMalformedResponse)
			}
			continue
		}
		if len(buf) > 0 && errors.Is(err, io.EOF) {
			return buf, io.EOF
		}
		if len(buf) == 0 {
			return nil, err
		}
		return buf, err
	}
}

// readExact reads exactly n bytes into a fresh buffer for VALUE/VA bodies.
func (c *conn) readExact(n int) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.rr == nil {
		return nil, fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	if n < 0 {
		return nil, fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(c.rr, buf)
	return buf, err
}

func (c *conn) readFramedValueInto(dst []byte) error {
	if c.isClosed() {
		return errConnClosed
	}
	if c.rr == nil {
		return fmt.Errorf("udp stream read: %w", ErrNotSupported)
	}
	return readFramedValueIntoBufio(c.rr, dst)
}

// readDatagram reads one UDP datagram. The slice is valid until the next
// readDatagram or Close on this conn; assembleUDP copies what it keeps.
func (c *conn) readDatagram() ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	if c.udpIn == nil {
		c.udpIn = make([]byte, 65535)
	}
	n, err := c.raw.Read(c.udpIn)
	if err != nil {
		return nil, err
	}
	return c.udpIn[:n], nil
}

// Read implements io.Reader on the buffered socket.
func (c *conn) Read(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errConnClosed
	}
	if c.rr == nil {
		return c.raw.Read(p)
	}

	return c.rr.Read(p)
}

// Write implements io.Writer on the buffered socket.
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

func takeTLSWriteBuffer(n int) *[]byte {
	box := tlsWriteBufferPool.Get().(*[]byte)
	if cap(*box) < n {
		*box = make([]byte, 0, n)
	} else {
		*box = (*box)[:0]
	}
	return box
}

func putTLSWriteBuffer(box *[]byte) {
	if box == nil || cap(*box) > maxTLSCoalesceBytes {
		return
	}
	*box = (*box)[:0]
	tlsWriteBufferPool.Put(box)
}

func requestWireSize(req *request) int {
	return len(req.raw) + len(req.body) + len(req.tail)
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
	if cap(bufs) > 1024 {
		c.batchIOV = nil
	} else {
		c.batchIOV = bufs[:0]
	}
	c.iov = c.iovBuf[:0]
	return n, err
}

// Close implements io.Closer on the buffered socket.
func (c *conn) Close() error {
	return c.shutdown(true)
}

// closeSilent closes the socket without sending quit. Pool overflow and
// transport discard use this; Client.Close still sends quit.
func (c *conn) closeSilent() error {
	return c.shutdown(false)
}

func (c *conn) shutdown(quit bool) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	if quit && !isUDPNetwork(c.network) {
		_ = c.raw.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = c.raw.Write(_QuitCRLFBytes)
	}

	return c.raw.Close()
}

// expired reports the conn age and whether createdAt is before since.
func (c *conn) expired(since time.Time) (time.Duration, bool) {
	now := clock()
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
	if isUDPNetwork(c.network) && c.udpEpoch != udpRequestEpoch.Load() {
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
		c.returnedAt = clock()
	}
	return p.put(c)
}
