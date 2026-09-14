package memcached

import (
	"bufio"
	"context"
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

var errConnClosed = errors.New("connection is closed")

// clockFunc is the time source; tests replace clock.
type clockFunc func() time.Time

var clock clockFunc = time.Now

const connWriteBufferSize = 16 << 10

var udpScratchPool = sync.Pool{New: func() any {
	b := make([]byte, 65535)
	return &b
}}

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

// checkUnixSocketMode rejects a Unix socket that is world-writable or exceeds maxMode.
func checkUnixSocketMode(path string, anyMode bool, maxMode os.FileMode) error {
	if anyMode && maxMode == 0 {
		return nil
	}
	fi, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("unix socket: %w", err)
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

	rr *bufio.Reader
	wr *bufio.Writer
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
		rr:         bufio.NewReader(rawConn),
		wr:         bufio.NewWriterSize(rawConn, connWriteBufferSize),
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
	if d.IsZero() {
		return c.raw.SetReadDeadline(zeroTime)
	}

	return c.raw.SetReadDeadline(d)
}

// setWriteDeadline applies a write deadline on the raw socket; a zero time clears it.
func (c *conn) setWriteDeadline(d time.Time) error {
	if d.IsZero() {
		return c.raw.SetWriteDeadline(zeroTime)
	}

	return c.raw.SetWriteDeadline(d)
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

// readLine reads from the buffered socket through the next delim byte.
func (c *conn) readLine(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}

	return c.rr.ReadBytes(delim)
}

// readSlice returns a line that is only valid until the next read.
func (c *conn) readSlice(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	line, err := c.rr.ReadSlice(delim)
	if errors.Is(err, bufio.ErrBufferFull) {
		return c.rr.ReadBytes(delim)
	}
	return line, err
}

// readExact reads exactly n bytes into a fresh buffer for VALUE/VA bodies.
func (c *conn) readExact(n int) ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
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
	return readFramedValueIntoBufio(c.rr, dst)
}

// readDatagram reads one UDP datagram from the raw socket into a copied buffer.
func (c *conn) readDatagram() ([]byte, error) {
	if c.isClosed() {
		return nil, errConnClosed
	}
	scratch := udpScratchPool.Get().(*[]byte)
	n, err := c.raw.Read(*scratch)
	if err != nil {
		udpScratchPool.Put(scratch)
		return nil, err
	}
	out := make([]byte, n)
	copy(out, (*scratch)[:n])
	udpScratchPool.Put(scratch)
	return out, nil
}

// Read implements io.Reader on the buffered socket.
func (c *conn) Read(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errConnClosed
	}

	return c.rr.Read(p)
}

// Write implements io.Writer on the buffered socket.
func (c *conn) Write(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errConnClosed
	}

	return c.wr.Write(p)
}

// Flush implements flushing the write buffer to the socket.
func (c *conn) Flush() error {
	if c.isClosed() {
		return errConnClosed
	}
	return c.wr.Flush()
}

// Close implements io.Closer on the buffered socket.
func (c *conn) Close() error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	if !isUDPNetwork(c.network) {
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

// release clears I/O deadlines, stamps returnedAt, and returns the conn to its pool.
func (c *conn) release() error {
	_ = c.setReadDeadline(zeroTime)
	_ = c.setWriteDeadline(zeroTime)
	c.returnedAt = clock()
	return c.pool.put(c)
}
