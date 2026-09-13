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
	"time"
)

type clockFunc func() time.Time

var clock clockFunc = time.Now

const connWriteBufferSize = 16 << 10

var udpScratchPool = sync.Pool{New: func() any {
	b := make([]byte, 65535)
	return &b
}}

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

// wireConn is one pooled socket: line/framed reads, deadlines, and pool return.
type wireConn interface {
	io.ReadWriteCloser

	readLine(delim byte) ([]byte, error)
	readExact(n int) ([]byte, error)
	readDatagram() ([]byte, error)
	// expired reports whether createdAt is before since. The duration is age.
	expired(since time.Time) (time.Duration, bool)
	// idle reports whether returnedAt is before since. The duration is time left.
	idle(since time.Time) (time.Duration, bool)

	release() error
	setConnPool(p *connPool)
	getConnPool() *connPool

	setReadDeadline(d time.Time) error
	setWriteDeadline(d time.Time) error
	Flush() error
}

var _ wireConn = (*conn)(nil)

// conn is a buffered TCP, Unix, or UDP socket used as a wireConn.
type conn struct {
	createdAt  time.Time
	addr       net.Addr
	network    string
	returnedAt time.Time

	sync.Mutex // guards following
	raw        net.Conn
	closed     bool
	pool       *connPool

	rr *bufio.Reader
	wr *bufio.Writer
}

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

func (c *conn) getConnPool() *connPool {
	return c.pool
}

func (c *conn) setConnPool(p *connPool) {
	c.pool = p
}

var zeroTime = time.Time{}

func (c *conn) setReadDeadline(d time.Time) error {
	if d.IsZero() {
		return c.raw.SetReadDeadline(zeroTime)
	}

	return c.raw.SetReadDeadline(d)
}

func (c *conn) setWriteDeadline(d time.Time) error {
	if d.IsZero() {
		return c.raw.SetWriteDeadline(zeroTime)
	}

	return c.raw.SetWriteDeadline(d)
}

func (c *conn) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

func (c *conn) buffered() int {
	if c.rr == nil {
		return 0
	}
	return c.rr.Buffered()
}

func (c *conn) readLine(delim byte) ([]byte, error) {
	if c.isClosed() {
		return nil, errors.New("connection is closed")
	}

	return c.rr.ReadBytes(delim)
}

func (c *conn) readExact(n int) ([]byte, error) {
	if c.isClosed() {
		return nil, errors.New("connection is closed")
	}
	if n < 0 {
		return nil, fmt.Errorf("negative read: %w", ErrMalformedResponse)
	}
	// Owned buffer: VALUE/VA payloads are stored on the response and Item.
	buf := make([]byte, n)
	_, err := io.ReadFull(c.rr, buf)
	return buf, err
}

func (c *conn) readDatagram() ([]byte, error) {
	if c.isClosed() {
		return nil, errors.New("connection is closed")
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

func (c *conn) Read(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errors.New("connection is closed")
	}

	return c.rr.Read(p)
}

func (c *conn) Write(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errors.New("connection is closed")
	}

	return c.wr.Write(p)
}

func (c *conn) Flush() error {
	if c.isClosed() {
		return errors.New("connection is closed")
	}
	return c.wr.Flush()
}

func (c *conn) Close() error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.closed {
		return nil
	}

	c.closed = true

	if !isUDPNetwork(c.network) {
		_ = c.raw.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = c.raw.Write(_QuitCRLFBytes)
	}

	return c.raw.Close()
}

func (c *conn) expired(since time.Time) (time.Duration, bool) {
	now := clock()
	past := now.Sub(c.createdAt)
	if since.IsZero() {
		return past, false
	}

	return past, c.createdAt.Before(since)
}

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

func (c *conn) release() error {
	_ = c.setReadDeadline(zeroTime)
	_ = c.setWriteDeadline(zeroTime)
	c.returnedAt = clock()
	return c.pool.put(c)
}
