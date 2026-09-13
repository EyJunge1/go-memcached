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

type nowFuncType func() time.Time

var nowFunc nowFuncType = time.Now

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

// memcachedConn wraps a net.Conn and provides a way to read and write data
// from the connection.
// It also provides support for a connection pool mechanism, including expired check,
// idle check and refresh the last time the connection is put back to the pool.
type memcachedConn interface {
	io.ReadWriteCloser

	// readLine reads a line from the connection using the given delimiter.
	readLine(delim byte) ([]byte, error)
	// readExact reads exactly n bytes from the connection.
	readExact(n int) ([]byte, error)
	// readDatagram reads one UDP datagram, preserving packet boundaries.
	readDatagram() ([]byte, error)
	// expired returns true if the connection is expired.
	// it always returns the duration of time since the connection is created.
	expired(since time.Time) (time.Duration, bool)
	// idle returns bool whether memcachedConn stays idle since the given time(since).
	// if false, the duration of time since the connection is idle will be returned.
	idle(since time.Time) (time.Duration, bool)

	// release returns the connection to the pool.
	release() error
	setConnPool(p *connPool)
	getConnPool() *connPool

	setReadDeadline(d time.Time) error
	setWriteDeadline(d time.Time) error
	Flush() error
}

var _ memcachedConn = (*conn)(nil) // tcp socket
// _ memcachedConn = (*unixConn)(nil) // unix domain socket

// conn is a network implementation of memcachedConn.
//
// it wraps a net.Conn and provides a way to read and write
// data from the connection. supports the following three network types:
// tcp / udp / unix domain socket. the Default network type is tcp.
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

func newConnContext(ctx context.Context, addr *Addr, o *clientOptions) (*conn, error) {
	if o == nil {
		o = &clientOptions{tcpNoDelay: true}
	}
	if addr != nil && strings.HasPrefix(addr.Network, "unix") {
		if err := checkUnixSocketMode(addr.Address, o.unixSocketAnyMode, o.unixSocketMaxMode); err != nil {
			return nil, err
		}
	}
	rawConn, err := addr.dial(ctx, o.dialTimeout, o.keepAlive, o.dialContext)
	if err != nil {
		return nil, fmt.Errorf("dialContext: %w", err)
	}
	applyTCPNoDelay(rawConn, o.tcpNoDelay)

	network := ""
	if addr != nil {
		network = addr.Network
	}
	cn := &conn{
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
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

// Read reads data from the connection
func (c *conn) Read(p []byte) (n int, err error) {
	if c.isClosed() {
		return 0, errors.New("connection is closed")
	}

	return c.rr.Read(p)
}

// Write writes data to the connection
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

// Close closes the connection
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
	now := nowFunc()
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
	c.returnedAt = nowFunc()
	// put the connection back to the pool
	return c.pool.put(c)
}
