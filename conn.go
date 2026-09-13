package memcached

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type nowFuncType func() time.Time

var nowFunc nowFuncType = time.Now

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
	returnedAt time.Time

	sync.Mutex // guards following
	raw        net.Conn
	closed     bool
	pool       *connPool

	rr *bufio.Reader
	wr *bufio.Writer
}

// newConnWithContext dials a TCP connection
func newConnContext(ctx context.Context, addr *Addr, dialTimeout, keepAlive time.Duration, dialFn DialContextFunc) (*conn, error) {
	rawConn, err := addr.dial(ctx, dialTimeout, keepAlive, dialFn)
	if err != nil {
		return nil, fmt.Errorf("dialContext: %w", err)
	}

	cn := &conn{
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		raw:        rawConn,
		addr:       rawConn.RemoteAddr(),
		rr:         bufio.NewReader(rawConn),
		wr:         bufio.NewWriter(rawConn),
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
	buf := make([]byte, n)
	_, err := io.ReadFull(c.rr, buf)
	return buf, err
}

func (c *conn) readDatagram() ([]byte, error) {
	if c.isClosed() {
		return nil, errors.New("connection is closed")
	}
	buf := make([]byte, 65535)
	n, err := c.raw.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
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

	n, err = c.wr.Write(p)
	if err != nil {
		return n, err
	}
	return n, c.wr.Flush()
}

// Close closes the connection
func (c *conn) Close() error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.closed {
		return nil
	}

	c.closed = true

	// send quit command to the server
	_ = c.raw.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, _ = c.raw.Write(_QuitCRLFBytes)

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
