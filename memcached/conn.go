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

const (
	_minTLSCoalesceBytes = 32 << 10
	_maxTLSCoalesceBytes = 256 << 10
)

var (
	_tlsWriteBufferPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 64<<10)
			return &buf
		},
	}
	errConnClosed = errors.New("connection is closed")
)

// clockFunc is the time source used by deadlines and idle accounting.
type clockFunc func() time.Time

// resolveClock returns now, or time.Now when now is nil.
func resolveClock(now clockFunc) clockFunc {
	if now == nil {
		return time.Now
	}
	return now
}

const _connWriteBufferSize = 16 << 10

// applyTCPNoDelay sets TCP_NODELAY on c when it unwraps to a *net.TCPConn.
func applyTCPNoDelay(c net.Conn, on bool) {
	type netConner interface{ NetConn() net.Conn }
	for i := 0; i < 3 && c != nil; i++ {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(on) // best-effort: TCP_NODELAY may fail on closed/non-TCP conns
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
	isClosed() bool

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
	now        clockFunc

	mu     sync.Mutex // guards Close
	raw    net.Conn
	closed atomic.Bool
	pool   *connPool

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
	now := time.Now
	cn := &conn{
		createdAt:  now(),
		returnedAt: now(),
		now:        now,
		raw:        rawConn,
		addr:       rawConn.RemoteAddr(),
		network:    network,
	}
	if isUDPNetwork(network) {
		// One raw Write is one datagram. bufio would copy and can split >16KiB.
		cn.udpIn = make([]byte, 65535)
		cn.udpOut = make([]byte, 0, _defaultBufferSize)
		cn.udpEpoch = _udpRequestEpoch.Load()
	} else {
		cn.rr = bufio.NewReaderSize(rawConn, _connWriteBufferSize)
		cn.wr = bufio.NewWriterSize(rawConn, _connWriteBufferSize)
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

var _zeroTime time.Time

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

const _protocolLineLimit = 64 * 1024
