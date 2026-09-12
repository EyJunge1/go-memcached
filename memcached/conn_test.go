package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

var _ wireConn = (*mockConn)(nil)

type mockConn struct {
	createdAt          time.Time
	returnedAt         time.Time
	readDeadline       time.Time
	writeDeadline      time.Time
	pool               *connPool
	deadlineCalls      int
	readDeadlineCalls  int
	writeDeadlineCalls int
}

func (m *mockConn) Read(_ []byte) (b int, err error) { return 0, nil }

func (m *mockConn) Write(_ []byte) (n int, err error) { return 0, nil }

func (m *mockConn) Flush() error { return nil }

func (m *mockConn) Close() error { return nil }

func (m *mockConn) isClosed() bool { return false }

func (m *mockConn) readLine(_ byte) ([]byte, error) { return nil, nil }

func (m *mockConn) readExact(n int) ([]byte, error) { return make([]byte, n), nil }

func (m *mockConn) readDatagram() ([]byte, error) { return nil, nil }

func (m *mockConn) expired(since time.Time) (time.Duration, bool) {
	now := time.Now()
	past := now.Sub(m.createdAt)
	if since.IsZero() {
		return past, false
	}

	return past, m.createdAt.Before(since)
}

func (m *mockConn) idle(since time.Time) (time.Duration, bool) {
	if since.IsZero() {
		return m.returnedAt.Sub(since), false
	}

	ok := m.returnedAt.Before(since)
	if ok {
		return 0, true
	}

	return m.returnedAt.Sub(since), false
}

func (m *mockConn) release() error {
	m.returnedAt = time.Now()
	return nil
}

func (m *mockConn) setConnPool(pool *connPool) { m.pool = pool }

func (m *mockConn) getConnPool() *connPool { return m.pool }

func (m *mockConn) setReadDeadline(d time.Time) error {
	m.readDeadlineCalls++
	if d.IsZero() {
		m.readDeadline = _zeroTime
		return nil
	}

	m.readDeadline = d
	return nil
}

func (m *mockConn) setWriteDeadline(d time.Time) error {
	m.writeDeadlineCalls++
	if d.IsZero() {
		m.writeDeadline = _zeroTime
		return nil
	}

	m.writeDeadline = d
	return nil
}

func (m *mockConn) setDeadline(d time.Time) error {
	m.deadlineCalls++
	if d.IsZero() {
		m.readDeadline = _zeroTime
		m.writeDeadline = _zeroTime
		return nil
	}
	m.readDeadline = d
	m.writeDeadline = d
	return nil
}

func (m *mockConn) closeSilent() error {
	return m.Close()
}

type closeTrackingConn struct {
	*mockConn

	closed atomic.Bool
}

func (m *closeTrackingConn) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *closeTrackingConn) closeSilent() error {
	m.closed.Store(true)
	return nil
}

func (m *closeTrackingConn) isClosed() bool { return m.closed.Load() }

func newMockConn() *mockConn {
	return &mockConn{
		createdAt:  time.Now(),
		returnedAt: time.Now(),
	}
}

func createConn(_ context.Context) (wireConn, error) {
	return newMockConn(), nil
}

func TestConnExpiredAndIdle(t *testing.T) {
	created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	returned := created.Add(time.Minute)
	cn := &conn{createdAt: created, returnedAt: returned}

	_, expired := cn.expired(time.Time{})
	isFalse(t, expired)
	_, expired = cn.expired(created.Add(30 * time.Second))
	isTrue(t, expired)
	_, expired = cn.expired(created.Add(-time.Second))
	isFalse(t, expired)

	_, idle := cn.idle(time.Time{})
	isFalse(t, idle)
	_, idle = cn.idle(returned.Add(time.Second))
	isTrue(t, idle)
	d, idle := cn.idle(returned.Add(-time.Second))
	isFalse(t, idle)
	equal(t, time.Second, d)
}

func TestConnClosedIO(t *testing.T) {
	c1, c2 := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, c2)
	}()
	t.Cleanup(func() {
		_ = c2.Close()
		<-done
	})

	cn := &conn{
		createdAt:  time.Now(),
		returnedAt: time.Now(),
		now:        time.Now,
		raw:        c1,
		rr:         bufio.NewReader(c1),
		wr:         bufio.NewWriter(c1),
	}
	noError(t, cn.Close())

	_, err := cn.readLine('\n')
	isError(t, err)
	contains(t, errText(t, err), "closed")
	_, err = cn.Write([]byte("x"))
	isError(t, err)
	contains(t, errText(t, err), "closed")
	err = cn.Flush()
	isError(t, err)
	contains(t, errText(t, err), "closed")
	_, err = cn.Read(make([]byte, 1))
	isError(t, err)
	_, err = cn.readExact(1)
	isError(t, err)
	contains(t, errText(t, err), "closed")
}

type deadlineCountConn struct {
	net.Conn

	deadline atomic.Int32
	read     atomic.Int32
	write    atomic.Int32
}

func (c *deadlineCountConn) SetDeadline(t time.Time) error {
	c.deadline.Add(1)
	return c.Conn.SetDeadline(t)
}

func (c *deadlineCountConn) SetReadDeadline(t time.Time) error {
	c.read.Add(1)
	return c.Conn.SetReadDeadline(t)
}

func (c *deadlineCountConn) SetWriteDeadline(t time.Time) error {
	c.write.Add(1)
	return c.Conn.SetWriteDeadline(t)
}

type writeCountConn struct {
	net.Conn

	n atomic.Int32
}

func (c *writeCountConn) Write(p []byte) (int, error) {
	c.n.Add(1)
	return c.Conn.Write(p)
}

func TestConnSetDeadlineRefreshesEveryOperation(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	dc := &deadlineCountConn{Conn: c1}
	cn := &conn{raw: dc, now: time.Now}
	far := time.Now().Add(5 * time.Second)
	noError(t, cn.setDeadline(far))
	noError(t, cn.setDeadline(time.Now().Add(5*time.Second)))
	equal(t, int32(2), dc.deadline.Load())
	noError(t, cn.setDeadline(time.Now().Add(100*time.Millisecond)))
	equal(t, int32(3), dc.deadline.Load())
}

func TestConnReleaseLeavesDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	dc := &deadlineCountConn{Conn: c1}
	pool := newConnPool(1, 1, 0, 0, createConn)
	cn := &conn{raw: dc, pool: pool, wr: bufio.NewWriter(dc)}
	cn.setConnPool(pool)
	pool.open.Add(1)
	noError(t, cn.release())
	equal(t, int32(0), dc.deadline.Load())
	equal(t, int32(0), dc.read.Load())
	equal(t, int32(0), dc.write.Load())
}
