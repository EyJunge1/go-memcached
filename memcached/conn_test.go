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
	createdAt     time.Time
	returnedAt    time.Time
	readDeadline  time.Time
	writeDeadline time.Time
	pool          *connPool
}

type closeTrackingConn struct {
	*mockConn
	closed atomic.Bool
}

func (m *closeTrackingConn) Close() error {
	m.closed.Store(true)
	return nil
}

func newMockConn() *mockConn {
	return &mockConn{
		createdAt:  time.Now(),
		returnedAt: time.Now(),
	}
}

func (m *mockConn) Read(_ []byte) (b int, err error) { return 0, nil }

func (m *mockConn) Write(_ []byte) (n int, err error) { return 0, nil }

func (m *mockConn) Flush() error { return nil }

func (m *mockConn) Close() error { return nil }

func (m *mockConn) readLine(_ byte) ([]byte, error) { return nil, nil }

func (m *mockConn) readExact(n int) ([]byte, error) { return make([]byte, n), nil }

func (m *mockConn) readDatagram() ([]byte, error) { return nil, nil }

func (m *mockConn) expired(since time.Time) (time.Duration, bool) {
	now := clock()
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
	if d.IsZero() {
		m.readDeadline = zeroTime
		return nil
	}

	m.readDeadline = d
	return nil
}

func (m *mockConn) setWriteDeadline(d time.Time) error {
	if d.IsZero() {
		m.writeDeadline = zeroTime
		return nil
	}

	m.writeDeadline = d
	return nil
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
	t.Cleanup(func() { _ = c2.Close() })
	go func() { _, _ = io.Copy(io.Discard, c2) }()

	cn := &conn{
		createdAt:  clock(),
		returnedAt: clock(),
		raw:        c1,
		rr:         bufio.NewReader(c1),
		wr:         bufio.NewWriter(c1),
	}
	noError(t, cn.Close())

	_, err := cn.readLine('\n')
	isError(t, err)
	contains(t, err.Error(), "closed")
	_, err = cn.Write([]byte("x"))
	isError(t, err)
	contains(t, err.Error(), "closed")
	err = cn.Flush()
	isError(t, err)
	contains(t, err.Error(), "closed")
	_, err = cn.Read(make([]byte, 1))
	isError(t, err)
	_, err = cn.readExact(1)
	isError(t, err)
	contains(t, err.Error(), "closed")
}
