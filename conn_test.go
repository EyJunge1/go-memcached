package memcached

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

var _ memcachedConn = (*mockConn)(nil)

// mockConn is an implementation of the memcachedConn interface for testing purposes.
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

func (m *mockConn) Close() error { return nil }

func (m *mockConn) readLine(_ byte) ([]byte, error) { return nil, nil }

func (m *mockConn) expired(since time.Time) (time.Duration, bool) {
	now := nowFunc()
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

func createConn(_ context.Context) (memcachedConn, error) {
	return newMockConn(), nil
}

func Test_connPool_new(t *testing.T) {
	tests := []struct {
		name           string
		maxIdle        int
		maxConn        int
		maxLifeTime    time.Duration
		maxIdleTimeout time.Duration
		wantErr        bool
	}{
		{
			name:           "basic initialization",
			maxIdle:        5,
			maxConn:        10,
			maxLifeTime:    time.Hour,
			maxIdleTimeout: 5 * time.Minute,
			wantErr:        false,
		},
		{
			name:           "zero max connections",
			maxIdle:        0,
			maxConn:        0,
			maxLifeTime:    time.Hour,
			maxIdleTimeout: time.Minute,
			wantErr:        false,
		},
		{
			name:           "maxIdle equals maxConn",
			maxIdle:        10,
			maxConn:        10,
			maxLifeTime:    time.Hour,
			maxIdleTimeout: time.Minute,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newConnPool(tt.maxIdle, tt.maxConn, tt.maxLifeTime, tt.maxIdleTimeout, createConn)

			notNil(t, pool)
			equal(t, tt.maxIdle, pool.maxIdle)
			equal(t, tt.maxConn, pool.maxConns)
			equal(t, tt.maxIdleTimeout, pool.maxIdleTime)
			equal(t, 0, pool.stats().IdleConns)
			equal(t, 0, int(pool.numOpen.Load()))
			isFalse(t, pool.closed)
			notNil(t, pool.closedCh)
			isNil(t, pool.cleanerCh)
			equal(t, int64(0), pool.maxIdleClosed)
			equal(t, int64(0), pool.maxIdleTimeClosed)
			equal(t, int64(0), pool.maxLifeTimeClosed)
		})
	}
}

func Test_connPool_get_put(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(5, 10, time.Hour, 5*time.Minute, createConn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var wg sync.WaitGroup
		for i := range 10 {
			wg.Go(func() {
				cn, err := pool.get(ctx)
				isNil(t, err)
				notNil(t, cn)

				time.Sleep(time.Duration(i*100) * time.Millisecond)

				err = pool.put(cn)
				isNil(t, err)
			})
		}

		wg.Wait()

		equal(t, 5, int(pool.numOpen.Load()))
		equal(t, 5, pool.stats().IdleConns)
		noError(t, pool.close())
	})
}

// Test_connPool_get_timeout_case1 mocking the case that the createConn function
// takes longer than the context timeout to return a connection.
func Test_connPool_get_timeout_case1(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		createConn := func(ctx context.Context) (memcachedConn, error) {
			delay := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-delay.C:
			}
			return newMockConn(), nil
		}

		pool := newConnPool(5, 10, time.Hour, 5*time.Minute, createConn)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		conn, err := pool.get(ctx)
		isError(t, err)
		isTrue(t, errors.Is(err, context.DeadlineExceeded))
		isNil(t, conn)
		noError(t, pool.close())
	})
}

// Test_connPool_get_timeout_case2 mocking the case that the all connections
// are occupied, we must wait for a connection to be put back to the pool but
// no connection is put back before (get) context timeout deadline.
func Test_connPool_get_timeout_case2(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(5, 10, time.Hour, 5*time.Minute, createConn)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		for range 10 {
			go func() {
				cn, err := pool.get(ctx)
				isNil(t, err)
				notNil(t, cn)

				time.Sleep(200 * time.Millisecond)
				_ = pool.put(cn)
			}()
		}

		time.Sleep(100 * time.Millisecond)

		conn, err := pool.get(ctx)
		isError(t, err)
		isNil(t, conn)
		time.Sleep(200 * time.Millisecond)
		noError(t, pool.close())
	})
}

func Test_connPool_get_oversize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(5, 10, time.Hour, 5*time.Minute, createConn)

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				cn, err := pool.get(ctx)
				isNil(t, err)
				notNil(t, cn)

				time.Sleep(200 * time.Millisecond)
				_ = pool.put(cn)
			})
		}

		time.Sleep(100 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		cn, err := pool.get(ctx)
		isError(t, err)
		isTrue(t, errors.Is(err, context.DeadlineExceeded))
		isNil(t, cn)

		wg.Wait()

		equal(t, 5, int(pool.numOpen.Load()))
		equal(t, 5, pool.stats().IdleConns)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		cn, err = pool.get(ctx)
		isNil(t, err)
		notNil(t, cn)
		noError(t, pool.close())
	})
}

// Test_connPool_cleanup_maxIdle tests the case that connection sits in
// idle list for more than maxIdleTimeout, it should be closed.
func Test_connPool_cleanup_maxIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// maxLifeTime = 3600s
		// maxIdleTimeout = 1s
		pool := newConnPool(5, 10, time.Hour, time.Second, createConn)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// get 10 connections and put them back, now there are 10 idle connections
		// and 10 numOpen connections.
		for range 10 {
			go func() {
				cn, err := pool.get(ctx)
				isNil(t, err)
				notNil(t, cn)

				time.Sleep(200 * time.Millisecond)
				_ = pool.put(cn)
			}()
		}

		time.Sleep(100 * time.Millisecond)
		stat := pool.stats()
		t.Logf("Before cleanup: %+v", stat)
		equal(t, 10, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxConns)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifeTimeClosed)
		equal(t, int64(0), stat.maxIdleTimeClosed)

		// wait for the cleaner to clean up the idle connection, decrease the numOpen
		// connections to 5.
		time.Sleep(3 * time.Second)
		equal(t, 0, int(pool.numOpen.Load()))
		equal(t, 0, pool.stats().IdleConns)
		isFalse(t, pool.closed)

		stat = pool.stats()
		t.Logf("After cleanup: %+v", stat)
		equal(t, 0, stat.TotalConns)
		equal(t, 0, stat.IdleConns) // since all connections are cleaned up(idle=1s)
		equal(t, 10, stat.MaxConns)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifeTimeClosed)
		equal(t, int64(5), stat.maxIdleTimeClosed)
		noError(t, pool.close())
	})
}

// Test_connPool_cleanup_maxLife tests the case that connection sits in
// idle list for more than maxLifeTime, it should be closed.
func Test_connPool_cleanup_maxLife(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// maxLifeTime = 1s
		// maxIdleTimeout = 3600s
		pool := newConnPool(5, 10, 2*time.Second, time.Hour, createConn)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// get 10 connections and put them back, now there are 10 idle connections
		// and 10 numOpen connections.
		for range 10 {
			go func() {
				cn, err := pool.get(ctx)
				isNil(t, err)
				notNil(t, cn)

				time.Sleep(200 * time.Millisecond)
				_ = pool.put(cn)
			}()
		}

		time.Sleep(100 * time.Millisecond)
		stat := pool.stats()
		t.Logf("Before cleanup: %+v", stat)
		equal(t, 10, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxConns)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifeTimeClosed)
		equal(t, int64(0), stat.maxIdleTimeClosed)

		// wait for the cleaner to clean up the idle connection, decrease the numOpen
		// connections to 5.
		time.Sleep(3 * time.Second)
		equal(t, 0, int(pool.numOpen.Load()))
		equal(t, 0, pool.stats().IdleConns)
		isFalse(t, pool.closed)

		stat = pool.stats()
		t.Logf("After cleanup: %+v", stat)
		equal(t, 0, stat.TotalConns)
		equal(t, 0, stat.IdleConns) // since all connections are cleaned up(life=2s)
		equal(t, 10, stat.MaxConns)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.maxIdleClosed)
		equal(t, int64(5), stat.maxLifeTimeClosed)
		equal(t, int64(0), stat.maxIdleTimeClosed)
		noError(t, pool.close())
	})
}

func Test_connectionCleanerRunLocked_both_limits(t *testing.T) {
	pool := newConnPool(10, 10, time.Hour, time.Hour, createConn)
	fixedNow := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	origNow := nowFunc
	nowFunc = func() time.Time { return fixedNow }
	t.Cleanup(func() { nowFunc = origNow })

	idleExpired := newMockConn()
	idleExpired.createdAt = fixedNow.Add(-30 * time.Minute)
	idleExpired.returnedAt = fixedNow.Add(-2 * time.Hour)

	lifeExpired := newMockConn()
	lifeExpired.createdAt = fixedNow.Add(-2 * time.Hour)
	lifeExpired.returnedAt = fixedNow.Add(-time.Minute)

	fresh := newMockConn()
	fresh.createdAt = fixedNow.Add(-time.Minute)
	fresh.returnedAt = fixedNow.Add(-time.Minute)

	pool.conns <- idleExpired
	pool.conns <- lifeExpired
	pool.conns <- fresh
	pool.numOpen.Store(3)
	connsBefore := pool.conns

	pool.maxIdleTime = time.Hour
	pool.maxLifeTime = time.Hour

	d, closing := pool.connectionCleanerRunLocked(time.Hour)
	if connsBefore != pool.conns {
		t.Fatal("cleaner must keep the idle channel")
	}
	hasLen(t, closing, 2)
	equal(t, int64(1), pool.maxIdleTimeClosed)
	equal(t, int64(1), pool.maxLifeTimeClosed)
	equal(t, 1, len(pool.conns))
	greater(t, d, time.Duration(0))

	kept := <-pool.conns
	same(t, fresh, kept)
}

func Test_connPool_cleanup_concurrent_get(t *testing.T) {
	pool := newConnPool(8, 8, time.Hour, 100*time.Millisecond, createConn)
	ctx := context.Background()

	for range 8 {
		cn, err := pool.get(ctx)
		noError(t, err)
		noError(t, pool.put(cn))
	}

	done := make(chan struct{})
	var getErr error
	go func() {
		defer close(done)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			cn, err := pool.get(ctx)
			if err != nil {
				getErr = err
				return
			}
			if cn == nil {
				getErr = errors.New("nil conn")
				return
			}
			_ = pool.put(cn)
		}
	}()

	<-done
	noError(t, getErr)
	noError(t, pool.close())
	equal(t, 0, int(pool.numOpen.Load()))
}

func Test_connPool_close_wakes_waiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(1, 1, time.Hour, time.Hour, createConn)
		ctx := context.Background()

		cn, err := pool.get(ctx)
		noError(t, err)
		notNil(t, cn)

		errCh := make(chan error, 1)
		go func() {
			_, getErr := pool.get(ctx)
			errCh <- getErr
		}()

		time.Sleep(50 * time.Millisecond)
		noError(t, pool.close())

		select {
		case getErr := <-errCh:
			isErrorIs(t, getErr, errPoolClosed)
		case <-time.After(time.Second):
			t.Fatal("waiting get was not woken by close")
		}

		noError(t, pool.put(cn))
		equal(t, 0, int(pool.numOpen.Load()))
		equal(t, 0, pool.stats().IdleConns)
	})
}

func Test_connPool_get_closes_connection_created_during_close(t *testing.T) {
	creationStarted := make(chan struct{})
	releaseCreation := make(chan struct{})
	createdConn := &closeTrackingConn{mockConn: newMockConn()}
	pool := newConnPool(1, 1, time.Hour, time.Hour, func(context.Context) (memcachedConn, error) {
		close(creationStarted)
		<-releaseCreation
		return createdConn, nil
	})

	type getResult struct {
		conn memcachedConn
		err  error
	}
	resultCh := make(chan getResult, 1)
	go func() {
		conn, err := pool.get(context.Background())
		resultCh <- getResult{conn: conn, err: err}
	}()

	<-creationStarted
	noError(t, pool.close())
	close(releaseCreation)

	result := <-resultCh
	isNil(t, result.conn)
	isErrorIs(t, result.err, errPoolClosed)
	isTrue(t, createdConn.closed.Load())
	equal(t, 0, int(pool.numOpen.Load()))
}
