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

func Test_connPool_new(t *testing.T) {
	tests := []struct {
		name        string
		maxIdle     int
		maxOpen     int
		maxLifetime time.Duration
		maxIdleTime time.Duration
		wantErr     bool
	}{
		{
			name:        "basic initialization",
			maxIdle:     5,
			maxOpen:     10,
			maxLifetime: time.Hour,
			maxIdleTime: 5 * time.Minute,
			wantErr:     false,
		},
		{
			name:        "zero max connections",
			maxIdle:     0,
			maxOpen:     0,
			maxLifetime: time.Hour,
			maxIdleTime: time.Minute,
			wantErr:     false,
		},
		{
			name:        "maxIdle equals maxOpen",
			maxIdle:     10,
			maxOpen:     10,
			maxLifetime: time.Hour,
			maxIdleTime: time.Minute,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newConnPool(tt.maxIdle, tt.maxOpen, tt.maxLifetime, tt.maxIdleTime, createConn)

			notNil(t, pool)
			equal(t, tt.maxIdle, pool.maxIdle)
			equal(t, tt.maxOpen, pool.maxOpen)
			equal(t, tt.maxIdleTime, pool.maxIdleTime)
			equal(t, 0, pool.stats().IdleConns)
			equal(t, 0, int(pool.numOpen.Load()))
			isFalse(t, pool.closed)
			notNil(t, pool.closedCh)
			isNil(t, pool.cleanerCh)
			equal(t, int64(0), pool.maxIdleClosed)
			equal(t, int64(0), pool.maxIdleTimeClosed)
			equal(t, int64(0), pool.maxLifetimeClosed)
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
// idle list for more than maxIdleTime, it should be closed.
func Test_connPool_cleanup_maxIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// maxLifetime = 3600s
		// maxIdleTime = 1s
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
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifetimeClosed)
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
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifetimeClosed)
		equal(t, int64(5), stat.maxIdleTimeClosed)
		noError(t, pool.close())
	})
}

// Test_connPool_cleanup_maxLife tests the case that connection sits in
// idle list for more than maxLifetime, it should be closed.
func Test_connPool_cleanup_maxLife(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// maxLifetime = 1s
		// maxIdleTime = 3600s
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
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.maxIdleClosed)
		equal(t, int64(0), stat.maxLifetimeClosed)
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
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.maxIdleClosed)
		equal(t, int64(5), stat.maxLifetimeClosed)
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
	pool.maxLifetime = time.Hour

	d, closing := pool.connectionCleanerRunLocked(time.Hour)
	if connsBefore != pool.conns {
		t.Fatal("cleaner must keep the idle channel")
	}
	hasLen(t, closing, 2)
	equal(t, int64(1), pool.maxIdleTimeClosed)
	equal(t, int64(1), pool.maxLifetimeClosed)
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

func Test_connPool_checkoutDropsExpired(t *testing.T) {
	pool := newConnPool(5, 10, 50*time.Millisecond, 0, createConn)
	old := newMockConn()
	old.createdAt = nowFunc().Add(-time.Hour)
	pool.conns <- old
	pool.numOpen.Add(1)

	got, err := pool.get(context.Background())
	noError(t, err)
	notNil(t, got)
	if got == old {
		t.Fatal("expired conn was reused")
	}
	noError(t, pool.close())
}

func Test_connPool_numOpenReservedBeforeCreate(t *testing.T) {
	var creating atomic.Int32
	started := make(chan struct{}, 2)
	block := make(chan struct{})
	pool := newConnPool(1, 1, 0, 0, func(context.Context) (memcachedConn, error) {
		creating.Add(1)
		started <- struct{}{}
		<-block
		return newMockConn(), nil
	})

	go func() { _, _ = pool.get(context.Background()) }()
	<-started
	go func() { _, _ = pool.get(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	equal(t, int32(1), creating.Load())
	close(block)
	noError(t, pool.close())
}

func Test_connPool_discard(t *testing.T) {
	pool := newConnPool(5, 10, 0, 0, func(context.Context) (memcachedConn, error) {
		return &closeTrackingConn{mockConn: newMockConn()}, nil
	})
	cn, err := pool.get(context.Background())
	noError(t, err)
	equal(t, 1, int(pool.numOpen.Load()))

	noError(t, pool.discard(cn))
	equal(t, 0, int(pool.numOpen.Load()))
	equal(t, 0, pool.stats().IdleConns)
	isTrue(t, cn.(*closeTrackingConn).closed.Load())
}

func TestMinIdleDoesNotRetainIdleExpired(t *testing.T) {
	var created atomic.Int32
	pool := newConnPool(2, 4, 0, time.Hour, func(context.Context) (memcachedConn, error) {
		created.Add(1)
		return newMockConn(), nil
	})
	pool.minIdle = 1
	fixed := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	orig := nowFunc
	nowFunc = func() time.Time { return fixed }
	t.Cleanup(func() { nowFunc = orig })

	old := newMockConn()
	old.createdAt = fixed.Add(-time.Minute)
	old.returnedAt = fixed.Add(-2 * time.Hour)
	pool.conns <- old
	pool.numOpen.Store(1)

	_, closing := pool.connectionCleanerRunLocked(time.Hour)
	hasLen(t, closing, 1)
	equal(t, 0, len(pool.conns))
	for _, cn := range closing {
		_ = cn.Close()
		pool.numOpen.Add(-1)
	}
	pool.ensureMinIdle(context.Background())
	equal(t, 1, len(pool.conns))
	equal(t, 1, int(pool.numOpen.Load()))
	equal(t, int32(1), created.Load())
	fresh := <-pool.conns
	if fresh == old {
		t.Fatal("minIdle retained the idle-expired connection")
	}
}

func TestCleanerDoesNotExceedMaxOpenUnderGet(t *testing.T) {
	pool := newConnPool(2, 2, 0, 50*time.Millisecond, createConn)
	pool.minIdle = 2
	ctx := context.Background()
	cn, err := pool.get(ctx)
	noError(t, err)
	noError(t, pool.put(cn))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20 {
			got, err := pool.get(ctx)
			if err != nil {
				return
			}
			_ = pool.put(got)
		}
	}()
	_, closing := pool.connectionCleanerRunLocked(time.Hour)
	for _, c := range closing {
		_ = c.Close()
		pool.numOpen.Add(-1)
	}
	pool.ensureMinIdle(ctx)
	<-done
	lessOrEqual(t, int(pool.numOpen.Load()), pool.maxOpen)
}
