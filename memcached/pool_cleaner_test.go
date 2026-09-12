package memcached

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestConnPoolCleanupMaxIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(5, 10, time.Hour, time.Second, createConn)
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
		stat := pool.stats()
		t.Logf("Before cleanup: %+v", stat)
		equal(t, 10, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.closedIdleCap)
		equal(t, int64(0), stat.closedLifetime)
		equal(t, int64(0), stat.closedIdleAge)

		time.Sleep(3 * time.Second)
		equal(t, 0, int(pool.open.Load()))
		equal(t, 0, pool.stats().IdleConns)
		isFalse(t, pool.closed.Load())

		stat = pool.stats()
		t.Logf("After cleanup: %+v", stat)
		equal(t, 0, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.closedIdleCap)
		equal(t, int64(0), stat.closedLifetime)
		equal(t, int64(5), stat.closedIdleAge)
		noError(t, pool.closePool())
	})
}

func TestConnPoolCleanupMaxLife(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(5, 10, 2*time.Second, time.Hour, createConn)
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
		stat := pool.stats()
		t.Logf("Before cleanup: %+v", stat)
		equal(t, 10, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(0), stat.closedIdleCap)
		equal(t, int64(0), stat.closedLifetime)
		equal(t, int64(0), stat.closedIdleAge)

		time.Sleep(3 * time.Second)
		equal(t, 0, int(pool.open.Load()))
		equal(t, 0, pool.stats().IdleConns)
		isFalse(t, pool.closed.Load())

		stat = pool.stats()
		t.Logf("After cleanup: %+v", stat)
		equal(t, 0, stat.TotalConns)
		equal(t, 0, stat.IdleConns)
		equal(t, 10, stat.MaxOpen)
		equal(t, 5, stat.MaxIdle)
		equal(t, int64(5), stat.closedIdleCap)
		equal(t, int64(5), stat.closedLifetime)
		equal(t, int64(0), stat.closedIdleAge)
		noError(t, pool.closePool())
	})
}

func TestConnectionCleanerRunLockedBothLimits(t *testing.T) {
	pool := newConnPool(10, 10, time.Hour, time.Hour, createConn)
	fixedNow := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pool.now = func() time.Time { return fixedNow }

	idleExpired := newMockConn()
	idleExpired.createdAt = fixedNow.Add(-30 * time.Minute)
	idleExpired.returnedAt = fixedNow.Add(-2 * time.Hour)

	lifeExpired := newMockConn()
	lifeExpired.createdAt = fixedNow.Add(-2 * time.Hour)
	lifeExpired.returnedAt = fixedNow.Add(-time.Minute)

	fresh := newMockConn()
	fresh.createdAt = fixedNow.Add(-time.Minute)
	fresh.returnedAt = fixedNow.Add(-time.Minute)

	pool.idle = append(pool.idle, idleExpired)
	pool.idle = append(pool.idle, lifeExpired)
	pool.idle = append(pool.idle, fresh)
	pool.open.Store(3)
	pool.maxIdleTime = time.Hour
	pool.maxLifetime = time.Hour

	d, closing := pool.connectionCleanerRunLocked(time.Hour)
	hasLen(t, closing, 2)
	equal(t, int64(1), pool.closedIdleAge)
	equal(t, int64(1), pool.closedLifetime)
	equal(t, 1, len(pool.idle))
	greater(t, d, time.Duration(0))

	kept := pool.idle[len(pool.idle)-1]
	pool.idle = pool.idle[:len(pool.idle)-1]
	same(t, fresh, kept)
}

func TestConnectionCleanerRunLockedPutAfterCleaner(t *testing.T) {
	pool := newConnPool(2, 3, time.Hour, time.Hour, createConn)
	t.Cleanup(func() { _ = pool.closePool() })
	now := time.Now()
	idle := &mockConn{createdAt: now, returnedAt: now}
	expired := &mockConn{createdAt: now, returnedAt: now.Add(-2 * time.Hour)}
	returned := &mockConn{createdAt: now, returnedAt: now}
	pool.idle = append(pool.idle, expired, idle)
	pool.open.Store(3)
	pool.maxIdleTime = time.Hour

	pool.mu.Lock()
	_, closing := pool.connectionCleanerRunLocked(time.Hour)
	pool.mu.Unlock()
	hasLen(t, closing, 1)

	noError(t, pool.put(returned))
	// maxIdle=2: idle survivor + returned, or returned replaces if full after cleaner
	equal(t, 2, len(pool.idle))
}

func TestConnPoolCleanupConcurrentGet(t *testing.T) {
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
	noError(t, pool.closePool())
	equal(t, 0, int(pool.open.Load()))
}

func TestMinIdleDoesNotRetainIdleExpired(t *testing.T) {
	var created atomic.Int32
	pool := newConnPool(2, 4, 0, time.Hour, func(context.Context) (wireConn, error) {
		created.Add(1)
		return newMockConn(), nil
	})
	t.Cleanup(func() { _ = pool.closePool() })
	pool.minIdle = 1
	fixed := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pool.now = func() time.Time { return fixed }

	old := newMockConn()
	old.createdAt = fixed.Add(-time.Minute)
	old.returnedAt = fixed.Add(-2 * time.Hour)
	pool.idle = append(pool.idle, old)
	pool.open.Store(1)

	_, closing := pool.connectionCleanerRunLocked(time.Hour)
	hasLen(t, closing, 1)
	equal(t, 0, len(pool.idle))
	for _, cn := range closing {
		_ = cn.Close()
		pool.open.Add(-1)
	}
	pool.ensureMinIdle(context.Background())
	equal(t, 1, len(pool.idle))
	equal(t, 1, int(pool.open.Load()))
	equal(t, int32(1), created.Load())
	fresh := pool.idle[len(pool.idle)-1]
	pool.idle = pool.idle[:len(pool.idle)-1]
	if fresh == old {
		t.Fatal("minIdle retained the idle-expired connection")
	}
}

func TestCleanerDoesNotExceedMaxOpenUnderGet(t *testing.T) {
	pool := newConnPool(2, 2, 0, 50*time.Millisecond, createConn)
	t.Cleanup(func() { _ = pool.closePool() })
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
		pool.open.Add(-1)
	}
	pool.ensureMinIdle(ctx)
	<-done
	lessOrEqual(t, int(pool.open.Load()), pool.maxOpen)
}

func TestConnPoolCloseCancelsCleanerDial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		pool := newConnPool(1, 2, 0, 0, func(ctx context.Context) (wireConn, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		pool.minIdle = 1

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Cleaner calls ensureMinIdle with Background; mergeDialContext still
			// binds the pool lifecycle so TimeoutNone dials abort on close.
			pool.ensureMinIdle(context.Background())
		}()
		<-started
		noError(t, pool.closePool())
		<-done
		equal(t, 0, int(pool.open.Load()))
	})
}
