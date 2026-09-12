package memcached

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestConnPoolCloseWakesWaiters(t *testing.T) {
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
		noError(t, pool.closePool())

		select {
		case getErr := <-errCh:
			isErrorIs(t, getErr, errPoolClosed)
		case <-time.After(time.Second):
			t.Fatal("waiting get was not woken by close")
		}

		noError(t, pool.put(cn))
		equal(t, 0, int(pool.open.Load()))
		equal(t, 0, pool.stats().IdleConns)
	})
}

func TestConnPoolGetClosesConnectionCreatedDuringClose(t *testing.T) {
	creationStarted := make(chan struct{})
	releaseCreation := make(chan struct{})
	createdConn := &closeTrackingConn{mockConn: newMockConn()}
	pool := newConnPool(1, 1, time.Hour, time.Hour, func(context.Context) (wireConn, error) {
		close(creationStarted)
		<-releaseCreation
		return createdConn, nil
	})

	type getResult struct {
		conn wireConn
		err  error
	}
	resultCh := make(chan getResult, 1)
	go func() {
		conn, err := pool.get(context.Background())
		resultCh <- getResult{conn: conn, err: err}
	}()

	<-creationStarted
	noError(t, pool.closePool())
	close(releaseCreation)

	result := <-resultCh
	isNil(t, result.conn)
	isErrorIs(t, result.err, errPoolClosed)
	isTrue(t, createdConn.closed.Load())
	equal(t, 0, int(pool.open.Load()))
}

func TestConnPoolWaiterWakesAfterDiscard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dials atomic.Int32
		pool := newConnPool(1, 1, 0, 0, func(context.Context) (wireConn, error) {
			dials.Add(1)
			return newMockConn(), nil
		})
		cn, err := pool.get(context.Background())
		noError(t, err)
		equal(t, int32(1), dials.Load())

		started := make(chan struct{})
		got := make(chan error, 1)
		go func() {
			close(started)
			_, getErr := pool.get(context.Background())
			got <- getErr
		}()
		<-started
		time.Sleep(10 * time.Millisecond)
		noError(t, pool.discard(cn))
		noError(t, <-got)
		equal(t, int32(2), dials.Load())
		noError(t, pool.closePool())
	})
}

func TestConnPoolWaiterWakesWithNoIdleRetention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dials atomic.Int32
		pool := newConnPool(0, 1, 0, 0, func(context.Context) (wireConn, error) {
			dials.Add(1)
			return newMockConn(), nil
		})
		cn, err := pool.get(context.Background())
		noError(t, err)

		started := make(chan struct{})
		got := make(chan error, 1)
		go func() {
			close(started)
			_, getErr := pool.get(context.Background())
			got <- getErr
		}()
		<-started
		time.Sleep(10 * time.Millisecond)
		noError(t, pool.put(cn))
		noError(t, <-got)
		equal(t, int32(2), dials.Load())
		noError(t, pool.closePool())
	})
}

func TestWarmupRespectsMaxOpenUnderParallelGet(t *testing.T) {
	var peak atomic.Int32
	var open atomic.Int32
	pool := newConnPool(8, 2, 0, 0, func(context.Context) (wireConn, error) {
		n := open.Add(1)
		for {
			cur := peak.Load()
			if n <= cur || peak.CompareAndSwap(cur, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		open.Add(-1)
		return newMockConn(), nil
	})
	pool.minIdle = 8

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			cn, err := pool.get(context.Background())
			if err != nil {
				return
			}
			_ = pool.put(cn)
		})
	}
	pool.warmup(context.Background(), pool.minIdle)
	wg.Wait()
	lessOrEqual(t, int(peak.Load()), pool.maxOpen)
	lessOrEqual(t, int(pool.open.Load()), pool.maxOpen)
	noError(t, pool.closePool())
}

func TestWarmupStopsWhenPoolClosed(t *testing.T) {
	started := make(chan struct{})
	pool := newConnPool(4, 4, 0, 0, func(ctx context.Context) (wireConn, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.warmup(context.Background(), 4)
	}()
	<-started
	noError(t, pool.closePool())
	<-done
	equal(t, 0, int(pool.open.Load()))
}

func TestConcurrentPutAndCloseDrainsIdle(t *testing.T) {
	for range 1000 {
		pool := newConnPool(1, 1, 0, 0, createConn)
		cn := newMockConn()
		cn.setConnPool(pool)
		pool.open.Store(1)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = pool.put(cn)
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = pool.closePool()
		}()
		close(start)
		wg.Wait()

		equal(t, 0, int(pool.open.Load()))
		equal(t, 0, len(pool.idle))
	}
}
