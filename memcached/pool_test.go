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

func TestConnPoolNew(t *testing.T) {
	tests := []struct {
		name        string
		maxIdle     int
		maxOpen     int
		maxLifetime time.Duration
		maxIdleTime time.Duration
	}{
		{
			name:        "basic initialization",
			maxIdle:     5,
			maxOpen:     10,
			maxLifetime: time.Hour,
			maxIdleTime: 5 * time.Minute,
		},
		{
			name:        "zero max connections",
			maxIdle:     0,
			maxOpen:     0,
			maxLifetime: time.Hour,
			maxIdleTime: time.Minute,
		},
		{
			name:        "maxIdle equals maxOpen",
			maxIdle:     10,
			maxOpen:     10,
			maxLifetime: time.Hour,
			maxIdleTime: time.Minute,
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
			equal(t, 0, int(pool.open.Load()))
			isFalse(t, pool.closed.Load())
			notNil(t, pool.closedCh)
			isNil(t, pool.cleanerCh)
			equal(t, int64(0), pool.closedIdleCap)
			equal(t, int64(0), pool.closedIdleAge)
			equal(t, int64(0), pool.closedLifetime)
		})
	}
}

func TestConnPoolGetPut(t *testing.T) {
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

		equal(t, 5, int(pool.open.Load()))
		equal(t, 5, pool.stats().IdleConns)
		noError(t, pool.closePool())
	})
}

func TestConnPoolGetTimeoutCase1(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		createConn := func(ctx context.Context) (wireConn, error) {
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
		noError(t, pool.closePool())
	})
}

func TestConnPoolGetTimeoutCase2(t *testing.T) {
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
		noError(t, pool.closePool())
	})
}

func TestConnPoolGetOversize(t *testing.T) {
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

		equal(t, 5, int(pool.open.Load()))
		equal(t, 5, pool.stats().IdleConns)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		cn, err = pool.get(ctx)
		isNil(t, err)
		notNil(t, cn)
		noError(t, pool.closePool())
	})
}

func TestConnPoolCheckoutDropsExpired(t *testing.T) {
	pool := newConnPool(5, 10, 50*time.Millisecond, 0, createConn)
	old := newMockConn()
	old.createdAt = time.Now().Add(-time.Hour)
	pool.idle = append(pool.idle, old)
	pool.open.Add(1)

	got, err := pool.get(context.Background())
	noError(t, err)
	notNil(t, got)
	if got == old {
		t.Fatal("expired conn was reused")
	}
	noError(t, pool.closePool())
}

func TestConnPoolOpenReservedBeforeCreate(t *testing.T) {
	var creating atomic.Int32
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	pool := newConnPool(1, 1, 0, 0, func(context.Context) (wireConn, error) {
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
	noError(t, pool.closePool())
}

func TestConnPoolDiscard(t *testing.T) {
	pool := newConnPool(5, 10, 0, 0, func(context.Context) (wireConn, error) {
		return &closeTrackingConn{mockConn: newMockConn()}, nil
	})
	cn, err := pool.get(context.Background())
	noError(t, err)
	equal(t, 1, int(pool.open.Load()))

	noError(t, pool.discard(cn))
	equal(t, 0, int(pool.open.Load()))
	equal(t, 0, pool.stats().IdleConns)
	tracked, ok := cn.(*closeTrackingConn)
	isTrue(t, ok)
	isTrue(t, tracked.closed.Load())
}
