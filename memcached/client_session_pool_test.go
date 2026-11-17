package memcached

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSessionPoolRunsConcurrentWorkersAndCloses(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	pool, err := client.NewSessionPool(ctx, []byte("worker"), 2)
	noError(t, err)
	equal(t, 2, pool.Size())

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range 8 {
		wg.Go(func() {
			key := []byte("abcdefgh"[i : i+1])
			errs[i] = pool.Do(ctx, func(session *Session) error {
				return session.Store(ctx, Item{Key: key, Value: key})
			})
		})
	}
	wg.Wait()
	for _, err := range errs {
		noError(t, err)
	}
	for i := range 8 {
		key := []byte("abcdefgh"[i : i+1])
		equal(t, key, mustGet(ctx, t, client, key).Value)
	}

	noError(t, pool.Close())
	isErrorIs(t, pool.Do(ctx, func(*Session) error { return nil }), ErrClosed)
}

func TestSessionPoolValidatesArguments(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.NewSessionPool(context.Background(), []byte("k"), 0)
	isErrorIs(t, err, ErrInvalidArgument)

	pool, err := client.NewSessionPool(context.Background(), []byte("k"), 1)
	noError(t, err)
	t.Cleanup(func() { noError(t, pool.Close()) })
	isErrorIs(t, pool.Do(context.Background(), nil), ErrInvalidArgument)
}

func TestNewSessionPoolRejectsSizeAboveMaxOpen(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.MaxOpen = 1
		cfg.Pool.MaxIdle = 1
	})
	_, err := client.NewSessionPool(context.Background(), []byte("k"), 2)
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestSessionPoolDropWaiter(t *testing.T) {
	p := &SessionPool{
		waiters: []chan struct{}{
			make(chan struct{}, 1),
			make(chan struct{}, 1),
			make(chan struct{}, 1),
		},
	}
	target := p.waiters[1]
	p.dropWaiter(target)
	hasLen(t, p.waiters, 2)
	p.dropWaiter(make(chan struct{}, 1)) // unknown waiter: no-op
	hasLen(t, p.waiters, 2)
	p.dropWaiter(p.waiters[0])
	p.dropWaiter(p.waiters[0])
	hasLen(t, p.waiters, 0)
}

func TestSessionPoolDoContextCanceledWhileWaiting(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	pool, err := client.NewSessionPool(context.Background(), []byte("k"), 1)
	noError(t, err)
	t.Cleanup(func() { noError(t, pool.Close()) })

	block := make(chan struct{})
	doneHold := make(chan error, 1)
	go func() {
		doneHold <- pool.Do(context.Background(), func(*Session) error {
			<-block
			return nil
		})
	}()
	// Ensure the sole session is leased before waiting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		pool.mu.Lock()
		nIdle, nWait := len(pool.idle), len(pool.waiters)
		pool.mu.Unlock()
		if nIdle == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session never leased")
		}
		time.Sleep(time.Millisecond)
		_ = nWait
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	isErrorIs(t, pool.Do(ctx, func(*Session) error { return nil }), context.DeadlineExceeded)

	close(block)
	noError(t, <-doneHold)
}
