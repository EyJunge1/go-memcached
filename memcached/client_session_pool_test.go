package memcached

import (
	"context"
	"sync"
	"testing"
)

func TestSessionPoolRunsConcurrentWorkersAndCloses(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	pool, err := client.NewSessionPool(ctx, []byte("worker"), 2)
	noError(t, err)
	equal(t, 2, pool.Size())

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			key := []byte{byte('a' + i)}
			errs <- pool.Do(ctx, func(session *Session) error {
				return session.Store(ctx, Item{Key: key, Value: key})
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		noError(t, err)
	}
	for i := range 8 {
		key := []byte{byte('a' + i)}
		equal(t, key, mustGet(t, client, ctx, key).Value)
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
