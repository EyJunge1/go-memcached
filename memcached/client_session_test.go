package memcached

import (
	"context"
	"testing"
	"time"
)

func TestSessionReusesHeldConnection(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	session, err := client.Session(ctx, []byte("k"))
	noError(t, err)

	noError(t, session.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}))
	var got GetResult
	noError(t, session.GetOne(ctx, []byte("k"), &got, GetOpts{}))
	equal(t, []byte("v"), got.Value)
	stats := client.PoolStats()
	hasLen(t, stats, 1)
	equal(t, 0, stats[0].Idle)

	noError(t, session.Close())
	stats = client.PoolStats()
	hasLen(t, stats, 1)
	equal(t, 1, stats[0].Idle)
	isErrorIs(t, session.GetOne(ctx, []byte("k"), &got, GetOpts{}), ErrClosed)
}

func TestSessionRejectsKeyForAnotherNode(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	session, err := client.Session(context.Background(), []byte("a-key"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })

	err = session.Store(context.Background(), Item{Key: []byte("b-key"), Value: []byte("v")})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestSessionRunsPreparedBatch(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	var got GetResult
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("v")}).
		GetOne([]byte("k"), &got, GetOpts{}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	session, err := client.Session(ctx, []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })

	for range 2 {
		noError(t, session.RunPrepared(ctx, batch))
		equal(t, []byte("v"), got.Value)
	}
}

func TestSessionRunsPreparedBatchDynamicValues(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	key := []byte("dynamic")
	batch, err := client.Pipeline().
		Store(Item{Key: key, Value: []byte("default")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	session, err := client.Session(ctx, key)
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })

	noError(t, session.RunPreparedValues(ctx, batch, [][]byte{[]byte("much-longer-dynamic")}))
	equal(t, []byte("much-longer-dynamic"), mustGet(ctx, t, client, key).Value)
	noError(t, session.RunPrepared(ctx, batch))
	equal(t, []byte("default"), mustGet(ctx, t, client, key).Value)
}

func TestSessionDirectConvenienceMethods(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	session, err := client.Session(ctx, []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })

	noError(t, session.Store(ctx, Item{Key: []byte("k"), Value: []byte("v")}))
	var got GetResult
	noError(t, session.GetOne(ctx, []byte("k"), &got, GetOpts{ReturnCAS: true}))
	isTrue(t, got.Found)
	equal(t, []byte("v"), got.Value)
	noError(t, session.Touch(ctx, []byte("k"), Rel(time.Minute), MutateOpts{}))
	noError(t, session.Delete(ctx, []byte("k"), MutateOpts{}))
	isErrorIs(t, session.Delete(ctx, []byte("k"), MutateOpts{}), ErrNotFound)

	noError(t, session.Store(ctx, Item{Key: []byte("n"), Value: []byte("10")}))
	var value uint64
	noError(t, session.Incr(ctx, []byte("n"), 5, &value, ArithmeticOpts{}))
	equal(t, uint64(15), value)
	noError(t, session.Decr(ctx, []byte("n"), 3, &value, ArithmeticOpts{}))
	equal(t, uint64(12), value)
	isErrorIs(t, session.Store(ctx, Item{Key: []byte("k"), Quiet: true}), ErrInvalidArgument)
}

func TestSessionRejectsClusterChange(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	t.Cleanup(func() { noError(t, session.Close()) })
	servers, err := ParseServers(b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))

	err = session.Store(context.Background(), Item{Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestNilSessionReturnsClosed(t *testing.T) {
	var session *Session
	client := testClient(t, "127.0.0.1:11211")
	isErrorIs(t, session.Run(context.Background(), client.Pipeline()), ErrClosed)
	isErrorIs(t, session.RunPrepared(context.Background(), nil), ErrClosed)
}
