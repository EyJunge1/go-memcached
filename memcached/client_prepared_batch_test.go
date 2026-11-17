package memcached

import (
	"context"
	"strconv"
	"testing"
)

func TestPreparedBatchReusesOwnedFramesAndDestinations(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	value := []byte("owned")
	var (
		stored PipelineResult
		got    GetResult
		read   PipelineResult
	)
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: value}, &stored).
		GetOne([]byte("k"), &got, GetOpts{}, &read).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	copy(value, "xxxxx")

	for range 2 {
		noError(t, batch.Run(ctx))
		equal(t, PipelineDone, stored.State)
		equal(t, PipelineDone, read.State)
		isTrue(t, got.Found)
		equal(t, []byte("owned"), got.Value)
	}
}

func TestPreparedBatchReroutesAfterSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("v")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })

	servers, err := ParseServers(b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	noError(t, batch.Run(context.Background()))

	onA, err := doGetOne(context.Background(), testClient(t, a.Addr()), []byte("k"), GetOpts{})
	noError(t, err)
	isFalse(t, onA.Found)
	equal(t, []byte("v"), mustGet(context.Background(), t, client, []byte("k")).Value)
}

func TestPreparedBatchRejectsInvalidPipelineAndUseAfterClose(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.Pipeline().Delete([]byte("bad key"), MutateOpts{}).Prepare()
	isErrorIs(t, err, ErrInvalidKey)

	batch, err := client.Pipeline().Delete([]byte("k"), MutateOpts{}).Prepare()
	noError(t, err)
	noError(t, batch.Close())
	isErrorIs(t, batch.Run(context.Background()), ErrClosed)
}

func TestPreparedBatchRunValuesBorrowsVariableLengthValues(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: nil}).
		Store(Item{Key: []byte("b"), Value: []byte("default")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })

	noError(t, batch.RunValues(ctx, [][]byte{[]byte("dynamic-value"), []byte("x")}))
	equal(t, []byte("dynamic-value"), mustGet(ctx, t, client, []byte("a")).Value)
	equal(t, []byte("x"), mustGet(ctx, t, client, []byte("b")).Value)

	noError(t, batch.Run(ctx))
	equal(t, 0, len(mustGet(ctx, t, client, []byte("a")).Value))
	equal(t, []byte("default"), mustGet(ctx, t, client, []byte("b")).Value)
}

func TestPreparedBatchRunValuesValidatesSlots(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.MaxValueBytes = 4 })
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("v")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })

	isErrorIs(t, batch.RunValues(context.Background(), nil), ErrInvalidArgument)
	isErrorIs(t, batch.RunValues(context.Background(), [][]byte{[]byte("large")}), ErrInvalidValue)
}

func TestPatchStorageValueLengthCAS(t *testing.T) {
	req, resp, err := buildCASCommand([]byte("k"), []byte("v"), 0, 0, 123, nil, false)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	noError(t, patchStorageValueLength(req, 12345))
	equal(t, []byte("cas k 0 0 12345 123\r\n"), req.raw)
}

func TestPreparedBatchMultiNodeFanout(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("a-1"), Value: []byte("1")}).
		Store(Item{Key: []byte("b-1"), Value: []byte("2")}).
		Store(Item{Key: []byte("a-2"), Value: []byte("3")}).
		Store(Item{Key: []byte("b-2"), Value: []byte("4")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	noError(t, batch.Run(ctx))

	equal(t, []byte("1"), mustGet(ctx, t, client, []byte("a-1")).Value)
	equal(t, []byte("2"), mustGet(ctx, t, client, []byte("b-1")).Value)
	equal(t, []byte("3"), mustGet(ctx, t, client, []byte("a-2")).Value)
	equal(t, []byte("4"), mustGet(ctx, t, client, []byte("b-2")).Value)
}

func TestPreparedBatchSharedStreamRun(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Pool.SharedStreams = 1
	})
	ctx := context.Background()
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("ss-a"), Value: []byte("1")}).
		Store(Item{Key: []byte("ss-b"), Value: []byte("2")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	noError(t, batch.Run(ctx))
	equal(t, []byte("1"), mustGet(ctx, t, client, []byte("ss-a")).Value)
	equal(t, []byte("2"), mustGet(ctx, t, client, []byte("ss-b")).Value)
}

func TestPreparedBatchRunValuesRealloc(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	batch, err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("x")}).
		Prepare()
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	noError(t, batch.RunValues(ctx, [][]byte{long}))
	equal(t, long, mustGet(ctx, t, client, []byte("k")).Value)
}

func BenchmarkPreparedBatchRefreshRoutes(b *testing.B) {
	a := startFakeMemcached(b)
	z := startFakeMemcached(b)
	client := testClient(b, a.Addr()+","+z.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	pipeline := client.Pipeline()
	for i := range 64 {
		prefix := byte('a')
		if i&1 != 0 {
			prefix = 'b'
		}
		key := append([]byte{prefix}, "-key-"...)
		key = strconv.AppendInt(key, int64(i), 10)
		pipeline.Store(Item{Key: key, Value: []byte("v")})
	}
	batch, err := pipeline.Prepare()
	noError(b, err)
	if batch == nil {
		b.Fatal("nil prepared batch")
	}
	b.Cleanup(func() { noError(b, batch.Close()) })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.refreshRoutes(); err != nil {
			b.Fatal(err)
		}
	}
}
