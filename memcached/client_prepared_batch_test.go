package memcached

import (
	"context"
	"fmt"
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

	onA, err := doGetOne(testClient(t, a.Addr()), context.Background(), []byte("k"), GetOpts{})
	noError(t, err)
	isFalse(t, onA.Found)
	equal(t, []byte("v"), mustGet(t, client, context.Background(), []byte("k")).Value)
}

func TestPreparedBatchRejectsInvalidPipelineAndUseAfterClose(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.Pipeline().Delete([]byte("bad key")).Prepare()
	isErrorIs(t, err, ErrInvalidKey)

	batch, err := client.Pipeline().Delete([]byte("k")).Prepare()
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
	equal(t, []byte("dynamic-value"), mustGet(t, client, ctx, []byte("a")).Value)
	equal(t, []byte("x"), mustGet(t, client, ctx, []byte("b")).Value)

	noError(t, batch.Run(ctx))
	equal(t, 0, len(mustGet(t, client, ctx, []byte("a")).Value))
	equal(t, []byte("default"), mustGet(t, client, ctx, []byte("b")).Value)
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
	req, resp := buildCasCommand([]byte("k"), []byte("v"), 0, 0, 123, false)
	defer releaseReqAndResp(req, resp)
	noError(t, patchStorageValueLength(req, 12345))
	equal(t, []byte("cas k 0 0 12345 123\r\n"), req.raw)
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
		pipeline.Store(Item{Key: fmt.Appendf(nil, "%c-key-%d", prefix, i), Value: []byte("v")})
	}
	batch, err := pipeline.Prepare()
	noError(b, err)
	b.Cleanup(func() { noError(b, batch.Close()) })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := batch.refreshRoutes(); err != nil {
			b.Fatal(err)
		}
	}
}
