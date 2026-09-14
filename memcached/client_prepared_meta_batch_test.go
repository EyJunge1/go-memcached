package memcached

import (
	"context"
	"fmt"
	"testing"
)

func TestPreparedMetaBatchReusesOwnedWireAndDestinations(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	key := []byte("k")
	value := []byte("owned")
	ops := []MetaOp{
		{Key: key, Body: value, Set: &MetaSet{}},
		{Key: key, Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
	}
	dst := make([]MetaResult, len(ops))
	batch, err := client.Meta().PreparePipeline(ops, dst)
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	copy(key, "x")
	copy(value, "xxxxx")

	for range 2 {
		noError(t, batch.Run(ctx))
		equal(t, MetaStatusHD, dst[0].Status)
		equal(t, MetaStatusVA, dst[1].Status)
		equal(t, []byte("owned"), dst[1].Item.Value)
	}
}

func TestPreparedMetaBatchValidatesAndCloses(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.Meta().PreparePipeline(
		[]MetaOp{{Key: []byte("k"), Get: &MetaGet{}}},
		nil,
	)
	isErrorIs(t, err, ErrInvalidArgument)

	dst := make([]MetaResult, 1)
	batch, err := client.Meta().PreparePipeline(
		[]MetaOp{{Key: []byte("k"), Get: &MetaGet{}}},
		dst,
	)
	noError(t, err)
	noError(t, batch.Close())
	isErrorIs(t, batch.Run(context.Background()), ErrClosed)

	opaque := []byte("same")
	_, err = client.Meta().PreparePipeline([]MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Opaque: opaque}},
		{Key: []byte("b"), Get: &MetaGet{Opaque: opaque}},
	}, make([]MetaResult, 2))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestPreparedMetaBatchReroutesAfterSetServers(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })
	dst := make([]MetaResult, 1)
	batch, err := client.Meta().PreparePipeline([]MetaOp{{
		Key:  []byte("k"),
		Body: []byte("v"),
		Set:  &MetaSet{},
	}}, dst)
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })

	servers, err := ParseServers(b.Addr())
	noError(t, err)
	noError(t, client.SetServers(servers))
	noError(t, batch.Run(context.Background()))
	equal(t, []byte("v"), mustGet(t, client, context.Background(), []byte("k")).Value)
}

func TestPreparedMetaBatchSupportsLargeBatches(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	const count = 4096
	ops := make([]MetaOp, count)
	dst := make([]MetaResult, count)
	for i := range ops {
		ops[i] = MetaOp{Key: fmt.Appendf(nil, "missing-%d", i), Get: &MetaGet{}}
	}
	batch, err := client.Meta().PreparePipeline(ops, dst)
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	noError(t, batch.Run(context.Background()))
	for i := range dst {
		equal(t, MetaStatusEN, dst[i].Status)
	}
}

func TestPreparedMetaBatchRunValuesAndPersistentIndex(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	ops := make([]MetaOp, metaPipelineIndexThreshold)
	ops[0] = MetaOp{Key: []byte("a"), Body: []byte("default-a"), Set: &MetaSet{}}
	ops[1] = MetaOp{Key: []byte("b"), Body: nil, Set: &MetaSet{}}
	for i := 2; i < len(ops); i++ {
		ops[i] = MetaOp{Key: fmt.Appendf(nil, "missing-index-%d", i), Get: &MetaGet{}}
	}
	dst := make([]MetaResult, len(ops))
	batch, err := client.Meta().PreparePipeline(ops, dst)
	noError(t, err)
	t.Cleanup(func() { noError(t, batch.Close()) })
	index := batch.groups[0].index
	isTrue(t, index != nil)

	noError(t, batch.RunValues(ctx, [][]byte{[]byte("x"), []byte("dynamic-b-long")}))
	equal(t, []byte("x"), mustGet(t, client, ctx, []byte("a")).Value)
	equal(t, []byte("dynamic-b-long"), mustGet(t, client, ctx, []byte("b")).Value)
	same(t, index, batch.groups[0].index)

	noError(t, batch.Run(ctx))
	equal(t, []byte("default-a"), mustGet(t, client, ctx, []byte("a")).Value)
	equal(t, 0, len(mustGet(t, client, ctx, []byte("b")).Value))
	same(t, index, batch.groups[0].index)
}

func BenchmarkPreparedMetaBatchRefreshRoutes(b *testing.B) {
	a := startFakeMemcached(b)
	z := startFakeMemcached(b)
	client := testClient(b, a.Addr()+","+z.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ops := make([]MetaOp, 64)
	dst := make([]MetaResult, len(ops))
	for i := range ops {
		prefix := byte('a')
		if i&1 != 0 {
			prefix = 'b'
		}
		ops[i] = MetaOp{Key: fmt.Appendf(nil, "%c-key-%d", prefix, i), Get: &MetaGet{}}
	}
	batch, err := client.Meta().PreparePipeline(ops, dst)
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
