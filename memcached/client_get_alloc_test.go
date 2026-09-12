package memcached

import (
	"context"
	"testing"
)

type hotPathAllocEnv struct {
	client    *Client
	ctx       context.Context
	key       []byte
	val       []byte
	keys      [][]byte
	reused    []GetResult
	multiKeys [][]byte
	multiDst  []GetResult
	meta      *MetaClient
	mg        MetaGet
	metaSet   MetaResult
	metaDst   MetaResult
	pipeKeys  [][]byte
	pipeline  *Pipeline
	reused4k  []GetResult
}

func startHotPathAllocEnv(t *testing.T) hotPathAllocEnv {
	t.Helper()
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	key := []byte("alloc-key")
	val := []byte("test_value")
	mustStore(ctx, t, client, key, val)
	keys := [][]byte{key}
	reused := []GetResult{{Value: make([]byte, 0, 64)}}
	noError(t, client.Get(ctx, keys, reused, GetOpts{}))

	multiKeys := make([][]byte, 8)
	multiDst := make([]GetResult, 8)
	for i := range multiKeys {
		multiKeys[i] = []byte{byte('m'), byte('0' + i)}
		mustStore(ctx, t, client, multiKeys[i], val)
		multiDst[i].Value = make([]byte, 0, 64)
	}
	noError(t, client.Get(ctx, multiKeys, multiDst, GetOpts{}))

	meta := client.Meta()
	mg := MetaGet{Return: MetaGetReturn{Value: true}}
	var metaSet MetaResult
	noError(t, meta.Set(ctx, key, val, &metaSet, MetaSet{}))
	var metaDst MetaResult
	metaDst.Item.Value = make([]byte, 0, 64)
	noError(t, meta.Get(ctx, key, &metaDst, mg))

	pipeKeys := make([][]byte, 8)
	for i := range pipeKeys {
		pipeKeys[i] = []byte{byte('p'), byte('0' + i)}
	}
	p := client.Pipeline()
	for _, k := range pipeKeys {
		p.Store(Item{Key: k, Value: val})
	}
	noError(t, p.Run(ctx))

	large := make([]byte, 4096)
	for i := range large {
		large[i] = 'x'
	}
	mustStore(ctx, t, client, key, large)
	reused4k := []GetResult{{Value: make([]byte, 0, 4096)}}
	noError(t, client.Get(ctx, keys, reused4k, GetOpts{}))

	return hotPathAllocEnv{
		client: client, ctx: ctx, key: key, val: val, keys: keys, reused: reused,
		multiKeys: multiKeys, multiDst: multiDst,
		meta: meta, mg: mg, metaSet: metaSet, metaDst: metaDst,
		pipeKeys: pipeKeys, pipeline: p, reused4k: reused4k,
	}
}

func TestGetHotPathAllocs(t *testing.T) {
	if _raceEnabled {
		t.Skip("alloc counts are not meaningful under -race")
	}
	env := startHotPathAllocEnv(t)
	getReuseAllocs := testing.AllocsPerRun(200, func() {
		if err := env.client.Get(env.ctx, env.keys, env.reused, GetOpts{}); err != nil || !env.reused[0].Found {
			t.Fatalf("get reused: %v found=%v", err, env.reused[0].Found)
		}
	})
	if getReuseAllocs > 8 {
		t.Fatalf("Get reused allocs/op = %.1f, want <= 8 (includes in-process fake server)", getReuseAllocs)
	}

	getOnceAllocs := testing.AllocsPerRun(200, func() {
		dst := make([]GetResult, 1)
		if err := env.client.Get(env.ctx, env.keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			t.Fatalf("get one-shot: %v", err)
		}
	})
	if getOnceAllocs > 12 {
		t.Fatalf("Get one-shot allocs/op = %.1f, want <= 12 (includes in-process fake server)", getOnceAllocs)
	}

	get4kReuse := testing.AllocsPerRun(100, func() {
		if err := env.client.Get(env.ctx, env.keys, env.reused4k, GetOpts{}); err != nil || !env.reused4k[0].Found {
			t.Fatalf("get 4k reused: %v", err)
		}
	})
	get4kOnce := testing.AllocsPerRun(100, func() {
		dst := []GetResult{{Value: make([]byte, 0, 4096)}}
		if err := env.client.Get(env.ctx, env.keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			t.Fatalf("get 4k one-shot: %v", err)
		}
	})
	t.Logf("Get allocs/op: reused=%.1f one-shot=%.1f 4k reused=%.1f 4k one-shot=%.1f",
		getReuseAllocs, getOnceAllocs, get4kReuse, get4kOnce)
}

func TestStoreHotPathAllocs(t *testing.T) {
	env := startHotPathAllocEnv(t)
	storeAllocs := testing.AllocsPerRun(200, func() {
		if err := env.client.Store(env.ctx, Item{Key: env.key, Value: env.val}); err != nil {
			t.Fatalf("store: %v", err)
		}
	})
	if storeAllocs > 12 {
		t.Fatalf("Store allocs/op = %.1f, want <= 12 (includes in-process fake server)", storeAllocs)
	}
	t.Logf("Store allocs/op = %.1f", storeAllocs)
}

func TestMetaHotPathAllocs(t *testing.T) {
	env := startHotPathAllocEnv(t)
	metaAllocs := testing.AllocsPerRun(200, func() {
		if err := env.meta.Set(env.ctx, env.key, env.val, &env.metaSet, MetaSet{}); err != nil {
			t.Fatalf("meta set: %v", err)
		}
		if err := env.meta.Get(env.ctx, env.key, &env.metaDst, env.mg); err != nil || env.metaDst.Err() != nil {
			t.Fatalf("meta get: %v %v", err, env.metaDst.Err())
		}
	})
	if metaAllocs > 24 {
		t.Fatalf("Meta Set+Get allocs/op = %.1f, want <= 24 (includes in-process fake server)", metaAllocs)
	}
	metaGetOnly := testing.AllocsPerRun(200, func() {
		if err := env.meta.Get(env.ctx, env.key, &env.metaDst, env.mg); err != nil || env.metaDst.Err() != nil {
			t.Fatalf("meta get: %v %v", err, env.metaDst.Err())
		}
	})
	t.Logf("Meta allocs/op: Set+Get=%.1f Get reused=%.1f", metaAllocs, metaGetOnly)
}

func TestMultiGetHotPathAllocs(t *testing.T) {
	env := startHotPathAllocEnv(t)
	multiAllocs := testing.AllocsPerRun(100, func() {
		if err := env.client.Get(env.ctx, env.multiKeys, env.multiDst, GetOpts{}); err != nil {
			t.Fatalf("multiget: %v", err)
		}
		for i := range env.multiDst {
			if !env.multiDst[i].Found {
				t.Fatalf("multiget miss at %d", i)
			}
		}
	})
	if multiAllocs > 40 {
		t.Fatalf("MultiGet8 reused allocs/op = %.1f, want <= 40 (includes in-process fake server)", multiAllocs)
	}
	multiOnce := testing.AllocsPerRun(100, func() {
		dst := make([]GetResult, 8)
		if err := env.client.Get(env.ctx, env.multiKeys, dst, GetOpts{}); err != nil {
			t.Fatalf("multiget one-shot: %v", err)
		}
	})
	t.Logf("MultiGet8 allocs/op: reused=%.1f one-shot=%.1f", multiAllocs, multiOnce)
}

func TestPipelineHotPathAllocs(t *testing.T) {
	env := startHotPathAllocEnv(t)
	pipeAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range env.pipeKeys {
			env.pipeline.Store(Item{Key: k, Value: env.val})
		}
		if err := env.pipeline.Run(env.ctx); err != nil {
			t.Fatalf("pipeline: %v", err)
		}
	})
	if pipeAllocs > 96 {
		t.Fatalf("Pipeline Store x8 allocs/op = %.1f, want <= 96 (includes in-process fake server)", pipeAllocs)
	}
	t.Logf("Pipeline8 allocs/op = %.1f", pipeAllocs)
}

func BenchmarkClientGetSmall(b *testing.B) {
	srv := startFakeMemcached(b)
	client := testClient(b, srv.Addr())
	ctx := context.Background()
	key := []byte("alloc-key")
	mustStore(ctx, b, client, key, []byte("test_value"))
	keys := [][]byte{key}
	dst := []GetResult{{Value: make([]byte, 0, 64)}}
	if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := client.Get(ctx, keys, dst, GetOpts{}); err != nil || !dst[0].Found {
			b.Fatal(err)
		}
	}
}
