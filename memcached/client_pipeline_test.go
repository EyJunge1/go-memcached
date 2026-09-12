package memcached

import (
	"context"
	"testing"
	"time"
)

func TestPipelineTouch(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))

	err := client.Pipeline().Touch([]byte("k"), Rel(time.Hour), MutateOpts{}).Run(ctx)
	noError(t, err)
	waitLastCmdContains(t, srv, "touch k 3600")
	equal(t, []byte("v"), mustGet(ctx, t, client, []byte("k")).Value)

	err = client.Pipeline().Touch([]byte("a\r\nb"), Rel(time.Second), MutateOpts{}).Run(ctx)
	isErrorIs(t, err, ErrInvalidKey)
}

func TestPipelineStoreCAS(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("v"))
	items, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	cas := items[0].CAS
	notZero(t, cas)

	err = client.Pipeline().
		Store(Item{Mode: StoreCAS, Key: []byte("k"), Value: []byte("v2"), CAS: cas}).
		Run(ctx)
	noError(t, err)
	equal(t, []byte("v2"), mustGet(ctx, t, client, []byte("k")).Value)

	err = client.Pipeline().
		Store(Item{Mode: StoreCAS, Key: []byte("k"), Value: []byte("nope"), CAS: cas + 99}).
		Run(ctx)
	isErrorIs(t, err, ErrExists)
}

func TestPipelineStoreThenGetOneResults(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	var (
		stored  PipelineResult
		fetched PipelineResult
		got     GetResult
	)
	got.Value = make([]byte, 0, 16)

	err := client.Pipeline().
		Store(Item{Key: []byte("k"), Value: []byte("v")}, &stored).
		GetOne([]byte("k"), &got, GetOpts{}, &fetched).
		Run(ctx)
	noError(t, err)
	equal(t, PipelineDone, stored.State)
	noError(t, stored.Err)
	equal(t, PipelineDone, fetched.State)
	noError(t, fetched.Err)
	isTrue(t, got.Found)
	equal(t, []byte("v"), got.Value)
}

func TestPipelineGetRunsAfterStoreOutcome(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("k"), []byte("old"))
	var (
		stored  PipelineResult
		fetched PipelineResult
		got     GetResult
	)

	err := client.Pipeline().
		Store(Item{Mode: StoreAdd, Key: []byte("k"), Value: []byte("new")}, &stored).
		GetOne([]byte("k"), &got, GetOpts{}, &fetched).
		Run(ctx)
	isErrorIs(t, err, ErrNotStored)
	equal(t, PipelineDone, stored.State)
	isErrorIs(t, stored.Err, ErrNotStored)
	equal(t, PipelineDone, fetched.State)
	noError(t, fetched.Err)
	isTrue(t, got.Found)
	equal(t, []byte("old"), got.Value)
}

func TestPipelineGetMany(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("a"), []byte("1"))
	mustStore(ctx, t, client, []byte("b"), []byte("2"))
	keys := [][]byte{[]byte("a"), []byte("missing"), []byte("b")}
	dst := make([]GetResult, len(keys))
	results := make([]PipelineResult, len(keys))

	noError(t, client.Pipeline().Get(keys, dst, GetOpts{}, results).Run(ctx))
	equal(t, "get a missing b", lastCmdOf(srv))
	for i := range results {
		equal(t, PipelineDone, results[i].State)
		noError(t, results[i].Err)
	}
	isTrue(t, dst[0].Found)
	isFalse(t, dst[1].Found)
	isTrue(t, dst[2].Found)
	equal(t, []byte("1"), dst[0].Value)
	equal(t, []byte("2"), dst[2].Value)
}

func TestPipelineArithmetic(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("n"), []byte("10"))
	var (
		increased uint64
		decreased uint64
		incr      PipelineResult
		decr      PipelineResult
	)

	err := client.Pipeline().
		Incr([]byte("n"), 5, &increased, ArithmeticOpts{}, &incr).
		Decr([]byte("n"), 3, &decreased, ArithmeticOpts{}, &decr).
		Run(ctx)
	noError(t, err)
	equal(t, uint64(15), increased)
	equal(t, uint64(12), decreased)
	equal(t, PipelineDone, incr.State)
	equal(t, PipelineDone, decr.State)
}

func TestPipelineOwnsQueuedKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	key := []byte("original")
	p := client.Pipeline().Store(Item{Key: key, Value: []byte("v")})
	copy(key, "mutated!")

	noError(t, p.Run(ctx))
	equal(t, []byte("v"), mustGet(ctx, t, client, []byte("original")).Value)
}

func TestPipelineBorrowsQueuedValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	value := []byte("before")
	p := client.Pipeline().Store(Item{Key: []byte("k"), Value: value})
	copy(value, "after!")

	noError(t, p.Run(ctx))
	equal(t, []byte("after!"), mustGet(ctx, t, client, []byte("k")).Value)
}

func TestPipelineRejectsAliasedDestinations(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	mustStore(ctx, t, client, []byte("a-n"), []byte("10"))
	mustStore(ctx, t, client, []byte("b-n"), []byte("20"))

	var (
		got       GetResult
		counter   uint64
		first     PipelineResult
		second    PipelineResult
		duplicate PipelineResult
	)
	err := client.Pipeline().
		GetOne([]byte("a-n"), &got, GetOpts{}, &first).
		GetOne([]byte("b-n"), &got, GetOpts{}, &second).
		Incr([]byte("a-n"), 1, &counter, ArithmeticOpts{}).
		Incr([]byte("b-n"), 1, &counter, ArithmeticOpts{}).
		Store(Item{Key: []byte("a-k"), Value: []byte("v")}, &duplicate).
		Store(Item{Key: []byte("b-k"), Value: []byte("v")}, &duplicate).
		Run(ctx)
	isErrorIs(t, err, ErrInvalidArgument)
	equal(t, PipelineDone, first.State)
	equal(t, PipelineInvalid, second.State)
	equal(t, PipelineDone, duplicate.State)
}

func TestPipelineGetLengthErrorInvalidatesResults(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	results := []PipelineResult{
		{State: PipelineDone},
		{State: PipelineDone},
	}
	err := client.Pipeline().
		Get([][]byte{[]byte("a")}, nil, GetOpts{}, results).
		Run(context.Background())
	isErrorIs(t, err, ErrInvalidArgument)
	for i := range results {
		equal(t, PipelineInvalid, results[i].State)
		isErrorIs(t, results[i].Err, ErrInvalidArgument)
	}

	results = results[:1]
	results[0] = PipelineResult{State: PipelineDone}
	err = client.Pipeline().
		Get([][]byte{[]byte("a"), []byte("b")}, make([]GetResult, 2), GetOpts{}, results).
		Run(context.Background())
	isErrorIs(t, err, ErrInvalidArgument)
	equal(t, PipelineInvalid, results[0].State)
	isErrorIs(t, results[0].Err, ErrInvalidArgument)
}

func TestPipelineRejectsQuietAndBadKey(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v"), Quiet: true}).
		Store(Item{Mode: StoreSet, Key: []byte("a\r\nb"), Value: []byte("v")}).
		Store(Item{Mode: StoreMode(99), Key: []byte("x"), Value: []byte("v")}).
		Store(Item{Mode: StoreSet, Key: []byte("ok"), Value: []byte("v")}).
		Delete([]byte("bad key"), MutateOpts{}).
		Run(ctx)
	isErrorIs(t, err, ErrInvalidArgument)
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, []byte("v"), mustGet(ctx, t, client, []byte("ok")).Value)
}

func TestPipelineValidationResult(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	var result PipelineResult
	err := client.Pipeline().
		Store(Item{Key: []byte("bad key"), Value: []byte("v")}, &result).
		Run(context.Background())
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, PipelineInvalid, result.State)
	isErrorIs(t, result.Err, ErrInvalidKey)
}

func TestPipelineEmptyRunJoinsQueueErrors(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("a\r\nb"), Value: []byte("v")}).
		Run(context.Background())
	isErrorIs(t, err, ErrInvalidKey)
}

func TestPipelineReusableAfterRun(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	p := client.Pipeline()
	noError(t, p.Store(Item{Key: []byte("a"), Value: []byte("1")}).Run(ctx))
	equal(t, 0, len(p.ops))
	greater(t, cap(p.ops), 0)
	noError(t, p.Store(Item{Key: []byte("b"), Value: []byte("2")}).Run(ctx))
	equal(t, []byte("1"), mustGet(ctx, t, client, []byte("a")).Value)
	equal(t, []byte("2"), mustGet(ctx, t, client, []byte("b")).Value)
}
