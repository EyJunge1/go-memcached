package memcached

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type failPipelineWriteConn struct{ net.Conn }

func (c *failPipelineWriteConn) Write([]byte) (int, error) {
	return 0, errors.New("forced pipeline write failure")
}

func TestPipelineTouch(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))

	err := client.Pipeline().Touch([]byte("k"), Rel(time.Hour)).Run(ctx)
	noError(t, err)
	waitLastCmdContains(t, srv, "touch k 3600")
	equal(t, []byte("v"), mustGet(t, client, ctx, []byte("k")).Value)

	err = client.Pipeline().Touch([]byte("a\r\nb"), Rel(time.Second)).Run(ctx)
	isErrorIs(t, err, ErrInvalidKey)
}

func TestPipelineStoreCAS(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))
	items, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{ReturnCAS: true})
	noError(t, err)
	hasLen(t, items, 1)
	cas := items[0].CAS
	notZero(t, cas)

	err = client.Pipeline().
		Store(Item{Mode: StoreCAS, Key: []byte("k"), Value: []byte("v2"), CAS: cas}).
		Run(ctx)
	noError(t, err)
	equal(t, []byte("v2"), mustGet(t, client, ctx, []byte("k")).Value)

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
	mustStore(t, client, ctx, []byte("k"), []byte("old"))
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
	mustStore(t, client, ctx, []byte("a"), []byte("1"))
	mustStore(t, client, ctx, []byte("b"), []byte("2"))
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
	mustStore(t, client, ctx, []byte("n"), []byte("10"))
	var (
		increased uint64
		decreased uint64
		incr      PipelineResult
		decr      PipelineResult
	)

	err := client.Pipeline().
		Incr([]byte("n"), 5, &increased, &incr).
		Decr([]byte("n"), 3, &decreased, &decr).
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
	equal(t, []byte("v"), mustGet(t, client, ctx, []byte("original")).Value)
}

func TestPipelineBorrowsQueuedValue(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	value := []byte("before")
	p := client.Pipeline().Store(Item{Key: []byte("k"), Value: value})
	copy(value, "after!")

	noError(t, p.Run(ctx))
	equal(t, []byte("after!"), mustGet(t, client, ctx, []byte("k")).Value)
}

func TestPipelineRejectsAliasedDestinations(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.Cluster.pickBuilder = prefixPickBuilder{}
	})
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("a-n"), []byte("10"))
	mustStore(t, client, ctx, []byte("b-n"), []byte("20"))

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
		Incr([]byte("a-n"), 1, &counter).
		Incr([]byte("b-n"), 1, &counter).
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
		Delete([]byte("bad key")).
		Run(ctx)
	isErrorIs(t, err, ErrInvalidArgument)
	isErrorIs(t, err, ErrInvalidKey)
	equal(t, []byte("v"), mustGet(t, client, ctx, []byte("ok")).Value)
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

func TestPipelineRunCancelled(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var result PipelineResult
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}, &result).
		Run(ctx)
	isErrorIs(t, err, context.Canceled)
	equal(t, PipelineNotExecuted, result.State)
	isErrorIs(t, result.Err, context.Canceled)
}

func TestPipelineUDPRejected(t *testing.T) {
	srv := startFakeMemcachedUDP(t)
	client := testClient(t, srv.Addr())
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Run(context.Background())
	isErrorIs(t, err, ErrNotSupported)
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
	equal(t, []byte("1"), mustGet(t, client, ctx, []byte("a")).Value)
	equal(t, []byte("2"), mustGet(t, client, ctx, []byte("b")).Value)
}

func TestPipelineRecvErrorDiscardsConn(t *testing.T) {
	addr, dials := startCloseAfterCommand(t, "set")
	client := testClient(t, addr)
	var first, second PipelineResult
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}, &first).
		Store(Item{Mode: StoreSet, Key: []byte("other"), Value: []byte("v")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrTransport)
	equal(t, PipelineUnknown, second.State)
	isErrorIs(t, second.Err, ErrTransport)
	ver, verErr := doVersion(client, context.Background())
	noError(t, verErr)
	equal(t, []byte("after-close"), ver)
	greater(t, int(dials.Load()), 1)
}

func TestPipelineFatalReplyChargesRemainingResults(t *testing.T) {
	var replies int
	addr, _ := startCmdServer(t, func(fields []string) string {
		if fields[0] != "set" {
			return "ERROR\r\n"
		}
		replies++
		if replies == 1 {
			return "SERVER_ERROR oom\r\n"
		}
		return "STORED\r\n"
	})
	client := testClient(t, addr)
	var first, second PipelineResult

	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: []byte("1")}, &first).
		Store(Item{Key: []byte("b"), Value: []byte("2")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrServerError)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrServerError)
	equal(t, PipelineUnknown, second.State)
	isErrorIs(t, second.Err, ErrServerError)
}

func TestPipelineWriteFailureLeavesLaterOpsNotExecuted(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			cn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &failPipelineWriteConn{Conn: cn}, nil
		}
	})
	var first, second PipelineResult

	err := client.Pipeline().
		Store(Item{Key: []byte("a"), Value: make([]byte, connWriteBufferSize)}, &first).
		Store(Item{Key: []byte("b"), Value: []byte("2")}, &second).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	equal(t, PipelineUnknown, first.State)
	isErrorIs(t, first.Err, ErrTransport)
	equal(t, PipelineNotExecuted, second.State)
	isErrorIs(t, second.Err, ErrTransport)
}
