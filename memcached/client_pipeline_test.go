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
	items, err := doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{CAS: true})
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

func TestPipelineRunCancelled(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Run(ctx)
	isErrorIs(t, err, context.Canceled)
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
	err := client.Pipeline().
		Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).
		Run(context.Background())
	isErrorIs(t, err, ErrTransport)
	ver, verErr := client.Version(context.Background())
	noError(t, verErr)
	equal(t, "after-close", ver)
	greater(t, int(dials.Load()), 1)
}
