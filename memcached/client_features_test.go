package memcached

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func waitLastCmdContains(t *testing.T, srv *fakeMemcached, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		cmd := srv.lastCmd
		srv.mu.Unlock()
		if strings.Contains(cmd, want) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	t.Fatalf("%q does not contain %q", cmd, want)
}

func waitLastCmdHasToken(t *testing.T, srv *fakeMemcached, token string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		cmd := srv.lastCmd
		srv.mu.Unlock()
		for field := range strings.FieldsSeq(cmd) {
			if field == token {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	t.Fatalf("%q has no token %q", cmd, token)
}

func waitEitherLastCmdContains(t *testing.T, a, b *fakeMemcached, want string) bool {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		ac := a.lastCmd
		a.mu.Unlock()
		b.mu.Lock()
		bc := b.lastCmd
		b.mu.Unlock()
		if strings.Contains(ac, want) || strings.Contains(bc, want) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func TestMutateQuietSendsNoreply(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))

	noError(t, client.Delete(ctx, []byte("n"), MutateOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "delete n noreply")

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))
	var n uint64
	noError(t, client.Incr(ctx, []byte("n"), 3, &n, ArithmeticOpts{}))
	equal(t, uint64(13), n)
	item := mustGet(ctx, t, client, []byte("n"))
	equal(t, []byte("13"), item.Value)

	noError(t, client.Decr(ctx, []byte("n"), 1, &n, ArithmeticOpts{}))
	equal(t, uint64(12), n)

	noError(t, client.Touch(ctx, []byte("n"), Rel(time.Second), MutateOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "touch n 1 noreply")
}

func TestPipelineAppendZerosExp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	noError(t, client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	err := client.Pipeline().
		Store(Item{Mode: StoreAppend, Key: []byte("k"), Value: []byte("x"), Exp: Rel(30 * time.Second), Flags: 9}).
		Run(context.Background())
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "append k 0 0 1")
}

func TestCommandErrorFromGet(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = cn.Close() }()
		buf := make([]byte, 64)
		_, _ = cn.Read(buf)
		_, _ = cn.Write([]byte("ERROR bad args\r\n"))
	}()
	client := testClient(t, ln.Addr().String())
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrCommandError)
	isFalse(t, errors.Is(err, ErrTransport))
}
