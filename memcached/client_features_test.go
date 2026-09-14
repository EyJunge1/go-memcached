package memcached

import (
	"bufio"
	"context"
	"errors"
	"net"
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
		for _, field := range strings.Fields(cmd) {
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
	n, err := client.Incr(ctx, []byte("n"), 3)
	noError(t, err)
	equal(t, uint64(13), n)
	item := mustGet(t, client, ctx, []byte("n"))
	equal(t, []byte("13"), item.Value)

	n, err = client.Decr(ctx, []byte("n"), 1)
	noError(t, err)
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

func TestMetaSingleCommandStatusCodes(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	res, err := doMetaGet(client, ctx, []byte("missing"), MetaGet{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusEN, res.Status)

	_, err = client.Meta().Set(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	res, err = client.Meta().Set(ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)
	equal(t, MetaStatusNS, res.Status)

	hit, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true}})
	noError(t, err)
	cas := hit.Item.CAS
	wrong := cas + 1
	res, err = client.Meta().Set(ctx, []byte("k"), []byte("v3"), MetaSet{CompareCAS: &wrong})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrExists)
	equal(t, MetaStatusEX, res.Status)

	res, err = client.Meta().Delete(ctx, []byte("nope"), MetaDelete{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusNF, res.Status)
}

func TestMetaHintsRejectWhitespace(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := doMetaGet(client, context.Background(), []byte("k"), MetaGet{P: "bad token"})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.Meta().Noop(context.Background(), MetaNoop{L: "x y"})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestFlushAllRejectsNegative(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	isErrorIs(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Stagger: -time.Second}), ErrInvalidArgument)
}

func TestMetaNoopStatusAndProxyHints(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := client.Meta().Noop(context.Background(), MetaNoop{})
	noError(t, err)
	equal(t, MetaStatusMN, res.Status)

	res, err = client.Meta().Noop(context.Background(), MetaNoop{P: "hint", L: "/path"})
	noError(t, err)
	equal(t, MetaStatusMN, res.Status)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "Phint")
	contains(t, cmd, "L/path")
}

func TestMetaDebugStatus(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	item, err := client.Meta().Debug(ctx, []byte("missing"), MetaDebug{})
	noError(t, err)
	isErrorIs(t, item.Err(), ErrNotFound)
	equal(t, MetaStatusEN, item.Status)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	item, err = client.Meta().Debug(ctx, []byte("k"), MetaDebug{})
	noError(t, err)
	equal(t, MetaStatusME, item.Status)

	bin := []byte("bin-key")
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: bin, Value: []byte("v")}))
	item, err = client.Meta().Debug(ctx, bin, MetaDebug{BinaryKey: true})
	noError(t, err)
	equal(t, bin, item.Key)
	isTrue(t, item.KeyBase64)
	equal(t, base64Encode(bin), item.WireKey)
}

func TestMetaGetRecacheWindow(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Recache: -time.Second})
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}))
	_, err = doMetaGet(client, ctx, []byte("k"), MetaGet{Recache: 31 * 24 * time.Hour, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "R2678400")

	_, err = doMetaGet(client, ctx, []byte("k"), MetaGet{Recache: 5 * time.Second, Return: MetaGetReturn{Value: true}})
	noError(t, err)
	srv.mu.Lock()
	cmd = srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "R5")
}

func TestFlushOptsQuietAndStagger(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr()+","+b.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{
		Stagger: time.Second,
	}))
	delays := map[string]struct{}{}
	a.mu.Lock()
	delays[a.lastFlushDelay] = struct{}{}
	a.mu.Unlock()
	b.mu.Lock()
	delays[b.lastFlushDelay] = struct{}{}
	b.mu.Unlock()
	if _, ok := delays[""]; !ok {
		t.Fatalf("node 0 delay omitted, got %v", delays)
	}
	if _, ok := delays["1"]; !ok {
		t.Fatalf("node 1 delay 1, got %v", delays)
	}

	noError(t, mustAdmin(t, client).All().Flush(context.Background(), FlushOpts{Quiet: true}))
	if !waitEitherLastCmdContains(t, a, b, "noreply") {
		t.Fatalf("quiet flush_all missing noreply")
	}
}

func TestFlushDelayForAddsInTimeDomain(t *testing.T) {
	fn := flushDelayFor(FlushOpts{Delay: delayPtr(Rel(10 * time.Second))})
	equal(t, Exptime(10), fn(0))
	equal(t, Exptime(10), fn(1))
	fn = flushDelayFor(FlushOpts{Stagger: time.Second})
	equal(t, ExptimeNever, fn(0))
	equal(t, Exptime(1), fn(1))
	equal(t, ExptimeNever, flushDelayFor(FlushOpts{})(0))
}

func TestAdminQuietNoreply(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	noError(t, admin.Verbosity(context.Background(), 1, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "verbosity 1 noreply")

	noError(t, admin.CacheMemLimit(context.Background(), 32, AdminOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "cache_memlimit 32 noreply")
}

func TestAuthRejectsWrongPassword(t *testing.T) {
	srv := startFakeMemcachedAuth(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Auth = &Auth{User: "u", Pass: "wrong"}
	})
	_, err := client.VersionAll(context.Background())
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	contains(t, err.Error(), "authentication failure")

	_, err = doGet(client, context.Background(), [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	err = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
}

func TestConnCloseSkipsQuitOnUDP(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "udp", wr: bufio.NewWriter(client)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cn.Close()
	}()
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if n != 0 {
		t.Fatalf("udp close wrote %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected EOF or timeout on udp close")
	}
	<-done
}

func TestConnCloseTCPWritesQuit(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "tcp", wr: bufio.NewWriter(client)}
	go func() { _ = cn.Close() }()
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	noError(t, err)
	equal(t, []byte("quit\r\n"), buf[:n])
}

func TestMetaArithmeticQuietReturnValueIsRejected(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("10")}))
	_, err := client.Meta().Arithmetic(ctx, []byte("n"), 1, MetaArithmetic{Quiet: true, ReturnValue: true})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestCommandErrorFromGet(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
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
	_, err = doGet(client, context.Background(), [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrCommandError)
	isFalse(t, errors.Is(err, ErrTransport))
}

func TestEmptyServersIsInvalidAddress(t *testing.T) {
	_, err := New(Config{})
	isErrorIs(t, err, ErrInvalidAddress)

	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowSetServers = true })
	noError(t, client.SetServers(nil))
	ctx := context.Background()
	_, err = doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrInvalidAddress)
	contains(t, err.Error(), "no servers configured")
	err = client.Pipeline().Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).Run(ctx)
	isErrorIs(t, err, ErrInvalidAddress)
	_, err = doMetaPipeline(client, ctx, []MetaOp{{Key: []byte("k"), Get: &MetaGet{}}})
	isErrorIs(t, err, ErrInvalidAddress)
}

func TestAdminMgDumpEmptyEN(t *testing.T) {
	srv := startFakeMemcached(t)
	srv.mu.Lock()
	srv.dumpEmpty = true
	srv.mu.Unlock()
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := admin.LRUCrawlerMgDump(ctx, "all")
	noError(t, err)
	_, ok := <-ch
	isFalse(t, ok)
}

func TestMetaDeleteMissAndQuietNS(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	res, err := client.Meta().Delete(ctx, []byte("missing"), MetaDelete{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	res, err = client.Meta().Delete(ctx, []byte("missing"), MetaDelete{Quiet: true})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)

	_, err = client.Meta().Set(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	res, err = client.Meta().Set(ctx, []byte("k"), []byte("v2"), MetaSet{Quiet: true, Mode: MetaSetModeAdd})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotStored)

	res, err = client.Meta().Arithmetic(ctx, []byte("nope"), 1, MetaArithmetic{})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
}

func TestMetaGetMissKeepsENFlags(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := doMetaGet(client, context.Background(), []byte("missing"), MetaGet{Return: MetaGetReturn{Key: true}})
	noError(t, err)
	isErrorIs(t, res.Err(), ErrNotFound)
	equal(t, MetaStatusEN, res.Status)
}

func TestMetaDeleteInvalidateTTL(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))
	cas := uint64(1)
	newCAS := uint64(2)

	_, err := client.Meta().Delete(ctx, []byte("k"), MetaDelete{
		Invalidate: true,
		TTL:        delayPtr(Rel(5 * time.Second)),
		CompareCAS: &cas,
		NewCAS:     &newCAS,
	})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "I")
	waitLastCmdHasToken(t, srv, "T5")
	waitLastCmdHasToken(t, srv, "C1")
	waitLastCmdHasToken(t, srv, "E2")

	got, err := doMetaGet(client, ctx, []byte("k"), MetaGet{Return: MetaGetReturn{Value: true}})
	noError(t, err)
	equal(t, []byte("v"), got.Item.Value)
	isTrue(t, got.Item.Stale)
}

func TestMetaDeleteRemoveValueOnlyWire(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))

	_, err := client.Meta().Delete(ctx, []byte("k"), MetaDelete{RemoveValueOnly: true})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "x")
}

func TestMetaSetAutoVivifyAppend(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	mustStore(t, client, ctx, []byte("k"), []byte("v"))

	_, err := client.Meta().Set(ctx, []byte("k"), []byte("x"), MetaSet{
		Mode:       MetaSetModeAppend,
		AutoVivify: delayPtr(Rel(time.Second)),
	})
	noError(t, err)
	waitLastCmdHasToken(t, srv, "MA")
	waitLastCmdHasToken(t, srv, "N1")
}
