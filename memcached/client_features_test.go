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
	noError(t, storeOK(client.Set(ctx, []byte("n"), []byte("10"), 0, 0)))

	noError(t, client.Delete(ctx, []byte("n"), MutateOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "delete n noreply")

	noError(t, storeOK(client.Set(ctx, []byte("n"), []byte("10"), 0, 0)))
	_, err := client.Incr(ctx, []byte("n"), 3, MutateOpts{Quiet: true})
	isErrorIs(t, err, ErrInvalidArgument)
	noError(t, client.IncrQuiet(ctx, []byte("n"), 3))
	item, err := client.GetOne(ctx, []byte("n"))
	noError(t, err)
	equal(t, []byte("13"), item.Value)

	_, err = client.Decr(ctx, []byte("n"), 1, MutateOpts{Quiet: true})
	isErrorIs(t, err, ErrInvalidArgument)
	noError(t, client.DecrQuiet(ctx, []byte("n"), 1))

	noError(t, client.Touch(ctx, []byte("n"), Rel(time.Second), MutateOpts{Quiet: true}))
	waitLastCmdContains(t, srv, "touch n 1 noreply")
}

func TestPipelineAppendZerosExp(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	noError(t, storeOK(client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)))
	err := client.Pipeline().
		Append(&Item{Key: []byte("k"), Value: []byte("x"), Exp: Rel(30 * time.Second), Flags: 9}).
		Flush(context.Background())
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

	res, err := client.MetaGet(ctx, []byte("missing"), MetaGet{})
	isErrorIs(t, err, ErrNotFound)
	equal(t, MetaStatusEN, res.Status)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	isErrorIs(t, err, ErrNotStored)
	// Status is set on the returned result even when err != nil
	res, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSet{Mode: MetaSetModeAdd})
	isErrorIs(t, err, ErrNotStored)
	equal(t, MetaStatusNS, res.Status)

	hit, err := client.MetaGet(ctx, []byte("k"), MetaGet{Return: MetaGetReturn{CAS: true}})
	noError(t, err)
	cas := hit.Item.CAS
	wrong := cas + 1
	res, err = client.MetaSet(ctx, []byte("k"), []byte("v3"), MetaSet{CompareCAS: &wrong})
	isErrorIs(t, err, ErrExists)
	equal(t, MetaStatusEX, res.Status)

	res, err = client.MetaDelete(ctx, []byte("nope"), MetaDelete{})
	isErrorIs(t, err, ErrNotFound)
	equal(t, MetaStatusNF, res.Status)
}

func TestMetaHintsRejectWhitespace(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.MetaGet(context.Background(), []byte("k"), MetaGet{P: "bad token"})
	isErrorIs(t, err, ErrInvalidArgument)
	_, err = client.MetaNoop(context.Background(), MetaNoop{L: "x y"})
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestFlushAllRejectsNegative(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.AllowAdmin = true
		cfg.AllowFlushAll = true
	})
	isErrorIs(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{Delay: -time.Second}), ErrInvalidArgument)
	isErrorIs(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{Stagger: -time.Second}), ErrInvalidArgument)
}

func TestMetaNoopStatusAndProxyHints(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := client.MetaNoop(context.Background(), MetaNoop{})
	noError(t, err)
	equal(t, MetaStatusMN, res.Status)

	res, err = client.MetaNoop(context.Background(), MetaNoop{P: "hint", L: "/path"})
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

	item, err := client.MetaDebug(ctx, []byte("missing"), MetaDebug{})
	isErrorIs(t, err, ErrNotFound)
	notNil(t, item)
	equal(t, MetaStatusEN, item.Status)

	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	item, err = client.MetaDebug(ctx, []byte("k"), MetaDebug{})
	noError(t, err)
	equal(t, MetaStatusME, item.Status)

	bin := []byte("bin-key")
	noError(t, storeOK(client.Set(ctx, bin, []byte("v"), 0, 0)))
	item, err = client.MetaDebug(ctx, bin, MetaDebug{BinaryKey: true})
	noError(t, err)
	equal(t, bin, item.Key)
	isTrue(t, item.KeyBase64)
	equal(t, base64Encode(bin), item.WireKey)
}

func TestGetEarlyRecacheWindow(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	_, _, err := client.GetEarlyRecache(ctx, []byte("k"), 0)
	isErrorIs(t, err, ErrInvalidArgument)
	_, _, err = client.GetEarlyRecache(ctx, []byte("k"), -time.Second)
	isErrorIs(t, err, ErrInvalidArgument)

	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("v"), 0, 0)))
	_, _, err = client.GetEarlyRecache(ctx, []byte("k"), 31*24*time.Hour)
	noError(t, err)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "R2678400")

	_, _, err = client.GetEarlyRecache(ctx, []byte("k"), 5*time.Second)
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
		cfg.Cluster.PickBuilder = NewCRC32HashPickBuilder()
	})
	noError(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{
		Delay:   time.Second,
		Stagger: time.Second,
	}))
	delays := map[string]struct{}{}
	a.mu.Lock()
	delays[a.lastFlushDelay] = struct{}{}
	a.mu.Unlock()
	b.mu.Lock()
	delays[b.lastFlushDelay] = struct{}{}
	b.mu.Unlock()
	if _, ok := delays["1"]; !ok {
		t.Fatalf("node 0 delay 1, got %v", delays)
	}
	if _, ok := delays["2"]; !ok {
		t.Fatalf("node 1 delay 2, got %v", delays)
	}

	noError(t, mustAdmin(t, client).FlushAll(context.Background(), FlushOpts{Quiet: true}))
	if !waitEitherLastCmdContains(t, a, b, "noreply") {
		t.Fatalf("quiet flush_all missing noreply")
	}
}

func TestFlushDelayForAddsInTimeDomain(t *testing.T) {
	fn := flushDelayFor(FlushOpts{Delay: 10 * time.Second, Stagger: time.Second})
	equal(t, Exptime(10), fn(0))
	equal(t, Exptime(11), fn(1))
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
	err := client.Ping(context.Background())
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	contains(t, err.Error(), "authentication failure")

	_, err = client.GetOne(context.Background(), []byte("k"))
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrTransport))
	err = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
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
	noError(t, storeOK(client.Set(ctx, []byte("n"), []byte("10"), 0, 0)))
	_, err := client.MetaArithmetic(ctx, []byte("n"), 1, MetaArithmetic{Quiet: true, ReturnValue: true})
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
	_, err = client.GetOne(context.Background(), []byte("k"))
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
	_, err = client.Get(ctx, GetOpts{}, []byte("k"))
	isErrorIs(t, err, ErrInvalidAddress)
	contains(t, err.Error(), "no servers configured")
	err = client.Pipeline().Set(&Item{Key: []byte("k"), Value: []byte("v")}).Flush(ctx)
	isErrorIs(t, err, ErrInvalidAddress)
	_, err = client.MetaPipeline(ctx, []MetaOp{{Op: MetaOpGet, Key: []byte("k")}})
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
	_, err := client.MetaDelete(ctx, []byte("missing"), MetaDelete{})
	isErrorIs(t, err, ErrNotFound)
	_, err = client.MetaDelete(ctx, []byte("missing"), MetaDelete{Quiet: true})
	isErrorIs(t, err, ErrNotFound)

	_, err = client.MetaSet(ctx, []byte("k"), []byte("v"), MetaSet{})
	noError(t, err)
	_, err = client.MetaSet(ctx, []byte("k"), []byte("v2"), MetaSet{Quiet: true, Mode: MetaSetModeAdd})
	isErrorIs(t, err, ErrNotStored)

	_, err = client.MetaArithmetic(ctx, []byte("nope"), 1, MetaArithmetic{})
	isErrorIs(t, err, ErrNotFound)
}

func TestMetaGetMissKeepsENFlags(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	res, err := client.MetaGet(context.Background(), []byte("missing"), MetaGet{Return: MetaGetReturn{Key: true}})
	isErrorIs(t, err, ErrNotFound)
	equal(t, MetaStatusEN, res.Status)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
