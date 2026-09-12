package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseArithmeticAcceptsSpacePadding(t *testing.T) {
	got, err := parseArithmetic([]byte("12 \r\n"))
	noError(t, err)
	equal(t, uint64(12), got)
}

func TestParseArithmeticEmpty(t *testing.T) {
	_, err := parseArithmetic([]byte("\r\n"))
	isErrorIs(t, err, ErrMalformedResponse)
	_, err = parseArithmetic(nil)
	isErrorIs(t, err, ErrMalformedResponse)
}

func TestParseVersionLine(t *testing.T) {
	var got []byte
	noError(t, parseVersionInto([][]byte{[]byte("VERSION 1.6.29\r\n")}, &got))
	equal(t, []byte("1.6.29"), got)

	noError(t, parseVersionInto([][]byte{[]byte("VERSION  1.6.29 \r\n")}, &got))
	equal(t, []byte("1.6.29"), got)

	isErrorIs(t, parseVersionInto(nil, &got), ErrMalformedResponse)
	isErrorIs(t, parseVersionInto([][]byte{[]byte("NOTVERSION\r\n")}, &got), ErrMalformedResponse)
	isErrorIs(t, parseVersionInto([][]byte{[]byte("VERSION x\r\n")}, nil), ErrInvalidArgument)

	line := []byte("VERSION 1.6.29\r\n")
	buf := make([]byte, 0, 16)
	noError(t, parseVersionInto([][]byte{line}, &buf))
	line[8] = 'X'
	equal(t, []byte("1.6.29"), buf)
	equal(t, 16, cap(buf))
}

func TestReplaceAppendPrependNotStored(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		mode StoreMode
	}{
		{name: "replace", mode: StoreReplace},
		{name: "append", mode: StoreAppend},
		{name: "prepend", mode: StorePrepend},
	} {
		t.Run(tt.name, func(t *testing.T) {
			noError(t, expectStore(ErrNotStored)(client.Store(ctx, Item{
				Mode: tt.mode, Key: []byte("missing"), Value: []byte("x"),
			})))
		})
	}
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("mid")}))
	noError(t, client.Store(ctx, Item{Mode: StoreAppend, Key: []byte("k"), Value: []byte("end")}))
	noError(t, client.Store(ctx, Item{Mode: StorePrepend, Key: []byte("k"), Value: []byte("pre")}))
	item := mustGet(ctx, t, client, []byte("k"))
	equal(t, []byte("premidend"), item.Value)
	equal(t, int32(1), dials.Load())
}

func TestIncrNonNumericDecrUnderflowWrap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("word"), Value: []byte("abc")}))
	var got uint64
	err := client.Incr(ctx, []byte("word"), 1, &got, ArithmeticOpts{})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "non-numeric")

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("2")}))
	noError(t, client.Decr(ctx, []byte("n"), 5, &got, ArithmeticOpts{}))
	equal(t, uint64(0), got)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("w"), Value: []byte("18446744073709551615")}))
	noError(t, client.Incr(ctx, []byte("w"), 1, &got, ArithmeticOpts{}))
	equal(t, uint64(0), got)
}

func TestExpiryTokenThirtyDaysAndMetaT(t *testing.T) {
	req, resp, err := buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, Rel(30*24*time.Hour), nil, false)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 2592000 1\r\nv\r\n"), req.wire())

	unix := Rel(31 * 24 * time.Hour)
	if unix <= 2592000 {
		t.Fatalf("31d must be unix, got %d", unix)
	}
	req, resp, err = buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, unix, nil, false)
	req, resp = mustBuildPair(t, req, resp, err)
	defer releaseReqAndResp(req, resp)
	contains(t, string(req.wire()), "set k 0 ")

	mg, mgResp := buildMetaGetCommand([]byte("foo"), &metaGetFlags{T: expiryToken(Rel(30 * 24 * time.Hour)), Tset: true})
	defer releaseReqAndResp(mg, mgResp)
	mg = requirePtr(t, mg)
	equal(t, []byte("mg foo T2592000\r\n"), mg.raw)
	ms, msResp := buildMetaSetCommand([]byte("foo"), []byte("v"), &metaSetFlags{T: expiryToken(Rel(30 * 24 * time.Hour)), Tset: true})
	defer releaseReqAndResp(ms, msResp)
	ms = requirePtr(t, ms)
	contains(t, string(ms.raw), " T2592000")

	touch, touchResp, err := buildTouchCommand([]byte("k"), Rel(30*24*time.Hour), nil, false)
	touch, touchResp = mustBuildPair(t, touch, touchResp, err)
	defer releaseReqAndResp(touch, touchResp)
	equal(t, []byte("touch k 2592000\r\n"), touch.raw)
	gat, err := encodeGetAndTouchesCommand("gat", Rel(30*24*time.Hour), nil, []byte("k"))
	gat = mustBuildReq(t, gat, err)
	defer gat.release()
	equal(t, []byte("gat 2592000 k\r\n"), gat.raw)
	gats, err := encodeGetAndTouchesCommand("gats", Rel(30*24*time.Hour), nil, []byte("k"))
	gats = mustBuildReq(t, gats, err)
	defer gats.release()
	equal(t, []byte("gats 2592000 k\r\n"), gats.raw)
}

func TestCancelDuringValueBodyDiscardsConn(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				errReply := []byte("ERROR\r\n")
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					cmd := strings.Fields(string(line))
					if len(cmd) == 0 {
						continue
					}
					switch cmd[0] {
					case "get", "gets":
						_, _ = cn.Write([]byte("VALUE k 0 100\r\n"))
						// Block on further reads until the client discards/closes
						// instead of sleeping past test teardown.
						_, _ = io.Copy(io.Discard, r)
						return
					case "version":
						_, _ = cn.Write([]byte("VERSION slow\r\n"))
					case "quit":
						return
					default:
						_, _ = cn.Write(errReply)
					}
				}
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
		cfg.Transport.ReadTimeout = time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err := doGet(ctx, client, [][]byte{[]byte("k")}, GetOpts{})
	cancel()
	isError(t, err)
	err = client.Version(context.Background(), new([]byte))
	noError(t, err)
	if dials.Load() < 2 {
		t.Fatalf("cancel during VALUE body must discard conn; dials=%d", dials.Load())
	}
}

func TestStoreQuietDiscardsConn(t *testing.T) {
	srv := startFakeMemcached(t)
	var dials atomic.Int32
	client := testClient(t, srv.Addr(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	ctx := context.Background()
	err := client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v"), Quiet: true})
	noError(t, err)
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k2"), Value: []byte("v2")}))
	if dials.Load() < 2 {
		t.Fatalf("quiet store should discard conn; dials=%d", dials.Load())
	}
}

func TestStatsDetailOnAllowedOnAppClient(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.AllowAdmin = true })
	_, err := doStatsAll(context.Background(), client, "sizes_enable")
	noError(t, err)
	_, err = doStatsAll(context.Background(), client, "detail", "on")
	isErrorIs(t, err, ErrInvalidArgument)

	admin := mustAdminOn(t, client)
	isErrorIs(t, admin.StatsSizes(context.Background(), nil), ErrInvalidArgument)
	got, err := doAdminStatsDetail(context.Background(), admin, StatsDetailOn)
	noError(t, err)
	equal(t, 0, len(got))
	got, err = doAdminStatsDetail(context.Background(), admin, StatsDetailOff)
	noError(t, err)
	equal(t, 0, len(got))
	got, err = doAdminStatsDetail(context.Background(), admin, StatsDetailDump)
	noError(t, err)
	v, ok := lookupStat(got, "foo")
	isTrue(t, ok)
	equal(t, "1 2 3", v)
	_, err = doAdminStatsDetail(context.Background(), admin, StatsDetailMode(99))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestIdempotentReadCommandExcludesStats(t *testing.T) {
	isFalse(t, idempotentReadCommand([]byte("stats")))
	isTrue(t, idempotentReadCommand([]byte("get")))
}

func TestIncrAcceptsSpacePaddedReplyKeepsConn(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				errReply := []byte("ERROR\r\n")
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					cmd := strings.Fields(string(line))
					if len(cmd) == 0 {
						continue
					}
					switch cmd[0] {
					case "incr", "decr":
						_, _ = cn.Write([]byte("12 \r\n"))
					case "version":
						_, _ = cn.Write([]byte("VERSION padded\r\n"))
					case "quit":
						return
					default:
						_, _ = cn.Write(errReply)
					}
				}
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	var got uint64
	noError(t, client.Incr(context.Background(), []byte("n"), 1, &got, ArithmeticOpts{}))
	equal(t, uint64(12), got)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("padded"), ver)
	equal(t, int32(1), dials.Load())
}
