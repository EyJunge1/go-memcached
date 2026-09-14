package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseArithmeticAcceptsSpacePadding(t *testing.T) {
	got, err := parseArithmetic([]byte("12 \r\n"))
	noError(t, err)
	equal(t, uint64(12), got)
}

func Test_parseArithmetic_empty(t *testing.T) {
	_, err := parseArithmetic([]byte("\r\n"))
	isErrorIs(t, err, ErrMalformedResponse)
	_, err = parseArithmetic(nil)
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_parseVersionLine(t *testing.T) {
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
	noError(t, expectStore(ErrNotStored)(client.Store(ctx, Item{Mode: StoreReplace, Key: []byte("missing"), Value: []byte("x")})))
	noError(t, expectStore(ErrNotStored)(client.Store(ctx, Item{Mode: StoreAppend, Key: []byte("missing"), Value: []byte("x")})))
	noError(t, expectStore(ErrNotStored)(client.Store(ctx, Item{Mode: StorePrepend, Key: []byte("missing"), Value: []byte("x")})))
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("mid")}))
	noError(t, client.Store(ctx, Item{Mode: StoreAppend, Key: []byte("k"), Value: []byte("end")}))
	noError(t, client.Store(ctx, Item{Mode: StorePrepend, Key: []byte("k"), Value: []byte("pre")}))
	item := mustGet(t, client, ctx, []byte("k"))
	equal(t, []byte("premidend"), item.Value)
	equal(t, int32(1), dials.Load())
}

func TestIncrNonNumericDecrUnderflowWrap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("word"), Value: []byte("abc")}))
	_, err := client.Incr(ctx, []byte("word"), 1)
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "non-numeric")

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("n"), Value: []byte("2")}))
	got, err := client.Decr(ctx, []byte("n"), 5)
	noError(t, err)
	equal(t, uint64(0), got)

	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("w"), Value: []byte("18446744073709551615")}))
	got, err = client.Incr(ctx, []byte("w"), 1)
	noError(t, err)
	equal(t, uint64(0), got)
}

func TestExpiryTokenThirtyDaysAndMetaT(t *testing.T) {
	req, resp := buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, Rel(30*24*time.Hour), false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 2592000 1\r\nv\r\n"), req.wire())

	unix := Rel(31 * 24 * time.Hour)
	if unix <= 2592000 {
		t.Fatalf("31d must be unix, got %d", unix)
	}
	req, resp = buildStorageCommand(_SetBytes, []byte("k"), []byte("v"), 0, unix, false)
	defer releaseReqAndResp(req, resp)
	contains(t, string(req.wire()), "set k 0 ")

	mg, _ := buildMetaGetCommand([]byte("foo"), &metaGetFlags{T: expiryToken(Rel(30 * 24 * time.Hour)), Tset: true})
	equal(t, []byte("mg foo T2592000\r\n"), mg.raw)
	ms, _ := buildMetaSetCommand([]byte("foo"), []byte("v"), &metaSetFlags{T: expiryToken(Rel(30 * 24 * time.Hour)), Tset: true})
	contains(t, string(ms.raw), " T2592000")

	touch, touchResp := buildTouchCommand([]byte("k"), Rel(30*24*time.Hour), false)
	defer releaseReqAndResp(touch, touchResp)
	equal(t, []byte("touch k 2592000\r\n"), touch.raw)
	gat, gatResp := buildGetAndTouchesCommand("gat", Rel(30*24*time.Hour), []byte("k"))
	defer releaseReqAndResp(gat, gatResp)
	equal(t, []byte("gat 2592000 k\r\n"), gat.raw)
	gats, gatsResp := buildGetAndTouchesCommand("gats", Rel(30*24*time.Hour), []byte("k"))
	defer releaseReqAndResp(gats, gatsResp)
	equal(t, []byte("gats 2592000 k\r\n"), gats.raw)
}

func TestCancelDuringValueBodyDiscardsConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
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
						time.Sleep(2 * time.Second)
						_, _ = io.Copy(io.Discard, r)
						return
					case "version":
						_, _ = cn.Write([]byte("VERSION slow\r\n"))
					case "quit":
						return
					default:
						_, _ = cn.Write([]byte("ERROR\r\n"))
					}
				}
			}(cn)
		}
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
		cfg.Transport.ReadTimeout = time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err = doGet(client, ctx, [][]byte{[]byte("k")}, GetOpts{})
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
	_, err := doStatsAll(client, context.Background(), "sizes_enable")
	noError(t, err)
	_, err = doStatsAll(client, context.Background(), "detail", "on")
	isErrorIs(t, err, ErrInvalidArgument)

	admin := mustAdminOn(t, client)
	isErrorIs(t, admin.StatsSizes(context.Background(), nil), ErrInvalidArgument)
	got, err := doAdminStatsDetail(admin, context.Background(), StatsDetailOn)
	noError(t, err)
	equal(t, 0, len(got))
	got, err = doAdminStatsDetail(admin, context.Background(), StatsDetailOff)
	noError(t, err)
	equal(t, 0, len(got))
	got, err = doAdminStatsDetail(admin, context.Background(), StatsDetailDump)
	noError(t, err)
	v, ok := lookupStat(got, "foo")
	isTrue(t, ok)
	equal(t, "1 2 3", v)
	_, err = doAdminStatsDetail(admin, context.Background(), StatsDetailMode(99))
	isErrorIs(t, err, ErrInvalidArgument)
}

func TestIdempotentReadCommandExcludesStats(t *testing.T) {
	isFalse(t, idempotentReadCommand([]byte("stats")))
	isTrue(t, idempotentReadCommand([]byte("get")))
}

func TestMetaPipelineNoopDoesNotBreakBarrier(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, client.Store(ctx, Item{Mode: StoreSet, Key: []byte("a"), Value: []byte("1")}))
	got, err := doMetaPipeline(client, ctx, []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Return: MetaGetReturn{Value: true}}},
		{Noop: &MetaNoop{ProxyHint: "hint", ProxyPath: "/path"}},
	})
	noError(t, err)
	hasLen(t, got, 2)
	equal(t, []byte("1"), got[0].Item.Value)
	equal(t, MetaStatusMN, got[1].Status)
	srv.mu.Lock()
	cmd := srv.lastCmd
	srv.mu.Unlock()
	contains(t, cmd, "mn")
	contains(t, cmd, "Phint")
	contains(t, cmd, "L/path")
	err = client.Version(ctx, new([]byte))
	noError(t, err)
}

func TestMetaSetHonorsMaxValueBytes(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.MaxValueBytes = 4 })
	_, err := doMetaSet(client, context.Background(), []byte("k"), []byte("too-long"), MetaSet{})
	isErrorIs(t, err, ErrInvalidValue)
}

func TestIncrAcceptsSpacePaddedReplyKeepsConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
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
						_, _ = cn.Write([]byte("ERROR\r\n"))
					}
				}
			}(cn)
		}
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	got, err := client.Incr(context.Background(), []byte("n"), 1)
	noError(t, err)
	equal(t, uint64(12), got)
	ver, err := doVersion(client, context.Background())
	noError(t, err)
	equal(t, []byte("padded"), ver)
	equal(t, int32(1), dials.Load())
}

func TestNoReplyClientErrorDoesNotPoisonNextGet(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
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
					case "set", "add", "replace", "append", "prepend", "cas":
						if len(cmd) >= 5 {
							if nbytes, err := strconv.Atoi(cmd[4]); err == nil && nbytes > 0 {
								_, _ = io.ReadFull(r, make([]byte, nbytes+2))
							}
						}
						if cmd[len(cmd)-1] == "noreply" {
							_, _ = cn.Write([]byte("CLIENT_ERROR bad command line\r\n"))
						} else {
							_, _ = cn.Write([]byte("STORED\r\n"))
						}
					case "get", "gets":
						_, _ = cn.Write([]byte("END\r\n"))
					case "version":
						_, _ = cn.Write([]byte("VERSION after-quiet\r\n"))
					case "quit":
						return
					default:
						_, _ = cn.Write([]byte("ERROR\r\n"))
					}
				}
			}(cn)
		}
	}()

	var dials atomic.Int32
	client := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	ctx := context.Background()
	err = client.Store(ctx, Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v"), Quiet: true})
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad command line")
	items, err := doGet(client, ctx, [][]byte{[]byte("missing")}, GetOpts{})
	noError(t, err)
	hasLen(t, items, 1)
	isFalse(t, items[0].Found)
	ver, err := doVersion(client, ctx)
	noError(t, err)
	equal(t, []byte("after-quiet"), ver)
	if dials.Load() < 2 {
		t.Fatalf("quiet CLIENT_ERROR must discard conn; dials=%d", dials.Load())
	}
}

func TestMetaPipelineRejectsDuplicateOpaque(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := doMetaPipeline(client, context.Background(), []MetaOp{
		{Key: []byte("a"), Get: &MetaGet{Opaque: []byte("same")}},
		{Key: []byte("b"), Get: &MetaGet{Opaque: []byte("same")}},
	})
	isErrorIs(t, err, ErrInvalidArgument)
}
