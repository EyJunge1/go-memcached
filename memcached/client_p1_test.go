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
	noError(t, expectStore(ErrNotStored)(client.Replace(ctx, []byte("missing"), []byte("x"), 0, 0)))
	noError(t, expectStore(ErrNotStored)(client.Append(ctx, []byte("missing"), []byte("x"))))
	noError(t, expectStore(ErrNotStored)(client.Prepend(ctx, []byte("missing"), []byte("x"))))
	noError(t, storeOK(client.Set(ctx, []byte("k"), []byte("mid"), 0, 0)))
	noError(t, storeOK(client.Append(ctx, []byte("k"), []byte("end"))))
	noError(t, storeOK(client.Prepend(ctx, []byte("k"), []byte("pre"))))
	item, err := client.GetOne(ctx, []byte("k"))
	noError(t, err)
	equal(t, []byte("premidend"), item.Value)
	equal(t, int32(1), dials.Load())
}

func TestIncrNonNumericDecrUnderflowWrap(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()

	noError(t, storeOK(client.Set(ctx, []byte("word"), []byte("abc"), 0, 0)))
	_, err := client.Incr(ctx, []byte("word"), 1, MutateOpts{})
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "non-numeric")

	noError(t, storeOK(client.Set(ctx, []byte("n"), []byte("2"), 0, 0)))
	got, err := client.Decr(ctx, []byte("n"), 5, MutateOpts{})
	noError(t, err)
	equal(t, uint64(0), got)

	noError(t, storeOK(client.Set(ctx, []byte("w"), []byte("18446744073709551615"), 0, 0)))
	got, err = client.Incr(ctx, []byte("w"), 1, MutateOpts{})
	noError(t, err)
	equal(t, uint64(0), got)
}

func TestExpiryTokenThirtyDaysAndMetaT(t *testing.T) {
	req, resp := buildStorageCommand("set", []byte("k"), []byte("v"), 0, Rel(30*24*time.Hour), false)
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("set k 0 2592000 1\r\nv\r\n"), req.wire())

	unix := Rel(31 * 24 * time.Hour)
	if unix <= 2592000 {
		t.Fatalf("31d must be unix, got %d", unix)
	}
	req, resp = buildStorageCommand("set", []byte("k"), []byte("v"), 0, unix, false)
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
	_, err = client.GetOne(ctx, []byte("k"))
	cancel()
	isError(t, err)
	_, err = client.Version(context.Background())
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
	err := client.Store(ctx, StoreSet, StoreRequest{Key: []byte("k"), Value: []byte("v"), Quiet: true})
	noError(t, err)
	noError(t, storeOK(client.Set(ctx, []byte("k2"), []byte("v2"), 0, 0)))
	if dials.Load() < 2 {
		t.Fatalf("quiet store should discard conn; dials=%d", dials.Load())
	}
}

func TestParseGeneralStats(t *testing.T) {
	st := ParseGeneralStats(map[string]string{"pid": "1", "version": "fake-1.0", "custom": "x"})
	equal(t, "fake-1.0", st.Version)
	equal(t, "x", st.Extra["custom"])
}

func TestStatsDetailOnAllowedOnAppClient(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	got, err := client.Stats(context.Background(), "detail", "on")
	noError(t, err)
	equal(t, "detail", got["arg"])
	_, err = client.Stats(context.Background(), "sizes_enable")
	noError(t, err)
}

func TestIdempotentReadCommandExcludesStats(t *testing.T) {
	isFalse(t, idempotentReadCommand([]byte("stats")))
	isTrue(t, idempotentReadCommand([]byte("get")))
}

func TestMetaPipelineNoopDoesNotBreakBarrier(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	ctx := context.Background()
	noError(t, storeOK(client.Set(ctx, []byte("a"), []byte("1"), 0, 0)))
	got, err := client.MetaPipeline(ctx, []MetaOp{
		{Op: MetaOpGet, Key: []byte("a"), Flags: MetaGet{Return: MetaGetReturn{Value: true}}},
		{Op: MetaOpNoop},
	})
	noError(t, err)
	hasLen(t, got, 2)
	equal(t, []byte("1"), got[0].Item.Value)
	equal(t, MetaStatusMN, got[1].Status)
	_, err = client.Version(ctx)
	noError(t, err)
}

func TestMetaSetHonorsMaxValueBytes(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr(), func(cfg *Config) { cfg.MaxValueBytes = 4 })
	_, err := client.MetaSet(context.Background(), []byte("k"), []byte("too-long"), MetaSet{})
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
	got, err := client.Incr(context.Background(), []byte("n"), 1, MutateOpts{})
	noError(t, err)
	equal(t, uint64(12), got)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "padded", ver)
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
	err = client.Store(ctx, StoreSet, StoreRequest{Key: []byte("k"), Value: []byte("v"), Quiet: true})
	noError(t, err)
	_, err = client.GetOne(ctx, []byte("missing"))
	isErrorIs(t, err, ErrNotFound)
	ver, err := client.Version(ctx)
	noError(t, err)
	equal(t, "after-quiet", ver)
	if dials.Load() < 2 {
		t.Fatalf("quiet CLIENT_ERROR must discard conn; dials=%d", dials.Load())
	}
}

func TestMetaPipelineRejectsDuplicateOpaque(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	_, err := client.MetaPipeline(context.Background(), []MetaOp{
		{Op: MetaOpGet, Key: []byte("a"), Flags: MetaGet{Opaque: "same"}},
		{Op: MetaOpGet, Key: []byte("b"), Flags: MetaGet{Opaque: "same"}},
	})
	isErrorIs(t, err, ErrInvalidArgument)
}
