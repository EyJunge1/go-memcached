package memcached

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func startCmdServer(t *testing.T, reply func(fields []string) string) (addr string, dials *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	var n atomic.Int32
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			go func(cn net.Conn) {
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(string(line))
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "set", "add", "replace", "append", "prepend", "cas":
						if len(fields) >= 5 {
							if nbytes, err := strconv.Atoi(fields[4]); err == nil && nbytes >= 0 {
								_, _ = io.ReadFull(r, make([]byte, nbytes+2))
							}
						}
					case "quit", "shutdown":
						return
					}
					if out := reply(fields); out != "" {
						_, _ = cn.Write([]byte(out))
					}
				}
			}(cn)
		}
	}()
	return ln.Addr().String(), &n
}

func TestCommandErrorHeldOnGet(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "ERROR bad args\r\n"
		case "version":
			return "VERSION held\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr, func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_, err := client.GetOne(context.Background(), []byte("k"))
	isErrorIs(t, err, ErrCommandError)
	isFalse(t, errors.Is(err, ErrTransport))
	contains(t, err.Error(), "bad args")
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "held", ver)
	equal(t, int32(1), dials.Load())
}

func TestPipelineClientErrorHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "set":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-pipe\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	err := client.Pipeline().Set(&Item{Key: []byte("k"), Value: []byte("v")}).Flush(context.Background())
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-pipe", ver)
	equal(t, int32(1), dials.Load())
}

func TestStatsClientErrorPublic(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "stats":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-stats\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.Stats(context.Background())
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	isFalse(t, errors.Is(err, ErrTransport))
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-stats", ver)
	equal(t, int32(1), dials.Load())
}

func TestWatchErrorAfterOK(t *testing.T) {
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
		_, _ = cn.Write([]byte("OK\r\nERROR cannot pipeline other commands before watch\r\n"))
	}()
	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := admin.Watch(ctx, WatchFetchers)
	noError(t, err)
	ev := <-events
	isErrorIs(t, ev.Err, ErrCommandError)
	contains(t, ev.Err.Error(), "cannot pipeline")
}

func TestDumpMidStreamCommandError(t *testing.T) {
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
		_, _ = cn.Write([]byte("key=foo exp=-1 la=0 cas=1 fetch=no cls=1 size=3\r\nERROR locked try again later\r\n"))
	}()
	client := testClient(t, ln.Addr().String(), func(cfg *Config) { cfg.AllowAdmin = true })
	admin := mustAdminOn(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := admin.LRUCrawlerMetaDump(ctx, "all")
	noError(t, err)
	row := <-ch
	equal(t, "foo", row.Fields["key"])
	isNil(t, row.Err)
	fail := <-ch
	isErrorIs(t, fail.Err, ErrCommandError)
	contains(t, fail.Err.Error(), "locked")
}

func TestMetaPipelineQuietSetIsHD(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	got, err := client.MetaPipeline(context.Background(), []MetaOp{
		{Op: MetaOpSet, Key: []byte("k"), Body: []byte("v")},
	})
	noError(t, err)
	hasLen(t, got, 1)
	equal(t, MetaStatusHD, got[0].Status)
	isNil(t, got[0].Err())
}

func TestGetAcceptsLFOnlyEND(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "END\n"
		case "version":
			return "VERSION lf-end\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.Get(context.Background(), GetOpts{}, []byte("k"))
	noError(t, err)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "lf-end", ver)
	equal(t, int32(1), dials.Load())
}

func TestMetaQuietBarrierAcceptsLFOnlyMN(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "mg":
			return ""
		case "mn":
			return "MN\n"
		case "version":
			return "VERSION lf-mn\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	res, err := client.MetaGet(context.Background(), []byte("k"), MetaGet{Quiet: true})
	isErrorIs(t, err, ErrNotFound)
	equal(t, MetaStatusEN, res.Status)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "lf-mn", ver)
	equal(t, int32(1), dials.Load())
}

func TestIncrQuietClientError(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "incr":
			return "CLIENT_ERROR bad delta\r\n"
		case "version":
			return "VERSION after-incr\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	err := client.IncrQuiet(context.Background(), []byte("n"), 1)
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad delta")
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-incr", ver)
	if dials.Load() < 2 {
		t.Fatalf("quiet incr CLIENT_ERROR must discard conn; dials=%d", dials.Load())
	}
}

func TestMetaPipelineChargeKeepsSiblingHD(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "HD O0\r\n"
		case "mn":
			return "CLIENT_ERROR bad meta\r\nMN\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	got, err := client.MetaPipeline(context.Background(), []MetaOp{
		{Op: MetaOpDelete, Key: []byte("k")},
	})
	isErrorIs(t, err, ErrClientError)
	hasLen(t, got, 1)
	equal(t, MetaStatusHD, got[0].Status)
	isNil(t, got[0].Err())
}

func TestParseArithmeticAfterRecvForecastsFaults(t *testing.T) {
	var value uint64
	fn := parseArithmeticAfterRecv(&value)
	resp := &response{rawLines: [][]byte{[]byte("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")}}
	err := fn(resp)
	isErrorIs(t, err, ErrClientError)
	isFalse(t, errors.Is(err, ErrMalformedResponse))

	resp = &response{rawLines: [][]byte{[]byte("NOT_FOUND\r\n")}}
	err = fn(resp)
	isErrorIs(t, err, ErrNotFound)
	isFalse(t, errors.Is(err, ErrMalformedResponse))
}

func TestAdminAllJoinsBusyAndServerError(t *testing.T) {
	busy, _ := startCmdServer(t, func(fields []string) string {
		return "BUSY crawler\r\n"
	})
	oom, _ := startCmdServer(t, func(fields []string) string {
		return "SERVER_ERROR oom\r\n"
	})
	cfg, err := testConfig(busy, func(c *Config) { c.AllowAdmin = true })
	noError(t, err)
	more, err := ParseServers(oom)
	noError(t, err)
	cfg.Servers = append(cfg.Servers, more...)
	client, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	err = mustAdmin(t, client).All().Verbosity(context.Background(), 1, AdminOpts{})
	isErrorIs(t, err, ErrBusy)
	isErrorIs(t, err, ErrServerError)
	contains(t, err.Error(), "crawler")
	contains(t, err.Error(), "oom")
}

type quietLineConn struct {
	mockConn
	line []byte
}

func (c *quietLineConn) readLine(byte) ([]byte, error) {
	if len(c.line) == 0 {
		return nil, io.EOF
	}
	line := c.line
	c.line = nil
	return line, nil
}

func (c *quietLineConn) buffered() int { return len(c.line) }

func TestReadQuietFaultBufferedAndEmpty(t *testing.T) {
	err := readQuietFault(context.Background(), &quietLineConn{line: []byte("CLIENT_ERROR bad flags\r\n")})
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	noError(t, readQuietFault(context.Background(), &quietLineConn{}))
}

func TestReadQuietFaultCancelledStillReadsBuffered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := readQuietFault(ctx, &quietLineConn{line: []byte("CLIENT_ERROR already here\r\n")})
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "already here")
	noError(t, readQuietFault(ctx, &quietLineConn{}))

	expired, stop := context.WithDeadline(context.Background(), nowFunc().Add(-time.Second))
	defer stop()
	err = readQuietFault(expired, &quietLineConn{line: []byte("SERVER_ERROR oom\r\n")})
	isErrorIs(t, err, ErrServerError)
}

func TestMetaPipelineChargeOnlyDoesNotInventHD(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "CLIENT_ERROR bad meta\r\n"
		case "mn":
			return "MN\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	got, err := client.MetaPipeline(context.Background(), []MetaOp{
		{Op: MetaOpDelete, Key: []byte("k")},
	})
	isErrorIs(t, err, ErrClientError)
	hasLen(t, got, 1)
	equal(t, MetaStatusUnknown, got[0].Status)
	isErrorIs(t, got[0].Err(), ErrClientError)
	isFalse(t, errors.Is(got[0].Err(), ErrTransport))
}

func TestGetOneNonexistentCommandHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "ERROR\r\n"
		case "version":
			return "VERSION after-error\r\n"
		default:
			return "CLIENT_ERROR unexpected\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.GetOne(context.Background(), []byte("k"))
	isErrorIs(t, err, ErrNonexistentCommand)
	isFalse(t, shouldDiscardConn(err))
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-error", ver)
	equal(t, int32(1), dials.Load())
}

func TestGetClientErrorHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-get\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.Get(context.Background(), GetOpts{}, []byte("k"))
	isErrorIs(t, err, ErrClientError)
	contains(t, err.Error(), "bad flags")
	isFalse(t, shouldDiscardConn(err))
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-get", ver)
	equal(t, int32(1), dials.Load())
}

func TestGetAndSetServerErrorDiscards(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "SERVER_ERROR oom\r\n"
		case "set":
			return "SERVER_ERROR oom\r\n"
		case "version":
			return "VERSION after-oom\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.Get(context.Background(), GetOpts{}, []byte("k"))
	isErrorIs(t, err, ErrServerError)
	isTrue(t, shouldDiscardConn(err))
	err = client.Set(context.Background(), []byte("k"), []byte("v"), 0, 0)
	isErrorIs(t, err, ErrServerError)
	isTrue(t, shouldDiscardConn(err))
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-oom", ver)
	if dials.Load() < 3 {
		t.Fatalf("SERVER_ERROR must discard; dials=%d", dials.Load())
	}
}

func TestGetValueBodyLooksLikeCLIENT_ERROR(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "VALUE k 0 12\r\nCLIENT_ERROR\r\nEND\r\n"
		case "version":
			return "VERSION after-body\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	item, err := client.GetOne(context.Background(), []byte("k"))
	noError(t, err)
	equal(t, []byte("CLIENT_ERROR"), item.Value)
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-body", ver)
	equal(t, int32(1), dials.Load())
}

func TestMetaRejectsWikiSTAndSpecDE(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "md":
			return "DE\r\n"
		case "ms":
			return "ST\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := client.MetaDelete(context.Background(), []byte("k"), MetaDelete{})
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(err))
	_, err = client.MetaSet(context.Background(), []byte("k"), []byte("v"), MetaSet{})
	isErrorIs(t, err, ErrMalformedResponse)
	isTrue(t, shouldDiscardConn(err))
}

func TestAdminPublicWireSentinels(t *testing.T) {
	ctx := context.Background()
	type tc struct {
		name   string
		reply  string
		want   error
		run    func(*AdminClient) error
		pooled bool
	}
	tests := []tc{
		{"badclass", "BADCLASS invalid src or dst class id\r\n", ErrBadClass, func(a *AdminClient) error {
			return a.SlabsReassign(ctx, 1, 2)
		}, true},
		{"nospare", "NOSPARE source class has no spare pages\r\n", ErrNoSpare, func(a *AdminClient) error {
			return a.SlabsReassign(ctx, 1, 2)
		}, true},
		{"notfull", "NOTFULL dest class must be full to steal from\r\n", ErrNotFull, func(a *AdminClient) error {
			return a.SlabsReassign(ctx, 1, 2)
		}, true},
		{"unsafe", "UNSAFE source class cannot move pages\r\n", ErrUnsafe, func(a *AdminClient) error {
			return a.SlabsReassign(ctx, 1, 2)
		}, true},
		{"memlimit-small", "MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m\r\n", ErrMemlimitTooSmall, func(a *AdminClient) error {
			return a.CacheMemLimit(ctx, 4, AdminOpts{})
		}, true},
		{"memlimit-adjust", "MEMLIMIT_ADJUST_FAILED out of bounds\r\n", ErrMemlimitAdjustFailed, func(a *AdminClient) error {
			return a.CacheMemLimit(ctx, 64, AdminOpts{})
		}, true},
		{"notstarted", "NOTSTARTED no items to crawl\r\n", ErrNotStarted, func(a *AdminClient) error {
			return a.LRUCrawlerCrawl(ctx, "all")
		}, true},
		{"watcher-too-many", "WATCHER_TOO_MANY log watcher limit reached\r\n", ErrWatcherTooMany, func(a *AdminClient) error {
			wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := a.Watch(wctx, WatchFetchers)
			return err
		}, false},
		{"watcher-failed", "WATCHER_FAILED failed to add log watcher\r\n", ErrWatcherFailed, func(a *AdminClient) error {
			wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := a.Watch(wctx, WatchFetchers)
			return err
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, dials := startCmdServer(t, func(fields []string) string {
				if fields[0] == "version" {
					return "VERSION after-admin\r\n"
				}
				return tt.reply
			})
			client := testClient(t, addr, func(c *Config) { c.AllowAdmin = true })
			err := tt.run(mustAdminOn(t, client))
			isErrorIs(t, err, tt.want)
			isFalse(t, errors.Is(err, ErrMalformedResponse))
			isFalse(t, shouldDiscardConn(err))
			contains(t, err.Error(), strings.Fields(tt.reply)[0])
			ver, err := client.Version(ctx)
			noError(t, err)
			equal(t, "after-admin", ver)
			if tt.pooled && dials.Load() != 1 {
				t.Fatalf("keepalive admin sentinel discarded conn; dials=%d", dials.Load())
			}
		})
	}
}

func TestAppStatsPrefixIsNotMalformed(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "stats":
			return "PREFIX foo 1 2 3\r\nEND\r\n"
		case "version":
			return "VERSION after-prefix\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	got, err := client.Stats(context.Background())
	noError(t, err)
	equal(t, 0, len(got))
	ver, err := client.Version(context.Background())
	noError(t, err)
	equal(t, "after-prefix", ver)
	equal(t, int32(1), dials.Load())
}

func TestShutdownCloseWithoutOK(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		return "ERROR unexpected\r\n"
	})
	client := testClient(t, addr, func(c *Config) { c.AllowAdmin = true })
	noError(t, mustAdminOn(t, client).Shutdown(context.Background(), false))
}

func TestVersionAllJoinsServerError(t *testing.T) {
	good, _ := startCmdServer(t, func(fields []string) string {
		return "VERSION good-1\r\n"
	})
	bad, _ := startCmdServer(t, func(fields []string) string {
		return "SERVER_ERROR oom\r\n"
	})
	cfg, err := testConfig(good)
	noError(t, err)
	more, err := ParseServers(bad)
	noError(t, err)
	cfg.Servers = append(cfg.Servers, more...)
	client, err := New(cfg)
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	got, err := client.VersionAll(context.Background())
	isErrorIs(t, err, ErrServerError)
	equal(t, 1, len(got))
	for _, v := range got {
		equal(t, "good-1", v)
	}
}

func TestInvalidNetworkProtocol(t *testing.T) {
	_, err := New(Config{Servers: []Server{{Network: "foo", Address: "127.0.0.1:1"}}})
	isErrorIs(t, err, ErrInvalidNetworkProtocol)
}

func TestUnknownIndicatorFromRecv(t *testing.T) {
	resp := &response{endIndicator: responseEndIndicator(99)}
	err := resp.recv(context.Background(), newMockConn(), 0)
	isErrorIs(t, err, ErrUnknownIndicator)
	isTrue(t, shouldDiscardConn(err))
}
