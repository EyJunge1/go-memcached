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
					case "quit":
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
